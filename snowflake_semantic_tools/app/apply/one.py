"""One change of a plan: re-check what plan saw, run the statements with retries, verify the write.

`ChangeApplier.apply` turns any failure, an unexpected exception included, into the change's
outcome. A change is routed to its composite lifecycle handler, skipped, pruned, or written. A
write is refused before anything runs when the plan no longer holds; once its statements ran,
the outcome reports the write even if checking it then failed, so state keeps the object as SST's.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from snowflake_semantic_tools.app.apply.errors import _exception_error, _failed, _rendered_ddl, _script_error, _skipped
from snowflake_semantic_tools.app.lifecycle.composite import CatalogPublicationPort
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    Change,
    ClassifiedError,
    ErrorKind,
    ExecResult,
    GrantCheck,
    GrantRow,
    ObservedArtifact,
    OutcomeStatus,
    RenderedArtifact,
    RetryPolicy,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, ident, join, keyword, privilege, qname, sql


def preserves_grants(change: Change) -> bool:
    """Report whether an update keeps the object's grants: a clause-preserving replace needs COPY GRANTS.

    Only an update whose artifact preserves grants by clause is checked; every statement that
    replaces the object must say COPY GRANTS, however it is spaced or cased.
    """
    if change.action is not Action.UPDATE or change.rendered is None:
        return True
    if change.rendered.grant_preservation is not GrantPreservation.CLAUSE:
        return True
    for statement in change.rendered.statements:
        normalized = " ".join(str(statement).upper().split())
        if "CREATE OR REPLACE" in normalized and "COPY GRANTS" not in normalized:
            return False
    return True


class ChangeApplier:
    """Apply the changes of a plan one by one; a run's parallel workers share one applier.

    It keeps no state between changes. The lifecycle handlers are the run's own mapping, not a
    copy, so a handler registered on the run after construction is used here too.
    """

    def __init__(
        self,
        port: CatalogPublicationPort,
        clock: ClockPort,
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler],
    ) -> None:
        self._port = port
        self._clock = clock
        self._lifecycle_handlers = lifecycle_handlers

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Apply one change and report how it ended; an exception that escapes fails the change."""
        try:
            return self._apply_guarded(change, options)
        except Exception as exc:
            return _failed(change, _exception_error(exc), _rendered_ddl(change))

    def _apply_guarded(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Route a change: to its composite handler, a skip, a prune, a refusal, or a write, in that order.

        The clock starts before routing, so a duration counts from the moment apply took up the change.
        """
        started = self._clock.monotonic_ms()
        lifecycle_handler = self._lifecycle_handlers.get(change.artifact_type)
        if lifecycle_handler is not None:
            return lifecycle_handler.apply(change, options)
        if change.action in (Action.NOOP, Action.BLOCKED):
            return _skipped(change, _rendered_ddl(change))
        if change.action is Action.PRUNE:
            return self._prune(change, options, started)
        rendered = change.rendered
        if rendered is None:
            return _failed(change, ClassifiedError("SST-APL001", "missing rendered artifact", ErrorKind.UNKNOWN), "")
        refusal = self._refusal(change, rendered)
        if refusal is not None:
            return refusal
        return self._write(change, rendered, options, started)

    def _prune(self, change: Change, options: ApplyOptions, started: int) -> ApplyOutcome:
        """Drop the object a prune retires, once its ownership marker still reads as plan saw it.

        A report-only prune, a prune the options do not allow, and a prune with nothing observed
        are skipped.
        """
        if not change.prune_executable or not options.allow_prune or change.observed is None:
            return _skipped(change, "")
        if self._marker_changed(change.observed):
            error = ClassifiedError("SST-APL012", "ownership marker changed", ErrorKind.UNKNOWN)
            return _failed(change, error, "", duration_ms=self._elapsed(started))
        result = self._port.execute_script(
            (
                sql(
                    "DROP {kind} {name}",
                    kind=keyword(change.observed.object_type),
                    name=qname(change.observed.qualified_name),
                ),
            )
        )
        return self._execution_outcome(change, result, 1, started, "")

    def _refusal(self, change: Change, rendered: RenderedArtifact) -> ApplyOutcome | None:
        """Refuse a write the plan no longer supports, before anything runs; None lets it proceed.

        Checked in order: an artifact only a composite handler may publish, a created object
        that appeared since the plan (unless temporary), an updated object whose marker changed,
        and a replace that would drop the grants.
        """
        if not rendered.generic_apply_safe:
            error = ClassifiedError(
                "SST-APL001", "artifact requires a dedicated publication handler", ErrorKind.UNKNOWN
            )
            return _failed(change, error, rendered.ddl)
        if (
            change.action is Action.CREATE
            and self._port.object_exists(rendered.object_type, rendered.target)
            and not rendered.temporary
        ):
            error = ClassifiedError("SST-APL012", "object appeared after plan", ErrorKind.UNKNOWN)
            return _failed(change, error, rendered.ddl)
        if change.action is Action.UPDATE and change.observed is not None and self._marker_changed(change.observed):
            error = ClassifiedError("SST-APL012", "object changed since plan", ErrorKind.UNKNOWN)
            return _failed(change, error, rendered.ddl)
        if not preserves_grants(change):
            error = ClassifiedError("SST-APL004", "replace omits COPY GRANTS", ErrorKind.UNKNOWN)
            return _failed(change, error, rendered.ddl)
        return None

    def _write(
        self,
        change: Change,
        rendered: RenderedArtifact,
        options: ApplyOptions,
        started: int,
    ) -> ApplyOutcome:
        """Run a checked change's statements, then verify what they wrote.

        The explicit grants an update keeps are read before anything runs, and the file the
        artifact publishes is staged before its statements. Either failing fails the change
        with nothing written.
        """
        try:
            before = self._grants_to_keep(change, rendered)
        except SnowflakePortError as exc:
            error = ClassifiedError("SST-APL008", str(exc), ErrorKind.PRIVILEGE)
            return _failed(
                change, error, rendered.ddl, duration_ms=self._elapsed(started), grants=GrantCheck.UNREADABLE
            )
        try:
            self._stage_upload(rendered)
        except SnowflakePortError as exc:
            return _failed(change, _exception_error(exc), rendered.ddl, duration_ms=self._elapsed(started))
        result, attempts = self._execute_with_retry(rendered.statements, options.retry)
        outcome = self._execution_outcome(change, result, attempts, started, rendered.ddl)
        if not outcome.write_succeeded:
            return outcome
        try:
            return self._verify_write(change, outcome, before)
        except Exception as exc:
            # The statements ran, so the object is written whatever failed while it was
            # checked. Reporting no write would drop it from state, and the next plan
            # would call an object SST created unmanaged.
            return replace(outcome, status=OutcomeStatus.FAILED, error=_exception_error(exc), write_succeeded=True)

    def _grants_to_keep(self, change: Change, rendered: RenderedArtifact) -> tuple[GrantRow, ...] | None:
        """Return the explicit grants an update must keep; None when it has none to check.

        Raises:
            SnowflakePortError: the grants could not be read.
        """
        if (
            change.action is not Action.UPDATE
            or change.observed is None
            or rendered.grant_preservation is GrantPreservation.NONE
        ):
            return None
        return self._explicit_grants(change.observed, rendered)

    def _stage_upload(self, rendered: RenderedArtifact) -> None:
        """Stage the file the artifact's statements read, when it publishes one."""
        if rendered.upload_path is not None and rendered.upload_content is not None:
            self._port.upload(rendered.upload_path, rendered.upload_content)

    def _execute_with_retry(self, statements: tuple[Sql, ...], retry: RetryPolicy) -> tuple[ExecResult, int]:
        """Run the statements, retrying a transient failure after the policy's backoff.

        Returns:
            The last attempt's result and how many attempts ran.
        """
        attempts = 0
        result = None
        for attempt in range(1, retry.max_attempts + 1):
            attempts = attempt
            result = self._port.execute_script(statements)
            if result.ok:
                break
            if not _script_error(result, "unknown Snowflake failure").retryable or attempt == retry.max_attempts:
                break
            self._clock.sleep(retry.delay_after(attempt))
        assert result is not None
        return result, attempts

    def _execution_outcome(
        self,
        change: Change,
        result: ExecResult,
        attempts: int,
        started: int,
        ddl: str,
    ) -> ApplyOutcome:
        """Report how the statements ran: applied, or failed with the error they reported.

        A failure after any statement ran is still a write, which state keeps.
        """
        duration = self._elapsed(started)
        if result.ok:
            return ApplyOutcome(
                change.key,
                change.action,
                OutcomeStatus.APPLIED,
                attempts,
                duration,
                ddl,
                write_succeeded=True,
            )
        return ApplyOutcome(
            change.key,
            change.action,
            OutcomeStatus.FAILED,
            attempts,
            duration,
            ddl,
            _script_error(result, "unknown Snowflake failure"),
            write_succeeded=bool(result.query_ids or result.rows_affected),
        )

    def _verify_write(
        self,
        change: Change,
        outcome: ApplyOutcome,
        before: tuple[GrantRow, ...] | None,
    ) -> ApplyOutcome:
        """Check the ownership marker and the explicit grants of an object the statements wrote.

        The marker is read back even after a partial failure; the grants only after a full
        write of an update that kept some.
        """
        assert change.rendered is not None
        rendered = change.rendered
        if (
            rendered.expected_marker is not None
            and self._port.describe_marker(rendered.target, rendered.object_type) != rendered.expected_marker
        ):
            error = ClassifiedError("SST-APL012", "ownership marker was not installed", ErrorKind.UNKNOWN)
            return replace(outcome, status=OutcomeStatus.FAILED, error=error, write_succeeded=True)
        if outcome.status is OutcomeStatus.FAILED or before is None or change.observed is None:
            return outcome
        return self._verify_grants(change, change.observed, outcome, before)

    def _verify_grants(
        self,
        change: Change,
        observed: ObservedArtifact,
        outcome: ApplyOutcome,
        before: tuple[GrantRow, ...],
    ) -> ApplyOutcome:
        """Replay the grants a replace drops, when the artifact needs it, then check none was lost.

        Grants are compared by identity, so a changed grantor is no loss. Grants that cannot be
        read again leave the write applied, with its grants unverified.
        """
        assert change.rendered is not None
        if change.rendered.grant_preservation is GrantPreservation.REPLAY:
            replay = self._replay_grants(change.rendered, before)
            if replay is not None:
                return replace(outcome, status=OutcomeStatus.FAILED, error=replay, write_succeeded=True)
        try:
            after = self._explicit_grants(observed, change.rendered)
        except SnowflakePortError:
            return replace(outcome, grants=GrantCheck.UNREADABLE)
        after_identities = {grant.identity for grant in after}
        lost = tuple(sorted(grant for grant in before if grant.identity not in after_identities))
        if lost:
            error = ClassifiedError("SST-APL009", repr(lost), ErrorKind.PRIVILEGE)
            return replace(outcome, status=OutcomeStatus.FAILED, error=error, write_succeeded=True)
        return replace(outcome, grants=GrantCheck.PRESERVED)

    def _explicit_grants(self, observed: ObservedArtifact, rendered: RenderedArtifact) -> tuple[GrantRow, ...]:
        """Read the grants on the object that a replace must keep: a role's privileges, not OWNERSHIP."""
        return tuple(
            grant
            for grant in self._port.show_grants(
                observed.object_type,
                observed.qualified_name,
                rendered.routine_signature,
            )
            if grant.is_explicit
        )

    def _replay_grants(self, rendered: RenderedArtifact, grants: tuple[GrantRow, ...]) -> ClassifiedError | None:
        """Grant again, in identity order, what the object held before the replace; None when all ran."""
        statements = tuple(
            _grant_statement(rendered, grant) for grant in sorted(grants, key=lambda item: item.identity)
        )
        if not statements:
            return None
        result = self._port.execute_script(statements)
        if result.ok:
            return None
        return _script_error(result, "grant replay failed")

    def _marker_changed(self, observed: ObservedArtifact) -> bool:
        """Report whether the object's ownership marker no longer reads as plan observed it."""
        return self._port.describe_marker(observed.qualified_name, observed.object_type) != observed.marker

    def _elapsed(self, started: int) -> int:
        """Return the milliseconds since `started`, by the clock's monotonic reading."""
        return self._clock.monotonic_ms() - started


def _grant_statement(rendered: RenderedArtifact, grant: GrantRow) -> Sql:
    """Render the GRANT that restores one explicit grant, spelling a database role as Snowflake does."""
    grantee_kind = "DATABASE ROLE" if grant.granted_to.upper() == "DATABASE_ROLE" else grant.granted_to
    return sql(
        "GRANT {privilege} ON {kind} {target} TO {grantee_kind} {grantee}{grant_option}",
        privilege=privilege(grant.privilege),
        kind=keyword(rendered.object_type),
        target=qname(rendered.target),
        grantee_kind=keyword(grantee_kind),
        grantee=_grantee_identifier(grant.grantee_name),
        grant_option=sql(" WITH GRANT OPTION") if grant.grant_option else sql(""),
    )


def _grantee_identifier(value: str) -> Sql:
    """Render a grantee as SHOW GRANTS printed it; a name of two or three dotted parts part by part."""
    parts = value.split(".")
    if len(parts) in (2, 3):
        return join(".", (_simple_identifier(part) for part in parts))
    return _simple_identifier(value)


def _simple_identifier(value: str) -> Sql:
    """Render one name part as SHOW GRANTS printed it, naming the same object.

    A part Snowflake would store as printed, upper case and valid unquoted, is written bare;
    any other, one with a lowercase letter, a space, or a quote, is quoted exactly with each
    inner quote doubled, so no name can close the identifier early.

    Raises:
        ValueError: the part is empty or is a name no identifier can spell, as `ident` refuses.
    """
    try:
        return ident(Identifier.shown(value))
    except ValueError:
        return ident(Identifier(value, quoted=True))
