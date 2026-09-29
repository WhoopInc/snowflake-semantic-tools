"""Guarded execution of a reviewed ChangeSet."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import MappingProxyType
from typing import Mapping

from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from ..domain.model.identifier import Identifier, QualifiedName
from ..domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    ApplyResult,
    Change,
    ChangeSet,
    ClassifiedError,
    ErrorKind,
    ExecResult,
    FailurePolicy,
    GrantCheck,
    GrantRow,
    OutcomeStatus,
)
from ..domain.model.registry import GrantPreservation
from ..domain.plan.diff import dependency_waves
from ..domain.ports.lifecycle import CompositeLifecycleHandler
from ..domain.ports.snowflake import ClockPort, SnowflakePort, SnowflakePortError, StateStore
from ..domain.state.model import (
    DEACTIVATED,
    FAILED_AFTER_WRITE,
    SST_VERSION,
    STATE_SCHEMA_VERSION,
    AppliedEntry,
    AppliedResourceInput,
    LastRun,
    State,
)


def classify_error(message: str, *, sqlstate: str | None = None) -> ClassifiedError:
    state = sqlstate or ""
    upper = message.upper()
    if state.startswith("28") or "INSUFFICIENT PRIVILEGE" in upper or "NOT AUTHORIZED" in upper:
        return ClassifiedError("SST-SNO004", message, ErrorKind.PRIVILEGE, False, sqlstate)
    if state in {"02000", "42S02"} or "DOES NOT EXIST" in upper:
        return ClassifiedError("SST-SNO003", message, ErrorKind.NOT_FOUND, False, sqlstate)
    if state.startswith("42") or "SYNTAX ERROR" in upper:
        return ClassifiedError("SST-SNO009", message, ErrorKind.SYNTAX, False, sqlstate)
    if state.startswith("08") or state in {"57014", "57P01"} or "TIMEOUT" in upper:
        return ClassifiedError("SST-SNO022", message, ErrorKind.TRANSIENT, True, sqlstate)
    return ClassifiedError("SST-SNO001", message, ErrorKind.UNKNOWN, False, sqlstate)


def preserves_grants(change: Change) -> bool:
    if change.action is not Action.UPDATE or change.rendered is None:
        return True
    if change.rendered.grant_preservation is not GrantPreservation.CLAUSE:
        return True
    for statement in change.rendered.statements:
        normalized = " ".join(statement.upper().split())
        if "CREATE OR REPLACE" in normalized and "COPY GRANTS" not in normalized:
            return False
    return True


class ApplyArtifacts:
    def __init__(
        self,
        port: SnowflakePort,
        state_store: StateStore,
        clock: ClockPort,
        *,
        state_table: QualifiedName,
        git_sha: str = "",
        actor: str = "",
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
    ) -> None:
        self._port = port
        self._state_store = state_store
        self._clock = clock
        self._state_table = state_table
        self._git_sha = git_sha
        self._actor = actor
        self._lifecycle_handlers = dict(lifecycle_handlers or {})

    def run(self, changeset: ChangeSet, previous: State, options: ApplyOptions = ApplyOptions()) -> ApplyResult:
        run_id = self._clock.new_run_id()
        started = self._clock.now_iso()
        diagnostics: list[Diagnostic] = list(changeset.diagnostics)
        if changeset.diagnostics.has_errors:
            return ApplyResult(
                (),
                DiagnosticBag(diagnostics),
                run_id,
                started,
                self._clock.now_iso(),
                False,
            )
        if changeset.blocked and options.on_failure is not FailurePolicy.CONTINUE:
            diagnostics.extend(
                D("SST-APL003", artifact=change.key, count=len(change.diagnostics) or 1) for change in changeset.blocked
            )
            return ApplyResult(
                (),
                DiagnosticBag(diagnostics),
                run_id,
                started,
                self._clock.now_iso(),
                False,
            )
        locked, holder, broke_stale = self._state_store.acquire_lock(
            run_id,
            break_stale=options.break_stale_lock,
        )
        if not locked:
            diagnostics.append(D("SST-APL011", value=holder or "another run"))
            return ApplyResult(
                (),
                DiagnosticBag(diagnostics),
                run_id,
                started,
                self._clock.now_iso(),
                False,
            )
        if broke_stale:
            diagnostics.append(D("SST-APL010", value=holder or "expired run"))

        outcomes: list[ApplyOutcome] = []
        failed_or_skipped: set[str] = set()
        try:
            relation_failure = self._preflight_relations(changeset.changes)
            if relation_failure is not None:
                outcomes.extend(relation_failure)
                failed_or_skipped.update(outcome.key for outcome in relation_failure)
            else:
                for wave in dependency_waves(changeset.changes):
                    runnable: list[Change] = []
                    for change in wave:
                        blockers = tuple(
                            dependency for dependency in change.depends_on if dependency in failed_or_skipped
                        )
                        if blockers and options.on_failure is FailurePolicy.STOP_DEPENDENTS:
                            outcomes.append(
                                ApplyOutcome(
                                    change.key,
                                    change.action,
                                    OutcomeStatus.SKIPPED,
                                    0,
                                    0,
                                    change.rendered.ddl if change.rendered else "",
                                )
                            )
                            diagnostics.append(
                                D(
                                    "SST-APL002",
                                    artifact=change.key,
                                    blocker=", ".join(blockers),
                                )
                            )
                            failed_or_skipped.add(change.key)
                            continue
                        runnable.append(change)
                    if options.on_failure is FailurePolicy.STOP_ALL:
                        wave_outcomes_list: list[ApplyOutcome] = []
                        for change in runnable:
                            outcome = self._apply_one(change, options)
                            wave_outcomes_list.append(outcome)
                            if outcome.status is OutcomeStatus.FAILED:
                                break
                        wave_outcomes = tuple(wave_outcomes_list)
                    else:
                        with ThreadPoolExecutor(max_workers=options.parallelism) as pool:
                            wave_outcomes = tuple(
                                pool.map(
                                    lambda item: self._apply_one(item, options),
                                    runnable,
                                )
                            )
                    for change, outcome in zip(runnable, wave_outcomes):
                        outcomes.append(outcome)
                        if outcome.grants is GrantCheck.UNREADABLE:
                            diagnostics.append(D("SST-APL008", artifact=change.key))
                        if outcome.status is OutcomeStatus.FAILED:
                            failed_or_skipped.add(change.key)
                            diagnostics.append(self._outcome_diagnostic(change, outcome))
                            if options.on_failure is FailurePolicy.STOP_ALL:
                                remaining = [
                                    item for item in changeset.changes if item.key not in {o.key for o in outcomes}
                                ]
                                outcomes.extend(
                                    ApplyOutcome(
                                        item.key,
                                        item.action,
                                        OutcomeStatus.SKIPPED,
                                        0,
                                        0,
                                        item.rendered.ddl if item.rendered else "",
                                    )
                                    for item in remaining
                                )
                                break
                    if failed_or_skipped and options.on_failure is FailurePolicy.STOP_ALL:
                        break
            if len(outcomes) != len(changeset.changes):
                diagnostics.append(
                    D(
                        "SST-APL900",
                        found=len(outcomes),
                        expected=len(changeset.changes),
                    )
                )
            # A report-only prune executes nothing, but state still records the run:
            # the entry is retained and the manifest moves on, or SST-MAN021 never clears.
            if not changeset.writes and not changeset.report_only:
                return ApplyResult(
                    tuple(outcomes),
                    DiagnosticBag(diagnostics),
                    run_id,
                    started,
                    self._clock.now_iso(),
                    False,
                )
            state = self._finish_state(changeset, previous, tuple(outcomes), run_id, started)
            self._write_remote_state(changeset, state)
            self._state_store.write_local(state)
            return ApplyResult(
                tuple(outcomes),
                DiagnosticBag(diagnostics),
                run_id,
                started,
                state.last_run.finished_at if state.last_run else self._clock.now_iso(),
                True,
            )
        finally:
            self._state_store.release_lock(run_id)

    def _write_remote_state(self, changeset: ChangeSet, state: State) -> None:
        self._port.write_state(
            self._state_table,
            changeset.target.name,
            changeset.manifest_id,
            state.applied,
        )

    def _preflight_relations(self, changes: tuple[Change, ...]) -> tuple[ApplyOutcome, ...] | None:
        relations = {
            relation
            for change in changes
            if change.action in (Action.CREATE, Action.UPDATE) and change.rendered is not None
            for relation in change.rendered.required_relations
        }
        missing = tuple(
            relation for relation in sorted(relations) if not self._port.object_exists("TABLE OR VIEW", relation)
        )
        if not missing:
            return None
        detail = "missing required relations: " + ", ".join(relation.sql for relation in missing)
        error = ClassifiedError("SST-PRT005", detail, ErrorKind.NOT_FOUND)
        return tuple(
            ApplyOutcome(
                change.key,
                change.action,
                OutcomeStatus.FAILED,
                0,
                0,
                change.rendered.ddl if change.rendered else "",
                error,
            )
            for change in changes
        )

    def _apply_one(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        try:
            return self._apply_one_guarded(change, options)
        except Exception as exc:
            ddl = change.rendered.ddl if change.rendered else ""
            error = classify_error(
                str(exc),
                sqlstate=getattr(exc, "sqlstate", None),
            )
            return ApplyOutcome(
                change.key,
                change.action,
                OutcomeStatus.FAILED,
                0,
                0,
                ddl,
                error,
            )

    def _apply_one_guarded(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        started = self._clock.monotonic_ms()
        ddl = change.rendered.ddl if change.rendered else ""
        lifecycle_handler = self._lifecycle_handlers.get(change.artifact_type)
        if lifecycle_handler is not None:
            return lifecycle_handler.apply(change, options)
        if change.action in (Action.NOOP, Action.BLOCKED):
            return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, ddl)
        if change.action is Action.PRUNE:
            if not change.prune_executable:
                return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, "")
            if not options.allow_prune or change.observed is None:
                return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, "")
            current = self._port.describe_marker(
                change.observed.qualified_name,
                change.observed.object_type,
            )
            if current != change.observed.marker:
                error = ClassifiedError("SST-APL012", "ownership marker changed", ErrorKind.UNKNOWN)
                return ApplyOutcome(
                    change.key,
                    change.action,
                    OutcomeStatus.FAILED,
                    0,
                    self._clock.monotonic_ms() - started,
                    "",
                    error,
                )
            prune_result = self._port.execute_script(
                (f"DROP {change.observed.object_type} {change.observed.qualified_name.sql}",)
            )
            return self._execution_outcome(change, prune_result, 1, started, "")
        if change.rendered is None:
            error = ClassifiedError("SST-APL001", "missing rendered artifact", ErrorKind.UNKNOWN)
            return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, 0, ddl, error)
        if not change.rendered.generic_apply_safe:
            error = ClassifiedError(
                "SST-APL001",
                "artifact requires a dedicated publication handler",
                ErrorKind.UNKNOWN,
            )
            return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, 0, ddl, error)
        if (
            change.action is Action.CREATE
            and self._port.object_exists(
                change.rendered.object_type,
                change.rendered.target,
            )
            and not change.rendered.temporary
        ):
            error = ClassifiedError("SST-APL012", "object appeared after plan", ErrorKind.UNKNOWN)
            return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, 0, ddl, error)
        if change.action is Action.UPDATE and change.observed is not None:
            current = self._port.describe_marker(
                change.observed.qualified_name,
                change.observed.object_type,
            )
            if current != change.observed.marker:
                error = ClassifiedError("SST-APL012", "object changed since plan", ErrorKind.UNKNOWN)
                return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, 0, ddl, error)
        if not preserves_grants(change):
            error = ClassifiedError("SST-APL004", "replace omits COPY GRANTS", ErrorKind.UNKNOWN)
            return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, 0, ddl, error)

        before = None
        if (
            change.action is Action.UPDATE
            and change.observed is not None
            and change.rendered.grant_preservation is not GrantPreservation.NONE
        ):
            try:
                before = tuple(
                    grant
                    for grant in self._port.show_grants(
                        change.observed.object_type,
                        change.observed.qualified_name,
                        change.rendered.routine_signature,
                    )
                    if grant.is_explicit
                )
            except SnowflakePortError as exc:
                error = ClassifiedError("SST-APL008", str(exc), ErrorKind.PRIVILEGE)
                return ApplyOutcome(
                    change.key,
                    change.action,
                    OutcomeStatus.FAILED,
                    0,
                    self._clock.monotonic_ms() - started,
                    ddl,
                    error,
                    GrantCheck.UNREADABLE,
                )

        if change.rendered.upload_path is not None and change.rendered.upload_content is not None:
            try:
                self._port.upload(change.rendered.upload_path, change.rendered.upload_content)
            except SnowflakePortError as exc:
                error = classify_error(str(exc), sqlstate=exc.sqlstate)
                return ApplyOutcome(
                    change.key,
                    change.action,
                    OutcomeStatus.FAILED,
                    0,
                    self._clock.monotonic_ms() - started,
                    ddl,
                    error,
                )

        attempts = 0
        result = None
        for attempt in range(1, options.retry.max_attempts + 1):
            attempts = attempt
            result = self._port.execute_script(change.rendered.statements)
            if result.ok:
                break
            message = result.error.message if result.error else "unknown Snowflake failure"
            classified = classify_error(message, sqlstate=result.error.sqlstate if result.error else None)
            if not classified.retryable or attempt == options.retry.max_attempts:
                break
            self._clock.sleep(options.retry.delay_after(attempt))
        assert result is not None
        outcome = self._execution_outcome(change, result, attempts, started, ddl)
        if outcome.write_succeeded and change.rendered.expected_marker is not None:
            current_marker = self._port.describe_marker(
                change.rendered.target,
                change.rendered.object_type,
            )
            if current_marker != change.rendered.expected_marker:
                error = ClassifiedError(
                    "SST-APL012",
                    "ownership marker was not installed",
                    ErrorKind.UNKNOWN,
                )
                return replace(outcome, status=OutcomeStatus.FAILED, error=error, write_succeeded=True)
        if outcome.status is OutcomeStatus.FAILED or before is None or change.observed is None:
            return outcome
        if change.rendered.grant_preservation is GrantPreservation.REPLAY:
            replay = self._replay_grants(change, before)
            if replay is not None:
                return replace(outcome, status=OutcomeStatus.FAILED, error=replay, write_succeeded=True)
        try:
            after = tuple(
                grant
                for grant in self._port.show_grants(
                    change.observed.object_type,
                    change.observed.qualified_name,
                    change.rendered.routine_signature,
                )
                if grant.is_explicit
            )
        except SnowflakePortError:
            return replace(outcome, grants=GrantCheck.UNREADABLE)
        after_identities = {grant.identity for grant in after}
        lost = tuple(sorted(grant for grant in before if grant.identity not in after_identities))
        if lost:
            error = ClassifiedError("SST-APL009", repr(lost), ErrorKind.PRIVILEGE)
            return replace(outcome, status=OutcomeStatus.FAILED, error=error, write_succeeded=True)
        return replace(outcome, grants=GrantCheck.PRESERVED)

    def _replay_grants(self, change: Change, grants: tuple[GrantRow, ...]) -> ClassifiedError | None:
        assert change.rendered is not None
        statements = tuple(
            f"GRANT {grant.privilege.upper()} ON {change.rendered.object_type} "
            f"{change.rendered.target.sql} TO "
            f"{'DATABASE ROLE' if grant.granted_to.upper() == 'DATABASE_ROLE' else grant.granted_to.upper()} "
            f"{_grantee_identifier(grant.grantee_name)}"
            f"{' WITH GRANT OPTION' if grant.grant_option else ''}"
            for grant in sorted(grants, key=lambda item: item.identity)
        )
        if not statements:
            return None
        result = self._port.execute_script(statements)
        if result.ok:
            return None
        message = result.error.message if result.error else "grant replay failed"
        return classify_error(message, sqlstate=result.error.sqlstate if result.error else None)

    def _execution_outcome(
        self,
        change: Change,
        result: ExecResult,
        attempts: int,
        started: int,
        ddl: str,
    ) -> ApplyOutcome:
        duration = self._clock.monotonic_ms() - started
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
        message = result.error.message if result.error else "unknown Snowflake failure"
        error = classify_error(message, sqlstate=result.error.sqlstate if result.error else None)
        return ApplyOutcome(
            change.key,
            change.action,
            OutcomeStatus.FAILED,
            attempts,
            duration,
            ddl,
            error,
            write_succeeded=bool(result.query_ids or result.rows_affected),
        )

    @staticmethod
    def _outcome_diagnostic(change: Change, outcome: ApplyOutcome) -> Diagnostic:
        assert outcome.error is not None
        if outcome.error.code == "SST-APL004":
            return D("SST-APL004", artifact=change.key)
        if outcome.error.code == "SST-APL009":
            return D("SST-APL009", artifact=change.key, value=outcome.error.message)
        if outcome.error.code == "SST-APL008":
            return D("SST-APL008", artifact=change.key)
        if outcome.error.code == "SST-APL012":
            target = change.observed.qualified_name.sql if change.observed else change.key
            return D("SST-APL012", artifact=change.key, value=target)
        if outcome.error.code == "SST-APL016":
            return D("SST-APL016", artifact=change.key, detail=outcome.error.message)
        if outcome.error.code == "SST-APL022":
            return D("SST-APL022", artifact=change.key, detail=outcome.error.message)
        if outcome.error.code == "SST-APL028":
            if change.diagnostics and change.diagnostics[0].code == "SST-APL028":
                return change.diagnostics[0]
            return D("SST-INT902", detail=outcome.error.message)
        return D(
            "SST-APL001",
            artifact=change.key,
            value=change.action.value,
            detail=outcome.error.message,
        )

    def _finish_state(
        self,
        changeset: ChangeSet,
        previous: State,
        outcomes: tuple[ApplyOutcome, ...],
        run_id: str,
        started: str,
    ) -> State:
        finished = self._clock.now_iso()
        applied = dict(previous.applied)
        by_key = {change.key: change for change in changeset.changes}
        failed_without_write = {
            outcome.key
            for outcome in outcomes
            if outcome.status is OutcomeStatus.FAILED and not outcome.write_succeeded
        }
        for outcome in outcomes:
            change = by_key[outcome.key]
            if change.action is Action.PRUNE and not change.prune_executable:
                # Nothing was removed, so the entry stays and the next plan reports it
                # again; it now belongs to this manifest, which is what clears SST-MAN021.
                retained = previous.applied.get(change.key)
                if retained is not None:
                    applied[change.key] = replace(retained, manifest_id=changeset.manifest_id)
                continue
            if outcome.status is not OutcomeStatus.APPLIED and not outcome.write_succeeded:
                continue
            if change.action is Action.PRUNE:
                if change.prune_executable and outcome.status is OutcomeStatus.APPLIED:
                    retired = previous.applied.get(change.key)
                    if change.artifact_type in self._lifecycle_handlers and retired is not None:
                        # A composite prune deactivates rather than drops, so the
                        # object is still SST's: keep a tombstone to reactivate it.
                        applied[change.key] = replace(retired, outcome=DEACTIVATED, applied_at=finished, run_id=run_id)
                    else:
                        applied.pop(change.key, None)
                continue
            if change.rendered is None:
                continue
            lifecycle_handler = self._lifecycle_handlers.get(change.artifact_type)
            # A composite handler reports exactly the resources it verified, so an
            # empty report is authoritative rather than a cue to assume the rendered set.
            current_resources = (
                outcome.physical_resources
                if lifecycle_handler is not None
                else outcome.physical_resources
                or tuple((object_type, name.sql) for object_type, name in change.rendered.physical_resources)
            )
            physical_resources: tuple[AppliedResourceInput, ...] = current_resources
            previous_entry = previous.applied.get(change.key)
            if lifecycle_handler is not None:
                physical_resources = lifecycle_handler.merge_physical_resources(current_resources, previous_entry)
            applied[change.key] = AppliedEntry(
                fingerprint=change.rendered.fingerprint,
                qualified_name=change.rendered.target.sql,
                applied_at=finished,
                run_id=run_id,
                outcome=("applied" if outcome.status is OutcomeStatus.APPLIED else FAILED_AFTER_WRITE),
                ddl_sha256=change.rendered.fingerprint,
                manifest_id=changeset.manifest_id,
                git_sha=self._git_sha,
                component_fingerprints=(outcome.component_fingerprints or change.rendered.component_fingerprints),
                physical_resources=physical_resources,
            )
        for change in changeset.changes:
            if change.key in applied or change.observed is None:
                continue
            if change.key in failed_without_write and change.key in previous.applied:
                applied[change.key] = previous.applied[change.key]
                continue
            if change.key in failed_without_write:
                continue
            marker = change.observed.marker
            if marker is None:
                continue
            applied[change.key] = AppliedEntry(
                fingerprint=marker.fingerprint,
                qualified_name=change.observed.qualified_name.sql,
                applied_at=finished,
                run_id=run_id,
                outcome="observed",
                ddl_sha256=marker.fingerprint,
                manifest_id=marker.manifest_id,
                git_sha=self._git_sha,
                component_fingerprints=(),
                physical_resources=((change.observed.object_type, change.observed.qualified_name.sql),),
            )
        outcome_name = "partial" if any(item.status is OutcomeStatus.FAILED for item in outcomes) else "ok"
        return State(
            STATE_SCHEMA_VERSION,
            changeset.target,
            changeset.manifest_id,
            self._state_store.config_path,
            LastRun(
                run_id,
                started,
                finished,
                SST_VERSION,
                "apply",
                outcome_name,
                self._actor,
            ),
            MappingProxyType(applied),
        )


def _grantee_identifier(value: str) -> str:
    parts = value.split(".")
    if len(parts) in (2, 3):
        return ".".join(_simple_identifier(part) for part in parts)
    return _simple_identifier(value)


def _simple_identifier(value: str) -> str:
    try:
        return Identifier.parse(value).sql
    except ValueError:
        return Identifier(value, quoted=True).sql
