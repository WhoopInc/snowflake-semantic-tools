"""The skeleton every composite lifecycle handler shares, and the outcomes its steps build.

A composite artifact spans Snowflake objects that no single statement publishes, so its
handler observes them itself, decides the change, and publishes them one verified step at
a time. `CompositeHandler` runs that skeleton once and leaves each handler what is really
its own: what it observes, how it decides, how it publishes and prunes, and what it locks.
`PublicationRun` is one publish attempt's record of what it ran, wrote, and verified, so a
failure part-way reports exactly what state may take ownership of.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Mapping
from copy import copy
from dataclasses import replace
from types import MappingProxyType
from typing import Generic, Self, TypeVar, cast

from snowflake_semantic_tools.app.lifecycle.ports import PublicationPort
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.signatures import match_signature
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    Change,
    ChangeReason,
    ClassifiedError,
    CompositeObservation,
    CompositePlan,
    ExecResult,
    OutcomeStatus,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.model.skill import BundleEntry
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry, AppliedResourceInput, Manifest
from snowflake_semantic_tools.domain.validate.publication import statement_diagnostic

# The type Snowflake reports for an internal stage with server-side encryption only.
SSE_STAGE_TYPE = "INTERNAL NO CSE"

SubjectT = TypeVar("SubjectT")
ObservedT = TypeVar("ObservedT")


# The port a handler or run is typed with: at least `PublicationPort`, narrowed per artifact type.
PortT = TypeVar("PortT", bound=PublicationPort)


def create_sse_stage_sql(stage: QualifiedName) -> Sql:
    """Return the statement that creates a server-side encrypted stage unless it exists."""
    return sql("CREATE STAGE IF NOT EXISTS {stage} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')", stage=qname(stage))


class PublicationRefused(Exception):
    """A statement a publisher must never send, refused before it ran.

    It is not a `SnowflakePortError`, so a run's own error handling cannot report it as a
    Snowflake failure; `CompositeHandler.apply` reports `outcome`, which the run built with
    what it had written so far.
    """

    def __init__(self, outcome: ApplyOutcome) -> None:
        super().__init__(outcome.error.message if outcome.error is not None else "publication refused")
        self.outcome = outcome


def skipped(change: Change, ddl: str = "") -> ApplyOutcome:
    """Return the outcome of a change that apply had nothing to run for."""
    return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, ddl)


def failed(
    change: Change,
    detail: str,
    *,
    code: str = "SST-APL001",
    write_succeeded: bool = False,
    attempts: int = 0,
    physical_resources: tuple[tuple[str, str], ...] = (),
    value: str = "",
) -> ApplyOutcome:
    """Return a failed outcome for the change's artifact, with the error classified from its detail.

    Args:
        value: What `code`'s message names besides the artifact, such as a path or version.
    """
    artifact = change.rendered
    signature = match_signature(detail)
    return ApplyOutcome(
        change.key,
        change.action,
        OutcomeStatus.FAILED,
        attempts,
        0,
        artifact.ddl if artifact is not None else "",
        ClassifiedError(code, detail, signature.kind, signature.retryable, value=value),
        write_succeeded=write_succeeded,
        component_fingerprints=artifact.component_fingerprints if artifact is not None else (),
        physical_resources=physical_resources,
    )


def blocked(
    observation: CompositeObservation,
    diagnostic: Diagnostic,
    reason: ChangeReason = ChangeReason.VALIDATION_ERRORS,
) -> CompositePlan:
    """Return a plan that blocks the observed artifact for one diagnostic."""
    return CompositePlan(Action.BLOCKED, reason, observation, DiagnosticBag((diagnostic,)))


def unobservable(key: str, error: SnowflakePortError) -> CompositePlan:
    """Return the plan for an artifact whose resources Snowflake would not report.

    Diagnostics:
        SST-PLN001: the handler could not observe the artifact's resources.
    """
    diagnostic = D("SST-PLN001", value=key, detail=str(error))
    observation = CompositeObservation(key, diagnostics=DiagnosticBag((diagnostic,)))
    return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, observation.diagnostics)


def details_stale(
    planned: tuple[tuple[str, str], ...],
    current: tuple[tuple[str, str], ...],
    *,
    reverified: frozenset[str] = frozenset(),
    creatable: Mapping[str, tuple[str, str]] = MappingProxyType({}),
) -> bool:
    """Report whether a handler's observed details changed since the plan.

    `reverified` names details apply checks again anyway, so they are not compared, and
    `creatable` maps a detail to the `(absent, created)` values a sibling artifact's apply
    may have moved it between since the plan: that change alone is not staleness.
    """
    before = {key: value for key, value in planned if key not in reverified}
    after = {key: value for key, value in current if key not in reverified}
    for key, (absent, created) in creatable.items():
        if before.get(key) == absent and after.get(key) == created:
            before[key] = created
    return before != after


class CompositeHandler(CompositeLifecycleHandler, Generic[SubjectT, ObservedT, PortT]):
    """The lifecycle skeleton of one composite artifact type, run with the steps a handler supplies.

    `plan` finds what the artifact publishes with `_subject`, reads it from Snowflake with
    `_observe`, and decides the change with `_decide`; an observation Snowflake refuses
    blocks the artifact instead (SST-PLN001). `apply` checks a change in this order: a
    prune goes to `_apply_prune`, a change without a rendered artifact to
    `_apply_unrendered`, a NOOP or BLOCKED change is skipped, and a create or update goes
    to `_publish`.

    A subclass sets `artifact_type` and overrides `_subject`, `_observe`, `_decide` and
    `_publish`: what it observes, how it decides, and how it publishes, with its own
    ownership rules and locks. The other steps have defaults it may override. Prune is
    report-only: `report_prune` reports the artifact with the subclass's `_prune_detail`
    at its `_prune_order`, and `_apply_prune` skips it. `_apply_unrendered` skips a change
    without an artifact, and `merge_physical_resources` records exactly the resources the
    publish verified.
    """

    _prune_detail: str
    _prune_order: int

    def __init__(self, port: PortT) -> None:
        self._port = port

    def for_session(self, session: object) -> Self:
        """Return a copy of this handler on `session`, sharing every lock and record of what it created.

        `session` is a leased connection of the command's own settings, which implements every
        role the handler's port does.
        """
        bound = copy(self)
        bound._port = cast(PortT, session)
        return bound

    def plan(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        manifest: Manifest,
    ) -> CompositePlan:
        """Observe the artifact's resources and decide its change; an unreadable resource blocks it.

        An artifact the handler finds unchanged, but whose state entry another manifest
        recorded, is an UPDATE for that reason: publishing re-verifies it and records it under
        this manifest, which is what clears SST-MAN021.
        """
        subject = self._subject(artifact)
        try:
            observed = self._observe(subject)
        except SnowflakePortError as exc:
            return unobservable(artifact.key, exc)
        decided = self._decide(artifact, state_entry, subject, observed)
        recorded_elsewhere = state_entry is not None and state_entry.manifest_id != manifest.manifest_id
        if decided.action is Action.NOOP and recorded_elsewhere:
            return replace(decided, action=Action.UPDATE, reason=ChangeReason.STATE_MANIFEST_MISMATCH)
        return decided

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Carry out one planned change, dispatched as the class describes.

        A statement the publication guards refuse fails the change with the guard's code, as
        `PublicationRun._run_statement` describes.
        """
        if change.action is Action.PRUNE:
            return self._apply_prune(change, options)
        artifact = change.rendered
        if artifact is None:
            return self._apply_unrendered(change)
        if change.action in (Action.NOOP, Action.BLOCKED):
            return skipped(change, artifact.ddl)
        try:
            return self._publish(change, artifact)
        except PublicationRefused as refused:
            return refused.outcome

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        """Report an artifact the project no longer declares as a prune apply never executes.

        Diagnostics:
            SST-PLN034: the resources SST keeps, and what to do before removing them.
        """
        resources = ", ".join(resource.qualified_name for resource in state_entry.applied_resources) or artifact_key
        diagnostic = D(
            "SST-PLN034",
            subject=artifact_key,
            artifact=artifact_key,
            value=resources,
            detail=self._prune_detail,
        )
        return Change(
            artifact_key,
            self.artifact_type,
            Action.PRUNE,
            ChangeReason.ORPHANED,
            None,
            None,
            (),
            self._prune_order,
            DiagnosticBag((diagnostic,)),
            prune_executable=False,
        )

    @staticmethod
    def merge_physical_resources(
        current: tuple[tuple[str, str], ...],
        previous: AppliedEntry | None,
    ) -> tuple[AppliedResourceInput, ...]:
        """Return the resources state records for a publish: by default, exactly those verified now."""
        del previous
        return current

    @abstractmethod
    def _subject(self, artifact: RenderedArtifact) -> SubjectT:
        """Return what the handler observes and publishes for a rendered artifact."""

    @abstractmethod
    def _observe(self, subject: SubjectT) -> ObservedT:
        """Read the subject's resources from Snowflake; a `SnowflakePortError` blocks the plan."""

    @abstractmethod
    def _decide(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        subject: SubjectT,
        observed: ObservedT,
    ) -> CompositePlan:
        """Decide the artifact's change from what Snowflake holds and what state recorded."""

    @abstractmethod
    def _publish(self, change: Change, artifact: RenderedArtifact) -> ApplyOutcome:
        """Create or update the artifact's resources, refusing a plan they drifted from."""

    def _apply_prune(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Skip a prune: by default it is report-only, and SST removes nothing."""
        del options
        return skipped(change)

    def _apply_unrendered(self, change: Change) -> ApplyOutcome:
        """Skip a change that carries no rendered artifact, which leaves nothing to publish."""
        return skipped(change)


class PublicationRun(Generic[PortT]):
    """One publish attempt: counts the statements it runs and records what it wrote and verified.

    A handler's run subclasses this and chains its steps with `_run_steps`, each returning
    a failed outcome, or None to go on. `fail` reports the attempts so far with only what
    the subclass's ownership rule lets state take as SST's: `_owns_write` and
    `_recorded_resources`, by default any write and each resource verified so far.
    """

    def __init__(self, port: PortT, change: Change, artifact: RenderedArtifact) -> None:
        self._port = port
        self._change = change
        self._artifact = artifact
        self._attempts = 0
        self._written = False
        self._verified: list[tuple[str, str]] = []

    def fail(self, detail: str, code: str = "SST-APL001", *, value: str = "") -> ApplyOutcome:
        """Return the failed outcome, reporting only the write and resources state may own.

        Args:
            value: What `code`'s message names besides the artifact, such as a path or version.
        """
        return failed(
            self._change,
            detail,
            code=code,
            write_succeeded=self._owns_write(),
            attempts=self._attempts,
            physical_resources=self._recorded_resources(),
            value=value,
        )

    def applied(
        self,
        *,
        write_succeeded: bool,
        component_fingerprints: tuple[tuple[str, str], ...],
        physical_resources: tuple[tuple[str, str], ...],
    ) -> ApplyOutcome:
        """Return the applied outcome, counting at least one attempt, with what the run published."""
        return ApplyOutcome(
            self._change.key,
            self._change.action,
            OutcomeStatus.APPLIED,
            max(self._attempts, 1),
            0,
            self._artifact.ddl,
            write_succeeded=write_succeeded,
            component_fingerprints=component_fingerprints,
            physical_resources=physical_resources,
        )

    def _owns_write(self) -> bool:
        """Report whether state may record the run's writes as SST's: by default, any write."""
        return self._written

    def _certification_pending(self) -> bool:
        """Report whether the run certifies its artifact and has not yet succeeded: by default, never."""
        return False

    def _recorded_resources(self) -> tuple[tuple[str, str], ...]:
        """Return the resources a failed run reports: by default each one verified, once."""
        return tuple(dict.fromkeys(self._verified))

    @staticmethod
    def _run_steps(*steps: Callable[[], ApplyOutcome | None]) -> ApplyOutcome | None:
        """Run the steps in order and return the first failure; None once every step succeeded."""
        for step in steps:
            failure = step()
            if failure is not None:
                return failure
        return None

    def _run_statement(self, statement: Sql) -> ExecResult:
        """Execute one statement as an attempt; a success counts as a write.

        Every statement a run sends passes the publication guards first. One they refuse is
        never sent: the run fails with what it wrote so far.

        Raises:
            PublicationRefused: the statement is raw text, or a grant issued before
                certification succeeded or naming no role type.

        Diagnostics:
            SST-VAL826, SST-VAL827: as `statement_diagnostic` reports them, as the failure's code.
        """
        refused = statement_diagnostic(self._change.key, statement, certification_pending=self._certification_pending())
        if refused is not None:
            raise PublicationRefused(
                self.fail(refused.message, refused.code, value=str(refused.context.get("value", "")))
            )
        self._attempts += 1
        result = self._port.execute_script((statement,))
        if result.ok:
            self._written = True
        return result

    def _upload_missing(
        self,
        prefix: str,
        entries: Iterable[BundleEntry],
        present: Collection[str],
    ) -> tuple[BundleEntry, SnowflakePortError] | None:
        """Upload each entry not yet present under the prefix, stopping at the first that fails.

        Returns the entry whose upload failed, with its error; None when every upload succeeded.
        """
        for entry in entries:
            if entry.path in present:
                continue
            self._attempts += 1
            try:
                self._port.upload(f"{prefix}{entry.path}", entry.content)
            except SnowflakePortError as exc:
                return entry, exc
            self._written = True
        return None

    def _verify_staged(
        self,
        prefix: str,
        entries: tuple[BundleEntry, ...],
        differs: Callable[[tuple[str, ...]], str],
    ) -> ApplyOutcome | None:
        """Require the prefix to hold exactly the entries, each reading back byte for byte (SST-APL016).

        `differs` words the failure from the listed paths when they are not the entries' paths.
        """
        listed = tuple(sorted(self._port.list_location(prefix)))
        if listed != tuple(sorted(entry.path for entry in entries)):
            return self.fail(differs(listed), "SST-APL016")
        for entry in entries:
            if self._port.read_staged_file(f"{prefix}{entry.path}") != entry.content:
                return self.fail(f"{prefix}{entry.path} does not read back byte for byte", "SST-APL016")
        return None
