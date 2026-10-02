"""The apply run: refuse or lock, preflight, run the dependency waves, account, and persist state.

`ApplyArtifacts.run` holds the run lease, the local and the remote lock, from the moment it
takes it until the run ends, and writes state only after every wave ran: remote first, then
local, so local state never claims what the state table does not hold. Parallel changes run
on sessions leased from a pool, one per worker, never on one shared connection.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.apply.errors import (
    _exception_error,
    _failed,
    _outcome_diagnostic,
    _rendered_ddl,
    _skipped,
)
from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.apply.one import ChangeApplier
from snowflake_semantic_tools.app.apply.state import EntryStamp, _applied_after, _run_outcome
from snowflake_semantic_tools.app.apply.temporary import temporary_notes, temporary_refusal
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    ApplyResult,
    Change,
    ChangeSet,
    ClassifiedError,
    ErrorKind,
    FailurePolicy,
    GrantCheck,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.plan import dependency_waves
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import SST_VERSION, STATE_SCHEMA_VERSION, LastRun, State
from snowflake_semantic_tools.domain.state.lock import LockClaim, state_write

_ApplyOne = Callable[[Change, ApplyOptions], ApplyOutcome]
_DEFAULT_LOCK_POLICY = LockPolicy()


class ApplyPort(CatalogPublicationPort, StatePort, Protocol):
    """The Snowflake roles apply uses: a catalog publication port that also keeps the state table."""


class ApplyArtifacts:
    """Execute a reviewed ChangeSet against Snowflake and record what it wrote in state.

    A `ChangeApplier` applies each change, re-checking what plan saw before it writes. The use
    case keeps nothing between runs; the run lease keeps two runs from applying at once.

    Args:
        sessions: Where parallel workers lease their sessions; None runs every change on
            `port`, one statement at a time.
        host: The machine the run lock records; empty when it is unknown.
    """

    def __init__(
        self,
        port: ApplyPort,
        state_store: StateStore,
        clock: ClockPort,
        *,
        state_table: QualifiedName,
        git_sha: str = "",
        actor: str = "",
        host: str = "",
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
        sessions: SessionPool[CatalogPublicationPort] | None = None,
        lock_policy: LockPolicy = _DEFAULT_LOCK_POLICY,
    ) -> None:
        self._port = port
        self._state_store = state_store
        self._clock = clock
        self._state_table = state_table
        self._git_sha = git_sha
        self._actor = actor
        self._host = host
        self._lifecycle_handlers = dict(lifecycle_handlers or {})
        self._sessions = sessions
        self._lock_policy = lock_policy
        self._changes = ChangeApplier(port, clock, self._lifecycle_handlers)

    def run(self, changeset: ChangeSet, previous: State, options: ApplyOptions = ApplyOptions()) -> ApplyResult:
        """Execute the plan's writes under the apply lock and record their outcomes in state.

        The run goes through these phases in order:

        1. Refusal, before the lock: a plan with an error diagnostic is refused with it, and so
           is a plan with blocked changes unless the policy is CONTINUE, and a `--temporary`
           run against a production-like target.
        2. Lock: the local lock, then the target's lock in Snowflake; either held by another
           run refuses the run, and an expired one is taken over only when
           `options.break_stale_lock` allows it. A heartbeat extends the remote lock while
           the run holds it.
        3. Preflight: when a relation some create or update requires does not exist, every
           change fails with SST-PRT005 and nothing runs.
        4. Waves: the dependency waves run in order, as `_WaveRun` describes.
        5. Accounting: the outcomes must account for every planned change.
        6. Persist: unless the plan has neither writes nor report-only prunes, the state the run
           leaves is written to the state table, then locally.

        A refused run never takes the lock; once taken, it is released however the run ends,
        including when writing state raises. A run whose remote lock another run broke starts
        no further wave, and still records what it wrote. State keeps every object a statement wrote, even
        when its change then failed, so a partial write stays SST's and the next plan sees it.
        The plan's own diagnostics come first in the result, then each phase's in order.

        Raises:
            ValueError: the changes' dependencies form a cycle.
            SnowflakePortError: writing the state table failed; local state is not written.

        Diagnostics:
            SST-APL002: a change was skipped because a change it depends on failed or was skipped.
            SST-APL003: the plan has a blocked change and the policy is not CONTINUE.
            SST-APL008: an object's grants could not be verified.
            SST-APL010: the run took over an expired lock.
            SST-APL011: another run holds the lock, or broke it while this run held it.
            SST-APL013: a temporary artifact would publish to a production-like target.
            SST-APL014: a temporary artifact now shadows the permanent object of its name.
            SST-APL015: a temporary artifact's alias was ignored.
            SST-APL900: the outcomes do not account for every planned change.
            Each failed change reports the diagnostic its error names; see `_outcome_diagnostic`.
        """
        run_id = self._clock.new_run_id()
        started = self._clock.now_iso()
        refusal = _refusal(changeset, options)
        if refusal is not None:
            return self._refused(refusal, run_id, started)
        lease = RunLease(
            self._state_store,
            self._port,
            self._state_table,
            changeset.target.name,
            LockClaim(run_id, self._actor, self._host, self._lock_policy.ttl_seconds),
            self._lock_policy,
        )
        locked, lock_diagnostics = lease.acquire(break_stale=options.break_stale_lock)
        reported = (*changeset.diagnostics, *temporary_notes(changeset), *lock_diagnostics)
        if not locked:
            return self._refused(reported, run_id, started)
        try:
            return self._run_locked(changeset, previous, options, reported, run_id, started, lease)
        finally:
            lease.release()

    def _refused(self, diagnostics: tuple[Diagnostic, ...], run_id: str, started: str) -> ApplyResult:
        """Report a run that executed nothing and wrote no state."""
        return ApplyResult((), DiagnosticBag(diagnostics), run_id, started, self._clock.now_iso(), False)

    def _run_locked(
        self,
        changeset: ChangeSet,
        previous: State,
        options: ApplyOptions,
        reported: tuple[Diagnostic, ...],
        run_id: str,
        started: str,
        lease: RunLease,
    ) -> ApplyResult:
        """Run the phases that need the lock: preflight, waves, accounting, then persist."""
        outcomes = self._preflight_relations(changeset.changes)
        wave_diagnostics: tuple[Diagnostic, ...] = ()
        if outcomes is None:
            outcomes, wave_diagnostics = _WaveRun(changeset, options, self._apply_one, lambda: lease.lost).run()
        diagnostics = DiagnosticBag((*reported, *wave_diagnostics, *_unaccounted(changeset, outcomes)))
        return self._persist(changeset, previous, outcomes, diagnostics, run_id, started)

    def _apply_one(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Apply one change: on `port` without a pool, else on a session leased for it alone.

        A session that cannot be opened fails the change with nothing written.
        """
        if self._sessions is None:
            return self._changes.apply(change, options)
        try:
            with self._sessions.lease() as port:
                return ChangeApplier(port, self._clock, self._lifecycle_handlers).apply(change, options)
        except SnowflakePortError as exc:
            return _failed(change, _exception_error(exc), _rendered_ddl(change))

    def _preflight_relations(self, changes: tuple[Change, ...]) -> tuple[ApplyOutcome, ...] | None:
        """Fail every change when a relation some create or update requires does not exist.

        The relations are checked in name order. Returns None when all of them exist.
        """
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
        return tuple(_failed(change, error, _rendered_ddl(change)) for change in changes)

    def _persist(
        self,
        changeset: ChangeSet,
        previous: State,
        outcomes: tuple[ApplyOutcome, ...],
        diagnostics: DiagnosticBag,
        run_id: str,
        started: str,
    ) -> ApplyResult:
        """Write the state the run leaves, remote then local, unless the plan had nothing to record."""
        # A report-only prune executes nothing, but state still records the run:
        # the entry is retained and the manifest moves on, or SST-MAN021 never clears.
        if not changeset.writes and not changeset.report_only:
            return ApplyResult(outcomes, diagnostics, run_id, started, self._clock.now_iso(), False)
        state = self._finish_state(changeset, previous, outcomes, run_id, started)
        self._write_remote_state(changeset, previous, state)
        self._state_store.write_local(state)
        return ApplyResult(
            outcomes,
            diagnostics,
            run_id,
            started,
            state.last_run.finished_at if state.last_run else self._clock.now_iso(),
            True,
        )

    def _finish_state(
        self,
        changeset: ChangeSet,
        previous: State,
        outcomes: tuple[ApplyOutcome, ...],
        run_id: str,
        started: str,
    ) -> State:
        """Build the state this run leaves: the entries `_applied_after` keeps, and the run itself."""
        finished = self._clock.now_iso()
        stamp = EntryStamp(run_id, finished, self._git_sha)
        applied = _applied_after(changeset, previous, outcomes, stamp, self._lifecycle_handlers)
        last_run = LastRun(run_id, started, finished, SST_VERSION, "apply", _run_outcome(outcomes), self._actor)
        return State(
            STATE_SCHEMA_VERSION,
            changeset.target,
            changeset.manifest_id,
            self._state_store.config_path,
            last_run,
            MappingProxyType(applied),
        )

    def _write_remote_state(self, changeset: ChangeSet, previous: State, state: State) -> None:
        """Write to the state table only the entries the run changed or retired, and its manifest."""
        self._port.write_state(
            self._state_table,
            changeset.target.name,
            changeset.manifest_id,
            state_write(previous.applied, state.applied),
        )


class _WaveRun:
    """Run a plan's dependency waves in order, collecting every outcome and its diagnostics.

    Within a wave, the policy decides what runs:

    - STOP_DEPENDENTS skips each change that depends on a failed or skipped one (SST-APL002),
      so a skip propagates to everything downstream, and runs the rest in parallel.
    - STOP_ALL runs the wave's changes one at a time; the first failure skips every change of
      the plan not yet run, in plan order, and no later wave runs.
    - CONTINUE runs every change in parallel, whatever failed before.

    The outcomes are recorded wave by wave: a wave's skipped changes, then those it ran, each
    in key order, and STOP_ALL's skips last, in plan order. An applied temporary artifact the
    plan saw a permanent object for reports SST-APL014; an outcome whose grants could not be
    verified reports SST-APL008 before its failure's diagnostic. Once `lost` reports the run
    lock broken, no further wave starts: every change without an outcome is skipped, in plan
    order, after one SST-APL011.
    """

    def __init__(
        self,
        changeset: ChangeSet,
        options: ApplyOptions,
        apply_one: _ApplyOne,
        lost: Callable[[], bool] = lambda: False,
    ) -> None:
        self._changeset = changeset
        self._options = options
        self._apply_one = apply_one
        self._lost = lost
        self._outcomes: list[ApplyOutcome] = []
        self._diagnostics: list[Diagnostic] = []
        self._failed_or_skipped: set[str] = set()

    def run(self) -> tuple[tuple[ApplyOutcome, ...], tuple[Diagnostic, ...]]:
        """Run every wave the policy allows; return the outcomes and the diagnostics, in order.

        Raises:
            ValueError: the changes' dependencies form a cycle.
        """
        for wave in dependency_waves(self._changeset.changes):
            if self._lost():
                self._diagnostics.append(D("SST-APL011", value="another run, which broke this run's lock"))
                self._skip_remaining()
                break
            runnable = self._runnable(wave)
            self._record(runnable, self._execute(runnable))
            if self._failed_or_skipped and self._options.on_failure is FailurePolicy.STOP_ALL:
                break
        return tuple(self._outcomes), tuple(self._diagnostics)

    def _runnable(self, wave: tuple[Change, ...]) -> list[Change]:
        """Return the wave's changes that may run; under STOP_DEPENDENTS, skip those a failure blocks."""
        runnable: list[Change] = []
        for change in wave:
            blockers = tuple(dependency for dependency in change.depends_on if dependency in self._failed_or_skipped)
            if blockers and self._options.on_failure is FailurePolicy.STOP_DEPENDENTS:
                self._outcomes.append(_skipped(change, _rendered_ddl(change)))
                self._diagnostics.append(D("SST-APL002", artifact=change.key, blocker=", ".join(blockers)))
                self._failed_or_skipped.add(change.key)
                continue
            runnable.append(change)
        return runnable

    def _execute(self, runnable: list[Change]) -> tuple[ApplyOutcome, ...]:
        """Apply the wave's runnable changes: one at a time up to a failure under STOP_ALL, else in parallel."""
        if self._options.on_failure is FailurePolicy.STOP_ALL:
            outcomes: list[ApplyOutcome] = []
            for change in runnable:
                outcome = self._apply_one(change, self._options)
                outcomes.append(outcome)
                if outcome.status is OutcomeStatus.FAILED:
                    break
            return tuple(outcomes)
        with ThreadPoolExecutor(max_workers=self._options.parallelism) as pool:
            return tuple(pool.map(lambda item: self._apply_one(item, self._options), runnable))

    def _record(self, runnable: list[Change], wave_outcomes: tuple[ApplyOutcome, ...]) -> None:
        """Record the wave's outcomes with their diagnostics; a failure under STOP_ALL skips the rest."""
        for change, outcome in zip(runnable, wave_outcomes, strict=False):
            self._outcomes.append(outcome)
            if _shadows(change, outcome):
                self._diagnostics.append(D("SST-APL014", artifact=change.key))
            if outcome.grants is GrantCheck.UNREADABLE:
                self._diagnostics.append(D("SST-APL008", artifact=change.key))
            if outcome.status is OutcomeStatus.FAILED:
                self._failed_or_skipped.add(change.key)
                self._diagnostics.append(_outcome_diagnostic(change, outcome))
                if self._options.on_failure is FailurePolicy.STOP_ALL:
                    self._skip_remaining()
                    break

    def _skip_remaining(self) -> None:
        """Skip, in plan order, every change of the plan that has no outcome yet."""
        done = {outcome.key for outcome in self._outcomes}
        self._outcomes.extend(
            _skipped(change, _rendered_ddl(change)) for change in self._changeset.changes if change.key not in done
        )


def _shadows(change: Change, outcome: ApplyOutcome) -> bool:
    """Report whether an applied temporary artifact took the name of a permanent object plan saw."""
    rendered = change.rendered
    return (
        outcome.status is OutcomeStatus.APPLIED
        and rendered is not None
        and rendered.temporary
        and change.observed is not None
    )


def _refusal(changeset: ChangeSet, options: ApplyOptions) -> tuple[Diagnostic, ...] | None:
    """Return what a plan is refused with before the lock is taken; None when it may run.

    Diagnostics:
        SST-APL003: a blocked change, unless the policy is CONTINUE; one per blocked change.
        SST-APL013: a temporary artifact against a production-like target; one per artifact.
    """
    if changeset.diagnostics.has_errors:
        return tuple(changeset.diagnostics)
    refused_temporary = temporary_refusal(changeset, options)
    if refused_temporary:
        return (*changeset.diagnostics, *refused_temporary)
    if changeset.blocked and options.on_failure is not FailurePolicy.CONTINUE:
        return (
            *changeset.diagnostics,
            *(D("SST-APL003", artifact=change.key, count=len(change.diagnostics) or 1) for change in changeset.blocked),
        )
    return None


def _unaccounted(changeset: ChangeSet, outcomes: tuple[ApplyOutcome, ...]) -> tuple[Diagnostic, ...]:
    """Report SST-APL900 when the outcomes do not account for every planned change, else nothing."""
    if len(outcomes) != len(changeset.changes):
        return (D("SST-APL900", found=len(outcomes), expected=len(changeset.changes)),)
    return ()
