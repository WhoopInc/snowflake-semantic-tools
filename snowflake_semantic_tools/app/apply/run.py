"""The apply run: refuse or lock, preflight, run the dependency waves, account, and persist state.

`ApplyArtifacts.run` holds the apply lock from the moment it takes it until the run ends, and
writes state only after every wave ran: remote first, then local, so local state never claims
what the state table does not hold.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType
from typing import Callable, Mapping

from snowflake_semantic_tools.app.apply.errors import _failed, _outcome_diagnostic, _rendered_ddl, _skipped
from snowflake_semantic_tools.app.apply.one import ChangeApplier
from snowflake_semantic_tools.app.apply.state import EntryStamp, _applied_after, _run_outcome
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag
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
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake import ClockPort, SnowflakePort, StateStore
from snowflake_semantic_tools.domain.state import SST_VERSION, STATE_SCHEMA_VERSION, LastRun, State

_ApplyOne = Callable[[Change, ApplyOptions], ApplyOutcome]


class ApplyArtifacts:
    """Execute a reviewed ChangeSet against Snowflake and record what it wrote in state.

    A `ChangeApplier` applies each change, re-checking what plan saw before it writes. The use
    case keeps nothing between runs; the state store's lock keeps two runs from applying at once.
    """

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
        self._changes = ChangeApplier(port, clock, self._lifecycle_handlers)

    def run(self, changeset: ChangeSet, previous: State, options: ApplyOptions = ApplyOptions()) -> ApplyResult:
        """Execute the plan's writes under the apply lock and record their outcomes in state.

        The run goes through these phases in order:

        1. Refusal, before the lock: a plan with an error diagnostic is refused with it, and so
           is a plan with blocked changes unless the policy is CONTINUE.
        2. Lock: a lock another run holds refuses the run; an expired one is taken over when
           `options.break_stale_lock` allows it.
        3. Preflight: when a relation some create or update requires does not exist, every
           change fails with SST-PRT005 and nothing runs.
        4. Waves: the dependency waves run in order, as `_WaveRun` describes.
        5. Accounting: the outcomes must account for every planned change.
        6. Persist: unless the plan has neither writes nor report-only prunes, the state the run
           leaves is written to the state table, then locally.

        A refused run never takes the lock; once taken, it is released however the run ends,
        including when writing state raises. State keeps every object a statement wrote, even
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
            SST-APL011: another run holds the lock.
            SST-APL900: the outcomes do not account for every planned change.
            Each failed change reports the diagnostic its error names; see `_outcome_diagnostic`.
        """
        run_id = self._clock.new_run_id()
        started = self._clock.now_iso()
        refusal = _refusal(changeset, options)
        if refusal is not None:
            return self._refused(refusal, run_id, started)
        locked, lock_diagnostics = self._lock(run_id, options)
        reported = (*changeset.diagnostics, *lock_diagnostics)
        if not locked:
            return self._refused(reported, run_id, started)
        try:
            return self._run_locked(changeset, previous, options, reported, run_id, started)
        finally:
            self._state_store.release_lock(run_id)

    def _lock(self, run_id: str, options: ApplyOptions) -> tuple[bool, tuple[Diagnostic, ...]]:
        """Take the apply lock for this run, reporting who held it.

        Returns:
            Whether the run holds the lock, with SST-APL011 when another run holds it and
            SST-APL010 when an expired lock was taken over.
        """
        locked, holder, broke_stale = self._state_store.acquire_lock(
            run_id,
            break_stale=options.break_stale_lock,
        )
        if not locked:
            return False, (D("SST-APL011", value=holder or "another run"),)
        if broke_stale:
            return True, (D("SST-APL010", value=holder or "expired run"),)
        return True, ()

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
    ) -> ApplyResult:
        """Run the phases that need the lock: preflight, waves, accounting, then persist."""
        outcomes = self._preflight_relations(changeset.changes)
        wave_diagnostics: tuple[Diagnostic, ...] = ()
        if outcomes is None:
            outcomes, wave_diagnostics = _WaveRun(changeset, options, self._changes.apply).run()
        diagnostics = DiagnosticBag((*reported, *wave_diagnostics, *_unaccounted(changeset, outcomes)))
        return self._persist(changeset, previous, outcomes, diagnostics, run_id, started)

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
        self._write_remote_state(changeset, state)
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

    def _write_remote_state(self, changeset: ChangeSet, state: State) -> None:
        self._port.write_state(
            self._state_table,
            changeset.target.name,
            changeset.manifest_id,
            state.applied,
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
    in key order, and STOP_ALL's skips last, in plan order. An outcome whose grants could not be
    verified reports SST-APL008 before its failure's diagnostic.
    """

    def __init__(self, changeset: ChangeSet, options: ApplyOptions, apply_one: _ApplyOne) -> None:
        self._changeset = changeset
        self._options = options
        self._apply_one = apply_one
        self._outcomes: list[ApplyOutcome] = []
        self._diagnostics: list[Diagnostic] = []
        self._failed_or_skipped: set[str] = set()

    def run(self) -> tuple[tuple[ApplyOutcome, ...], tuple[Diagnostic, ...]]:
        """Run every wave the policy allows; return the outcomes and the diagnostics, in order.

        Raises:
            ValueError: the changes' dependencies form a cycle.
        """
        for wave in dependency_waves(self._changeset.changes):
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
        for change, outcome in zip(runnable, wave_outcomes):
            self._outcomes.append(outcome)
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


def _refusal(changeset: ChangeSet, options: ApplyOptions) -> tuple[Diagnostic, ...] | None:
    """Return what a plan is refused with before the lock is taken; None when it may run.

    Diagnostics:
        SST-APL003: a blocked change, unless the policy is CONTINUE; one per blocked change.
    """
    if changeset.diagnostics.has_errors:
        return tuple(changeset.diagnostics)
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
