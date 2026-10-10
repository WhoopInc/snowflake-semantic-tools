"""The apply run: refuse or lock, verify state, preflight, run the waves, account, and persist.

`ApplyArtifacts.run` holds the run lease, the local and the remote lock, from the moment it
takes it until the run ends. Under the lock it first re-reads the state table, and refuses a
plan whose state changed since it was read. It writes state only after every wave ran, fenced
by the lease's token: remote first, then local, so local state never claims what the state
table does not hold. Changes run on sessions leased from a pool, one per worker, never on the
command's own session, and the heartbeat on one of its own; losing the lock halts them all.
An interrupt stops the waves, not the run: what finished is recorded before the lock is released.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import replace
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.apply.errors import (
    _cause_diagnostic,
    _exception_error,
    _failed,
    _outcome_diagnostic,
    _rendered_ddl,
    _skipped,
)
from snowflake_semantic_tools.app.apply.lock import BROKEN_BY_ANOTHER_RUN, LockPolicy, RunLease
from snowflake_semantic_tools.app.apply.one import ChangeApplier
from snowflake_semantic_tools.app.apply.stages import UploadStages
from snowflake_semantic_tools.app.apply.state import EntryStamp, _applied_after, _run_outcome
from snowflake_semantic_tools.app.apply.temporary import temporary_notes, temporary_refusal
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.app.state import table_state
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
from snowflake_semantic_tools.domain.ports.timing import NO_PHASES, PhaseTimer
from snowflake_semantic_tools.domain.state import SST_VERSION, STATE_SCHEMA_VERSION, LastRun, State
from snowflake_semantic_tools.domain.state.lock import LockClaim, LockFence, state_write
from snowflake_semantic_tools.domain.validate.publication import channel_outcome_diagnostics

_ApplyOne = Callable[[Change, ApplyOptions], ApplyOutcome]
# Whether the run lost its lock: who holds it now, as SST-APL011 names them; None while it holds it.
_Lost = Callable[[], str | None]
_DEFAULT_LOCK_POLICY = LockPolicy()


class ApplyPort(CatalogPublicationPort, StatePort, Protocol):
    """The Snowflake roles apply uses: a catalog publication port that also keeps the state table."""


class ApplyArtifacts:
    """Execute a reviewed ChangeSet against Snowflake and record what it wrote in state.

    A `ChangeApplier` applies each change, re-checking what plan saw before it writes. The use
    case keeps nothing between runs; the run lease keeps two runs from applying at once.

    Args:
        sessions: Where workers lease their sessions, one per change; None runs every change on
            `port`, one statement at a time.
        heartbeat: The session the lock heartbeat extends the lock on, used for nothing else;
            None extends it on `port`.
        host: The machine the run lock records; empty when it is unknown.
        phases: Times `lock` (taking it), `publish` (the preflight and the waves), and
            `state_write`.
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
        heartbeat: StatePort | None = None,
        lock_policy: LockPolicy = _DEFAULT_LOCK_POLICY,
        phases: PhaseTimer = NO_PHASES,
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
        self._heartbeat = heartbeat
        self._lock_policy = lock_policy
        self._phases = phases

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
        3. Verify: the state table is read again under the lock; when its entries for the
           target differ from `previous`, which the plan was made from, the run is refused.
        4. Preflight: when a relation some create or update requires does not exist, every
           change fails with SST-PRT005 and nothing runs.
        5. Waves: the dependency waves run in order, as `_WaveRun` describes.
        6. Accounting: the outcomes must account for every planned change.
        7. Persist: unless the plan has neither writes nor report-only prunes, the state the run
           leaves is written to the state table, fenced by the run's lock, then locally.

        An interrupt -- `KeyboardInterrupt`, which Ctrl-C raises and the CLI raises for SIGTERM
        and SIGHUP -- during the waves ends them as `_WaveRun` describes; the run then accounts
        and persists as usual, and its result is `interrupted`. An interrupt in any other phase
        propagates, the lock released first.

        A refused run never takes the lock, or releases it at once; once taken, it is released
        however the run ends, including when writing state raises. The moment the run loses
        its lock, its worker sessions refuse every further statement and no further change
        starts; it then writes no state, since the run that broke the lock owns the target's
        state. State otherwise keeps every object a statement wrote, even when its change then
        failed, so a partial write stays SST's and the next plan sees it.
        The plan's own diagnostics come first in the result, then each phase's in order.

        Raises:
            ValueError: the changes' dependencies form a cycle.
            SnowflakePortError: reading or writing the state table failed; local state is not
                written.
            KeyboardInterrupt: an interrupt outside the waves, or a third one while the run
                waited for its running changes.

        Diagnostics:
            SST-APL002: a change was skipped because a change it depends on failed or was skipped.
            SST-APL003: the plan has a blocked change and the policy is not CONTINUE.
            SST-APL008: an object's grants could not be verified.
            SST-APL010: the run took over an expired lock.
            SST-APL011: another run holds the lock, or broke it while this run held it.
            SST-APL012: the target's state changed between the plan and the lock.
            SST-APL013: a temporary artifact would publish to a production-like target.
            SST-APL014: a temporary artifact now shadows the permanent object of its name.
            SST-APL015: a temporary artifact's alias was ignored.
            SST-APL900: the outcomes do not account for every planned change.
            SST-VAL828: a skill, plugin or profile change, a publication channel's, has no
                outcome or several, so the run's result would not show that channel's state.
            Each failed change reports the diagnostic its error names; see `_outcome_diagnostic`.
            A change Snowflake refused also reports the refusal under its SNO code; see
            `_cause_diagnostic`.
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
            heartbeat_port=self._heartbeat,
            on_lost=self._halt,
        )
        # Released from the moment the lock may be taken: an interrupt that lands between taking
        # it and the run's first statement must not leave it held. Releasing a lease that was
        # refused releases nothing another run holds.
        try:
            with self._phases.phase("lock"):
                locked, lock_diagnostics = lease.acquire(break_stale=options.break_stale_lock)
            reported = (*changeset.diagnostics, *temporary_notes(changeset), *lock_diagnostics)
            if not locked:
                return self._refused(reported, run_id, started)
            changed = self._state_changed(changeset, previous)
            if changed is not None:
                return self._refused((*reported, changed), run_id, started)
            return self._run_locked(changeset, previous, options, reported, run_id, started, lease)
        finally:
            lease.release()

    def _halt(self, reason: str) -> None:
        """Stop every worker session before its next statement, once the run lost its lock."""
        if self._sessions is not None:
            self._sessions.halt(f"the run lock was lost: {reason} holds it")

    def _halt_interrupted(self) -> None:
        """Stop every worker session before its next statement, once the run was interrupted again."""
        if self._sessions is not None:
            self._sessions.halt("the run was interrupted")

    def _state_changed(self, changeset: ChangeSet, previous: State) -> Diagnostic | None:
        """Read the target's state again under the lock; SST-APL012 when it is not the state the plan read.

        Diagnostics:
            SST-APL012: the state table's entries for the target changed since the plan, or
                it can no longer be read.
        """
        current = table_state(self._port, self._state_table, changeset.target, previous.config_path)
        if current is not None and current.applied == previous.applied:
            return None
        value = f"its state in {self._state_table.sql}"
        return D("SST-APL012", subject=changeset.target.name, artifact=f"target {changeset.target.name}", value=value)

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
        """Run the phases that need the lock: preflight, waves, accounting, then persist.

        The waves share one `UploadStages`, so each stage the run uploads to is created at most once.
        """
        with self._phases.phase("publish"):
            outcomes = self._preflight_relations(changeset.changes, options.parallelism)
        wave_diagnostics: tuple[Diagnostic, ...] = ()
        stages = UploadStages()

        def lost() -> str | None:
            return lease.lost_reason if lease.lost else None

        def apply_one(change: Change, options: ApplyOptions) -> ApplyOutcome:
            return self._apply_one(change, options, stages)

        waves = _WaveRun(changeset, options, apply_one, lost, on_second_interrupt=self._halt_interrupted)
        if outcomes is None:
            with self._phases.phase("publish"):
                outcomes, wave_diagnostics = waves.run()
        accounted = (
            *_unaccounted(changeset, outcomes),
            *channel_outcome_diagnostics(
                ((change.key, change.artifact_type) for change in changeset.changes),
                [outcome.key for outcome in outcomes],
            ),
        )
        diagnostics = DiagnosticBag((*reported, *wave_diagnostics, *accounted))
        assert lease.fence is not None
        with self._phases.phase("state_write"):
            result = self._persist(changeset, previous, outcomes, diagnostics, run_id, started, lease.fence)
        return replace(result, interrupted=waves.interrupted)

    def _apply_one(self, change: Change, options: ApplyOptions, stages: UploadStages) -> ApplyOutcome:
        """Apply one change: on `port` without a pool, else on a session leased for it alone.

        On a leased session the change's composite handler runs there too. A session that
        cannot be opened fails the change with nothing written.
        """
        if self._sessions is None:
            return ChangeApplier(self._port, self._clock, self._lifecycle_handlers, stages=stages).apply(
                change, options
            )
        try:
            with self._sessions.lease() as port:
                return ChangeApplier(port, self._clock, self._lifecycle_handlers, leased=True, stages=stages).apply(
                    change, options
                )
        except SnowflakePortError as exc:
            return _failed(change, _exception_error(exc), _rendered_ddl(change))

    def _preflight_relations(self, changes: tuple[Change, ...], parallelism: int) -> tuple[ApplyOutcome, ...] | None:
        """Fail every change when a relation some create or update requires does not exist.

        The relations are checked in name order, at once on worker sessions when there is a
        pool, so the check waits for one lookup rather than one after another. Returns None
        when all of them exist.
        """
        relations = sorted(
            {
                relation
                for change in changes
                if change.action in (Action.CREATE, Action.UPDATE) and change.rendered is not None
                for relation in change.rendered.required_relations
            }
        )
        present = self._relations_exist(relations, parallelism)
        missing = tuple(relation for relation, exists in zip(relations, present, strict=True) if not exists)
        if not missing:
            return None
        detail = "missing required relations: " + ", ".join(relation.sql for relation in missing)
        error = ClassifiedError("SST-PRT005", detail, ErrorKind.NOT_FOUND)
        return tuple(_failed(change, error, _rendered_ddl(change)) for change in changes)

    def _relations_exist(self, relations: list[QualifiedName], parallelism: int) -> tuple[bool, ...]:
        """Whether each relation exists, in order: on `port` without a pool, else on leased sessions at once."""
        sessions = self._sessions
        if sessions is None or len(relations) < 2:
            return tuple(self._port.object_exists("TABLE OR VIEW", relation) for relation in relations)

        def exists(relation: QualifiedName) -> bool:
            with sessions.lease() as port:
                return port.object_exists("TABLE OR VIEW", relation)

        pool = ThreadPoolExecutor(max_workers=parallelism)
        try:
            return tuple(pool.map(exists, relations))
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _persist(
        self,
        changeset: ChangeSet,
        previous: State,
        outcomes: tuple[ApplyOutcome, ...],
        diagnostics: DiagnosticBag,
        run_id: str,
        started: str,
        fence: LockFence,
    ) -> ApplyResult:
        """Write the state the run leaves, remote then local, unless the plan had nothing to record.

        Diagnostics:
            SST-APL011: the state table refused the write because the run no longer holds
                its lock; reported unless the waves already reported the loss.
        """
        # A report-only prune executes nothing, but state still records the run, and the
        # object plan observed when no entry names it yet.
        if not changeset.writes and not changeset.report_only:
            return ApplyResult(outcomes, diagnostics, run_id, started, self._clock.now_iso(), False)
        state = self._finish_state(changeset, previous, outcomes, run_id, started)
        if not self._write_remote_state(changeset, previous, state, fence):
            refused = () if any(item.code == "SST-APL011" for item in diagnostics) else (lost_lock(),)
            return ApplyResult(
                outcomes, DiagnosticBag((*diagnostics, *refused)), run_id, started, self._clock.now_iso(), False
            )
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

    def _write_remote_state(self, changeset: ChangeSet, previous: State, state: State, fence: LockFence) -> bool:
        """Write to the state table only the entries the run changed or retired, and its manifest.

        Returns:
            Whether the write committed; False when the run's fence no longer holds the lock.
        """
        return self._port.write_state(
            self._state_table,
            changeset.target.name,
            changeset.manifest_id,
            state_write(previous.applied, state.applied),
            fence,
        )


class _WaveRun:
    """Run a plan's changes in dependency order, collecting every outcome and its diagnostics.

    The policy decides what runs, and when:

    - STOP_DEPENDENTS starts each change once every change it depends on ended, up to
      `parallelism` at once, and skips one that depends on a failed or skipped one
      (SST-APL002), so a skip propagates to everything downstream.
    - STOP_ALL runs the dependency waves in order and their changes one at a time; the first
      failure skips every change of the plan not yet run, in plan order, and no later wave runs.
    - CONTINUE starts each change as STOP_DEPENDENTS does, whatever failed before.

    A change never waits for one it does not depend on: the waves order only what is recorded.
    The outcomes are recorded wave by wave, once the changes ended: a wave's skipped changes,
    then those it ran, each in key order, and STOP_ALL's skips last, in plan order -- the same
    record however the changes' durations interleave. An applied temporary artifact the
    plan saw a permanent object for reports SST-APL014; an outcome whose grants could not be
    verified reports SST-APL008 before its failure's diagnostic, once: a change that failed
    because its grants could not be read reports it as that failure only. `lost` is checked before
    every change starts: once it reports the run lock lost, no further change starts, and once
    the running ones ended every change without an outcome is skipped, in plan order, after one
    SST-APL011 naming the run `lost` says holds the lock.

    The changes run on worker threads, even one at a time, so the calling thread only waits,
    and an interrupt (`KeyboardInterrupt`) reaches it there. From then on no further change
    starts; the changes already running are waited for, and each outcome they end with is
    recorded as usual, so state keeps what they wrote; every change without an outcome is then
    skipped, in plan order, and `interrupted` is True. A second interrupt while it waits calls
    `on_second_interrupt`, which halts the running changes' sessions before their next
    statement, and waits again; a third propagates.
    """

    def __init__(
        self,
        changeset: ChangeSet,
        options: ApplyOptions,
        apply_one: _ApplyOne,
        lost: _Lost = lambda: None,
        *,
        on_second_interrupt: Callable[[], None] = lambda: None,
    ) -> None:
        self._changeset = changeset
        self._options = options
        self._apply_one = apply_one
        self._lost = lost
        self._on_second_interrupt = on_second_interrupt
        self._outcomes: list[ApplyOutcome] = []
        self._diagnostics: list[Diagnostic] = []
        self._failed_or_skipped: set[str] = set()
        # The changes submitted so far, in order, with what each will end with.
        self._started: list[tuple[Change, Future[ApplyOutcome | None]]] = []
        self._interrupted = False
        # Outside STOP_ALL: the waves the record follows, what each change that ended ended
        # with (None when it did not start), and the dependencies that blocked each skipped one.
        self._waves: tuple[tuple[Change, ...], ...] | None = None
        self._ended: dict[str, ApplyOutcome | None] = {}
        self._blocked: dict[str, tuple[str, ...]] = {}

    @property
    def interrupted(self) -> bool:
        """Whether an interrupt ended the waves before every change the policy allows had run."""
        return self._interrupted

    def run(self) -> tuple[tuple[ApplyOutcome, ...], tuple[Diagnostic, ...]]:
        """Run every wave the policy allows; return the outcomes and the diagnostics, in order.

        Raises:
            ValueError: the changes' dependencies form a cycle.
            KeyboardInterrupt: a third interrupt, while the run waited for its running changes.
        """
        try:
            self._run_waves()
        except KeyboardInterrupt:
            self._interrupted = True
            self._wait_for_started()
            self._record_started()
            self._skip_remaining()
        return tuple(self._outcomes), tuple(self._diagnostics)

    def _run_waves(self) -> None:
        """Run the changes until the lock is lost, a failure stops them, or none is left."""
        waves = dependency_waves(self._changeset.changes)
        if self._options.on_failure is not FailurePolicy.STOP_ALL:
            self._run_as_ready(waves)
            return
        for wave in waves:
            if self._lost() is None:
                self._record(list(wave), self._execute(wave))
            holder = self._lost()
            if holder is not None:
                self._diagnostics.append(lost_lock(holder))
                self._skip_remaining()
                break
            if self._failed_or_skipped:
                break

    def _run_as_ready(self, waves: tuple[tuple[Change, ...], ...]) -> None:
        """Start each change once those it depends on ended, then record them all wave by wave.

        An interrupt cancels every change not yet started and propagates at once, without
        waiting for those running: `run` waits for them, and records what they ended with.
        """
        self._waves = waves
        planned = {change.key for wave in waves for change in wave}
        waiting = [change for wave in waves for change in wave]
        running: dict[Future[ApplyOutcome | None], Change] = {}
        pool = ThreadPoolExecutor(max_workers=self._options.parallelism)
        try:
            while True:
                if self._lost() is None:
                    waiting = self._start_ready(pool, waiting, planned, running)
                if not running:
                    break
                ended, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in ended:
                    self._end(running.pop(future), future.result())
        except KeyboardInterrupt:
            # Set before the queued changes are cancelled, so a worker taking one meanwhile skips it.
            self._interrupted = True
            raise
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
        self._record_ended()
        holder = self._lost()
        if holder is not None:
            self._diagnostics.append(lost_lock(holder))
            self._skip_remaining()

    def _start_ready(
        self,
        pool: ThreadPoolExecutor,
        waiting: list[Change],
        planned: set[str],
        running: dict[Future[ApplyOutcome | None], Change],
    ) -> list[Change]:
        """Start, in plan order, each waiting change whose dependencies all ended; return the rest.

        Under STOP_DEPENDENTS a change a failed or skipped dependency blocks is skipped instead,
        which can decide its own dependents in turn, so the waiting changes are read again until
        a reading decides none.
        """
        deciding = True
        while deciding:
            deciding = False
            undecided: list[Change] = []
            for change in waiting:
                if any(
                    key in planned and key not in self._ended and key not in self._blocked for key in change.depends_on
                ):
                    undecided.append(change)
                    continue
                blockers = tuple(key for key in change.depends_on if key in self._failed_or_skipped)
                if blockers and self._options.on_failure is FailurePolicy.STOP_DEPENDENTS:
                    self._blocked[change.key] = blockers
                    self._failed_or_skipped.add(change.key)
                    deciding = True
                    continue
                running[self._submit(pool, change)] = change
            waiting = undecided
        return waiting

    def _end(self, change: Change, outcome: ApplyOutcome | None) -> None:
        """Note what a change ended with; a failure blocks its dependents from here on."""
        self._ended[change.key] = outcome
        if outcome is not None and outcome.status is OutcomeStatus.FAILED:
            self._failed_or_skipped.add(change.key)

    def _record_ended(self) -> None:
        """Record every change that ended or was skipped, wave by wave: the skipped, then the ran.

        A change recorded already is not recorded again, so an interrupt that lands while this
        records leaves `run` to record only the rest.
        """
        assert self._waves is not None
        recorded = {outcome.key for outcome in self._outcomes}
        for wave in self._waves:
            for change in wave:
                blockers = self._blocked.get(change.key)
                if blockers is not None and change.key not in recorded:
                    self._outcomes.append(_skipped(change, _rendered_ddl(change)))
                    self._diagnostics.append(D("SST-APL002", artifact=change.key, blocker=", ".join(blockers)))
            ran = [change for change in wave if change.key in self._ended and change.key not in recorded]
            self._record(ran, tuple(self._ended[change.key] for change in ran))

    def _execute(self, wave: tuple[Change, ...]) -> tuple[ApplyOutcome | None, ...]:
        """Apply a wave's changes one at a time, up to the first failure, as STOP_ALL runs them.

        A change that would start once the lock is lost, or the run interrupted, does not
        start; its place holds None. An interrupt cancels every change not yet started and
        propagates at once, without waiting for those running: `run` waits for them.
        """
        self._started = []
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            outcomes: list[ApplyOutcome | None] = []
            for change in wave:
                outcome = self._submit(pool, change).result()
                outcomes.append(outcome)
                if outcome is None or outcome.status is OutcomeStatus.FAILED:
                    break
            return tuple(outcomes)
        except KeyboardInterrupt:
            # Set before the queued changes are cancelled, so a worker taking one meanwhile skips it.
            self._interrupted = True
            raise
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _submit(self, pool: ThreadPoolExecutor, change: Change) -> Future[ApplyOutcome | None]:
        future = pool.submit(self._attempt, change)
        self._started.append((change, future))
        return future

    def _attempt(self, change: Change) -> ApplyOutcome | None:
        """Apply one change unless the lock is lost or the run interrupted; None when it did not start."""
        if self._interrupted or self._lost() is not None:
            return None
        return self._apply_one(change, self._options)

    def _wait_for_started(self) -> None:
        """Wait for the interrupted wave's running changes; a second interrupt halts them first."""
        # A change cancelled before it started is never done for `wait`, which would block on it
        # for ever; every other was taken by a worker, so it ends.
        started = [future for _, future in self._started if not future.cancelled()]
        try:
            wait(started)
        except KeyboardInterrupt:
            self._on_second_interrupt()
            wait(started)

    def _record_started(self) -> None:
        """Record how each change the interrupted run started ended, unless its outcome is recorded already."""
        recorded = {outcome.key for outcome in self._outcomes}
        ended = [
            (change, future.result())
            for change, future in self._started
            if change.key not in recorded and not future.cancelled() and future.exception() is None
        ]
        if self._waves is not None:
            for change, outcome in ended:
                self._end(change, outcome)
            self._record_ended()
            return
        self._record([change for change, _ in ended], tuple(outcome for _, outcome in ended))

    def _record(self, runnable: list[Change], wave_outcomes: tuple[ApplyOutcome | None, ...]) -> None:
        """Record the wave's outcomes with their diagnostics; a failure under STOP_ALL skips the rest."""
        for change, outcome in zip(runnable, wave_outcomes, strict=False):
            if outcome is None:
                continue
            self._outcomes.append(outcome)
            if _shadows(change, outcome):
                self._diagnostics.append(D("SST-APL014", artifact=change.key))
            if outcome.grants is GrantCheck.UNREADABLE and not _reports_unreadable_grants(outcome):
                self._diagnostics.append(D("SST-APL008", artifact=change.key))
            if outcome.status is OutcomeStatus.FAILED:
                self._failed_or_skipped.add(change.key)
                self._diagnostics.append(_outcome_diagnostic(change, outcome))
                cause = _cause_diagnostic(change, outcome)
                if cause is not None:
                    self._diagnostics.append(cause)
                if self._options.on_failure is FailurePolicy.STOP_ALL:
                    self._skip_remaining()
                    break

    def _skip_remaining(self) -> None:
        """Skip, in plan order, every change of the plan that has no outcome yet."""
        done = {outcome.key for outcome in self._outcomes}
        self._outcomes.extend(
            _skipped(change, _rendered_ddl(change)) for change in self._changeset.changes if change.key not in done
        )


def lost_lock(holder: str = BROKEN_BY_ANOTHER_RUN) -> Diagnostic:
    """Report that the run lost its lock to `holder`.

    Diagnostics:
        SST-APL011: the run lock this run held was broken, or can no longer be proven held.
    """
    return D("SST-APL011", value=holder)


def _shadows(change: Change, outcome: ApplyOutcome) -> bool:
    """Report whether an applied temporary artifact took the name of a permanent object plan saw."""
    rendered = change.rendered
    return (
        outcome.status is OutcomeStatus.APPLIED
        and rendered is not None
        and rendered.temporary
        and change.observed is not None
    )


def _reports_unreadable_grants(outcome: ApplyOutcome) -> bool:
    """Report whether a failed outcome's own diagnostic already says its grants could not be read."""
    return outcome.status is OutcomeStatus.FAILED and outcome.error is not None and outcome.error.code == "SST-APL008"


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
