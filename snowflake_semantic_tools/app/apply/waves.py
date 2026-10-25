"""The dependency waves of one apply run: what starts when, what is skipped, and how it is recorded.

`_WaveRun` runs a plan's changes on worker threads under the failure policy, reads the run
lock again before each change starts, and stops starting changes once the lock is lost or the
run is interrupted. `ApplyArtifacts` (`run`) holds the lock and persists what the waves recorded.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from snowflake_semantic_tools.app.apply.errors import (
    _cause_diagnostic,
    _exception_error,
    _failed,
    _outcome_diagnostic,
    _rendered_ddl,
    _skipped,
)
from snowflake_semantic_tools.app.apply.lock import BROKEN_BY_ANOTHER_RUN
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    Change,
    ChangeSet,
    FailurePolicy,
    GrantCheck,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.plan import dependency_waves
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

_ApplyOne = Callable[[Change, ApplyOptions], ApplyOutcome]
# Whether the run lost its lock: who holds it now, as SST-APL011 names them; None while it holds it.
_Lost = Callable[[], str | None]
# Read the run lock again: whether the run still holds it. Raises `SnowflakePortError` when the
# lock cannot be read.
_Verify = Callable[[], bool]


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
    because its grants could not be read reports it as that failure only. An applied agent
    update whose default could not be pinned by name reports SST-APL107. `lost` is checked, and the lock read again
    through `verify`, before every change starts: once the lock is lost, no further change starts,
    and once the running ones ended every change without an outcome is skipped, in plan order,
    after one SST-APL011 naming the run `lost` says holds the lock. A lock `verify` cannot read
    fails the change it was read for, which then wrote nothing.

    A change skipped because a change it depends on failed or was skipped reports SST-APL002,
    naming the failures it descends from, unless the plan had nothing for it to do (NOOP): it is
    skipped quietly, and still blocks the changes that depend on it.

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
        verify: _Verify = lambda: True,
        on_second_interrupt: Callable[[], None] = lambda: None,
    ) -> None:
        self._changeset = changeset
        self._options = options
        self._apply_one = apply_one
        self._lost = lost
        self._verify = verify
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
            # Set before the queued changes are canceled, so a worker taking one meanwhile skips it.
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
                blockers = self._root_failures(change)
                if blockers and self._options.on_failure is FailurePolicy.STOP_DEPENDENTS:
                    self._blocked[change.key] = blockers
                    self._failed_or_skipped.add(change.key)
                    deciding = True
                    continue
                running[self._submit(pool, change)] = change
            waiting = undecided
        return waiting

    def _root_failures(self, change: Change) -> tuple[str, ...]:
        """Return the failed changes a change's failed or skipped dependencies descend from, once each.

        A dependency that was itself skipped stands for the failures that skipped it, so SST-APL002
        names a change that failed, never one that was only skipped.
        """
        return tuple(
            dict.fromkeys(
                root
                for key in change.depends_on
                if key in self._failed_or_skipped
                for root in self._blocked.get(key, (key,))
            )
        )

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
                    if change.action is not Action.NOOP:
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
            # Set before the queued changes are canceled, so a worker taking one meanwhile skips it.
            self._interrupted = True
            raise
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _submit(self, pool: ThreadPoolExecutor, change: Change) -> Future[ApplyOutcome | None]:
        future = pool.submit(self._attempt, change)
        self._started.append((change, future))
        return future

    def _attempt(self, change: Change) -> ApplyOutcome | None:
        """Apply one change unless the lock is lost or the run interrupted; None when it did not start.

        The lock is read again first, so a run whose lock another run broke while it was paused
        starts nothing more, rather than writing until its next heartbeat finds out.
        """
        if self._interrupted or self._lost() is not None:
            return None
        if passes_through(change):
            # It writes nothing, so it needs no lock read to be written safely.
            return self._apply_one(change, self._options)
        try:
            if not self._verify():
                return None
        except SnowflakePortError as exc:
            return _failed(change, _exception_error(exc), _rendered_ddl(change))
        return self._apply_one(change, self._options)

    def _wait_for_started(self) -> None:
        """Wait for the interrupted wave's running changes; a second interrupt halts them first."""
        # A change canceled before it started is never done for `wait`, which would block on it
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
        """Record the wave's outcomes with their diagnostics; a failure under STOP_ALL skips the rest.

        Diagnostics:
            SST-APL107: an applied agent update whose default apply could not pin by name.
        """
        for change, outcome in zip(runnable, wave_outcomes, strict=False):
            if outcome is None:
                continue
            self._outcomes.append(outcome)
            if _shadows(change, outcome):
                self._diagnostics.append(D("SST-APL014", artifact=change.key))
            if outcome.grants is GrantCheck.UNREADABLE and not _reports_unreadable_grants(outcome):
                self._diagnostics.append(D("SST-APL008", artifact=change.key))
            if outcome.default_unpinned is not None:
                self._diagnostics.append(D("SST-APL107", artifact=change.key, detail=outcome.default_unpinned))
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


def passes_through(change: Change) -> bool:
    """Report whether a change is only reported: a no-op or blocked change with its rendered artifact.

    It still runs in its wave, so what depends on it waits for it and a failure it descends
    from still skips what follows; but it reads nothing and writes nothing, so it takes no lock
    read, and the live progress neither counts nor shows it. A change without its rendered
    artifact is not one: its handler decides, and may fail it.
    """
    return change.action in (Action.NOOP, Action.BLOCKED) and change.rendered is not None


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
