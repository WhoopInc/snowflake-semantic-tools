"""Apply under the run lease, on pooled sessions: contention, heartbeat, a lost lock, and the state delta.

Every heartbeat here is stepped by hand through `SteppedTicker`, so no test depends on how fast
a thread runs: a beat happens exactly when the test steps it, and returns once it was handled.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.apply.lock import EventTicker, LockPolicy, RunLease
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    ApplyResult,
    Change,
    ExecResult,
    ExecutionError,
    FailurePolicy,
    OutcomeStatus,
    OwnershipMarker,
    ShowRow,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry, State
from snowflake_semantic_tools.domain.state.lock import LockClaim, LockFence
from tests.helpers.app_ports import FixedClock, InMemorySnowflake, InMemoryStateStore
from tests.helpers.artifact_builders import change, changeset, manifest, rendered, state, target
from tests.helpers.run_locks import SteppedTicker

STATE_TABLE = rendered("SST_STATE").target
LOST = "another run, which broke this run's lock holds the apply lock"


def stepped(ticker: SteppedTicker | None = None, *, ttl: int = 60) -> LockPolicy:
    """A policy whose heartbeat beats only when `ticker` is stepped; one beat is one second."""
    made = ticker or SteppedTicker()
    return LockPolicy(ttl_seconds=ttl, heartbeat_seconds=1, ticker=lambda: made)


class RunClock(FixedClock):
    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def new_run_id(self) -> str:
        return self.run_id


class SlowSnowflake(InMemorySnowflake):
    """Holds each script until `release` is set, and records how many ran on it at once."""

    def __init__(self, release: threading.Event | None = None) -> None:
        super().__init__()
        self.release = release
        self.entered = threading.Event()
        self.running = 0
        self.most_at_once = 0
        self._guard = threading.Lock()

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        with self._guard:
            self.running += 1
            self.most_at_once = max(self.most_at_once, self.running)
        self.entered.set()
        try:
            if self.release is not None:
                assert self.release.wait(timeout=5)
            return super().execute_script(statements)
        finally:
            with self._guard:
                self.running -= 1


class ListPool:
    """A session pool over fixed ports, recording which change ran on which port, and its halt."""

    def __init__(self, ports: Sequence[InMemorySnowflake]) -> None:
        self.idle = list(ports)
        self.guard = threading.Lock()
        self.leases = 0
        self.halted: str | None = None

    @contextmanager
    def lease(self) -> Iterator[CatalogPublicationPort]:
        with self.guard:
            port = self.idle.pop()
            self.leases += 1
        try:
            yield port
        finally:
            with self.guard:
                self.idle.append(port)

    def halt(self, reason: str) -> None:
        self.halted = reason


def apply_with(
    port: InMemorySnowflake,
    run_id: str,
    *,
    store: InMemoryStateStore | None = None,
    sessions: object = None,
    heartbeat: InMemorySnowflake | None = None,
    policy: LockPolicy | None = None,
) -> ApplyArtifacts:
    return ApplyArtifacts(
        port,
        store or InMemoryStateStore(),
        RunClock(run_id),
        state_table=STATE_TABLE,
        actor="ROLE",
        host="host-" + run_id,
        sessions=sessions,  # type: ignore[arg-type]
        heartbeat=heartbeat,
        lock_policy=policy or stepped(),
    )


class Background:
    """Run an apply on its own thread, to step its heartbeat while it is held in a statement."""

    def __init__(self, run: Callable[[], ApplyResult]) -> None:
        self.result: ApplyResult | None = None
        self._thread = threading.Thread(target=self._run, args=(run,))
        self._thread.start()

    def _run(self, run: Callable[[], ApplyResult]) -> None:
        self.result = run()

    def join(self) -> ApplyResult:
        self._thread.join(timeout=10)
        assert not self._thread.is_alive() and self.result is not None
        return self.result


def break_lock(port: InMemorySnowflake, run_id: str = "rival") -> None:
    """Let the run's lock expire, and another run take it over."""
    port.run_locks.now = 1_000_000.0
    assert port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim(run_id), break_stale=True).acquired


def test_two_concurrent_applies_of_one_target_never_both_run() -> None:
    release = threading.Event()
    first, second = SlowSnowflake(release), SlowSnowflake()
    second.run_locks = first.run_locks  # two machines, one Snowflake lock table
    artifact = rendered()
    running = Background(lambda: apply_with(first, "run-a").run(changeset(change(artifact)), state()))
    assert first.entered.wait(timeout=5)
    refused = apply_with(second, "run-b").run(changeset(change(artifact)), state())
    release.set()
    result = running.join()

    assert [item.code for item in refused.diagnostics] == ["SST-APL011"]
    assert refused.diagnostics[0].message == "run run-a (ROLE on host-run-a), expires 60.0 holds the apply lock"
    assert not refused.state_written and second.scripts == []
    assert result.success and first.run_locks.rows == {}


def test_many_runs_contending_for_one_lock_leave_exactly_one_holder_at_a_time() -> None:
    # Each claim is one critical section, as the lock transaction is: whatever the interleaving,
    # the fences issued are unique, and a claim that won was never refused a live lock it held.
    port = InMemorySnowflake()
    start = threading.Barrier(8)
    won: list[LockFence] = []
    guard = threading.Lock()

    def contend(index: int) -> None:
        start.wait(timeout=5)
        taken = port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim(f"run-{index}"), break_stale=False)
        if taken.fence is not None:
            with guard:
                won.append(taken.fence)

    threads = [threading.Thread(target=contend, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert len(won) == 1 and port.run_locks.holder("verify") == won[0].run_id


def test_a_remote_lock_refusal_releases_the_local_lock() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("other"), break_stale=False)
    result = apply_with(port, "run-a", store=store).run(changeset(change(rendered())), state())
    assert [item.code for item in result.diagnostics] == ["SST-APL011"]
    assert not store.locked and port.scripts == []


def test_an_expired_remote_lock_is_broken_only_with_break_stale_lock() -> None:
    port = InMemorySnowflake()
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("crashed", "ROLE", "", 10), break_stale=False)
    port.run_locks.now = 20.0
    kept = apply_with(port, "run-a").run(changeset(change(rendered())), state())
    assert [item.code for item in kept.diagnostics] == ["SST-APL011"]
    assert "run crashed (ROLE), expired" in kept.diagnostics[0].message

    broke = apply_with(port, "run-b").run(changeset(change(rendered())), state(), ApplyOptions(break_stale_lock=True))
    assert [item.code for item in broke.diagnostics] == ["SST-APL010"]
    assert broke.diagnostics[0].message == "broke a stale lock held by run crashed (ROLE), expired 10.0"
    assert broke.success and port.run_locks.rows == {}


def test_a_long_apply_extends_its_lock_once_per_beat_on_its_own_heartbeat_session() -> None:
    release, ticker = threading.Event(), SteppedTicker()
    port, heartbeat = SlowSnowflake(release), InMemorySnowflake()
    heartbeat.run_locks = port.run_locks
    port.extend_run_lock = None  # type: ignore[assignment, method-assign]  # never on the run's own session
    running = Background(
        lambda: apply_with(port, "run-a", heartbeat=heartbeat, policy=stepped(ticker)).run(
            changeset(change(rendered())), state()
        )
    )
    assert port.entered.wait(timeout=5)
    ticker.step(2)
    assert port.run_locks.extensions == ["run-a", "run-a"] and ticker.beats == 2
    release.set()
    assert running.join().success
    assert ticker.stopped and port.run_locks.rows == {}


def test_a_lock_broken_mid_wave_starts_no_further_change_and_writes_no_state() -> None:
    release, ticker = threading.Event(), SteppedTicker()
    port, store = SlowSnowflake(release), InMemoryStateStore()
    first, second = rendered("A"), rendered("B")
    running = Background(
        lambda: apply_with(port, "run-a", store=store, policy=stepped(ticker)).run(
            changeset(change(first), change(second)), state(), ApplyOptions(parallelism=1)
        )
    )
    assert port.entered.wait(timeout=5)
    break_lock(port)
    ticker.step()
    release.set()
    result = running.join()

    assert [(item.key, item.status) for item in result.outcomes] == [
        (first.key, OutcomeStatus.APPLIED),
        (second.key, OutcomeStatus.SKIPPED),
    ]
    assert [(item.code, item.message) for item in result.diagnostics] == [("SST-APL011", LOST)]
    assert len(port.scripts) == 1
    # The run that broke the lock owns the target's state: nothing of this run's is recorded.
    assert not result.state_written and port.remote_state == {} and store.state is None
    assert port.run_locks.holder("verify") == "rival"


def test_losing_the_lock_halts_every_worker_session_at_once() -> None:
    release, ticker = threading.Event(), SteppedTicker()
    main, worker = InMemorySnowflake(), SlowSnowflake(release)
    worker.run_locks = main.run_locks
    pool = ListPool([worker])
    running = Background(
        lambda: apply_with(main, "run-a", sessions=pool, policy=stepped(ticker)).run(
            changeset(change(rendered())), state()
        )
    )
    assert worker.entered.wait(timeout=5)
    break_lock(main)
    ticker.step()
    # Halted while the change is still in its statement, not once it returns.
    assert pool.halted == "the run lock was lost: another run, which broke this run's lock holds it"
    release.set()
    result = running.join()
    assert not result.state_written


def test_a_run_whose_state_changed_since_the_plan_is_refused_under_the_lock() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()
    artifact = rendered()
    entry = AppliedEntry(
        artifact.fingerprint, artifact.target.sql, "then", "other", APPLIED, artifact.fingerprint, "m" * 64
    )
    # Another run wrote the target's state after this plan read it empty.
    port.remote_state = MappingProxyType({artifact.key: entry})
    result = apply_with(port, "run-a", store=store).run(changeset(change(artifact)), state())
    assert [(item.code, item.message) for item in result.diagnostics] == [
        ("SST-APL012", f"target verify: its state in {STATE_TABLE.sql} changed since the plan")
    ]
    assert not result.state_written and port.scripts == [] and port.state_writes == []
    assert port.run_locks.rows == {} and not store.locked


def test_a_state_table_that_cannot_be_read_under_the_lock_refuses_the_run() -> None:
    port = InMemorySnowflake()
    port.remote_state = None
    result = apply_with(port, "run-a").run(changeset(change(rendered())), state())
    assert [item.code for item in result.diagnostics] == ["SST-APL012"] and port.scripts == []


def test_a_state_write_the_fence_no_longer_allows_is_reported_once() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()

    def broken_meanwhile(*args: object) -> ExecResult:
        break_lock(port)
        return ExecResult(True, ("q1",))

    port.execute_script = broken_meanwhile  # type: ignore[assignment, method-assign]
    result = apply_with(port, "run-a", store=store).run(changeset(change(rendered())), state())
    assert [(item.code, item.message) for item in result.diagnostics] == [("SST-APL011", LOST)]
    assert result.outcomes[0].status is OutcomeStatus.APPLIED
    assert not result.state_written and store.state is None and port.state_writes == []


def test_parallel_changes_run_on_their_own_sessions_never_on_the_shared_one() -> None:
    release = threading.Event()
    main = InMemorySnowflake()
    workers = [SlowSnowflake(release) for _ in range(3)]
    pool = ListPool(workers)
    artifacts = [rendered(name) for name in ("A", "B", "C")]
    running = Background(
        lambda: apply_with(main, "run-a", sessions=pool).run(
            changeset(*(change(item) for item in artifacts)), state(), ApplyOptions(parallelism=3)
        )
    )
    for worker in workers:
        assert worker.entered.wait(timeout=5)
    release.set()
    result = running.join()
    assert result.success and pool.leases == 3
    assert main.scripts == []
    assert sorted(script for worker in workers for script in worker.scripts) == sorted(
        (str(item.statements[0]),) for item in artifacts
    )
    assert all(worker.most_at_once <= 1 for worker in workers)
    assert set(main.remote_state or {}) == {item.key for item in artifacts}


class _RecordingHandler:
    """A composite handler that records the session each change ran on."""

    artifact_type = "skill"

    def __init__(self, port: object, ran_on: list[object]) -> None:
        self.port = port
        self.ran_on = ran_on

    def for_session(self, session: object) -> _RecordingHandler:
        return _RecordingHandler(session, self.ran_on)

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        del options
        self.ran_on.append(self.port)
        return ApplyOutcome(change.key, change.action, OutcomeStatus.APPLIED, 1, 1, "", write_succeeded=True)

    def plan(self, *args: object) -> object:
        raise NotImplementedError

    def merge_physical_resources(
        self, current: tuple[tuple[str, str], ...], previous: object
    ) -> tuple[tuple[str, str], ...]:
        return current

    def report_prune(self, *args: object) -> object:
        raise NotImplementedError


def test_a_composite_change_runs_on_the_session_its_worker_leased() -> None:
    main, worker = InMemorySnowflake(), InMemorySnowflake()
    ran_on: list[object] = []
    use_case = ApplyArtifacts(
        main,
        InMemoryStateStore(),
        RunClock("run-a"),
        state_table=STATE_TABLE,
        lifecycle_handlers={"skill": _RecordingHandler(main, ran_on)},  # type: ignore[dict-item]
        sessions=ListPool([worker]),
        lock_policy=stepped(),
    )
    skill = replace(change(rendered("S")), artifact_type="skill")
    assert use_case.run(changeset(skill), state()).success
    assert ran_on == [worker]


def test_without_a_pool_every_change_runs_on_the_one_session_in_plan_order() -> None:
    main = SlowSnowflake()
    artifacts = [rendered(name) for name in ("A", "B")]
    result = apply_with(main, "run-a").run(
        changeset(*(change(item) for item in artifacts)), state(), ApplyOptions(parallelism=1)
    )
    assert result.success and main.most_at_once == 1
    assert main.scripts == [(str(item.statements[0]),) for item in artifacts]


class FailingPool:
    @contextmanager
    def lease(self) -> Iterator[CatalogPublicationPort]:
        raise SnowflakePortError("could not open another session")
        yield InMemorySnowflake()  # pragma: no cover - never reached

    def halt(self, reason: str) -> None:  # pragma: no cover - the lock is never lost here
        del reason


def test_a_session_that_cannot_open_fails_its_change_with_nothing_written() -> None:
    main = InMemorySnowflake()
    result = apply_with(main, "run-a", sessions=FailingPool()).run(
        changeset(change(rendered())), state(), ApplyOptions(parallelism=2, on_failure=FailurePolicy.CONTINUE)
    )
    [outcome] = result.outcomes
    assert outcome.status is OutcomeStatus.FAILED and not outcome.write_succeeded
    assert outcome.error is not None and "could not open another session" in outcome.error.message


def test_state_writes_only_the_entries_the_run_changed_and_retires() -> None:
    kept, retired, new = rendered("KEPT"), rendered("GONE"), rendered("NEW")
    entry = AppliedEntry(kept.fingerprint, kept.target.sql, "then", "old", APPLIED, kept.fingerprint, "m" * 64)
    previous = State(
        2, target(), "m" * 64, "sst_config.yml", None, MappingProxyType({kept.key: entry, retired.key: entry})
    )
    port = InMemorySnowflake()
    port.remote_state = previous.applied
    prune = replace(change(retired, Action.PRUNE), prune_executable=True)
    result = apply_with(port, "run-a").run(changeset(change(new), prune), previous, ApplyOptions(allow_prune=True))
    assert result.state_written
    [write] = port.state_writes
    assert list(write.upserts) == [new.key] and write.deletes == ()
    assert port.state_manifest == "m" * 64
    assert port.remote_state is not None and set(port.remote_state) == {kept.key, retired.key, new.key}


def test_a_script_that_stopped_part_way_is_updated_by_the_next_plan() -> None:
    base = rendered()
    published = manifest({base.key: base})
    ownership = OwnershipMarker(published.manifest_id, base.fingerprint)
    artifact = replace(base, expected_marker=ownership)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    port.execute_results = [ExecResult(False, ("q1",), ExecutionError("second statement failed"))]
    store = InMemoryStateStore()
    use_case = apply_with(port, "run-a", store=store)
    result = use_case.run(replace(changeset(change(artifact)), manifest_id=published.manifest_id), state())
    assert result.outcomes[0].partial_write and store.state is not None
    port.rows = (ShowRow("V", "DB", "SCHEMA", "OWNER", "now", f"published {ownership.text}"),)
    replanned = PlanArtifacts(port).run({artifact.key: artifact}, published, store.state, target(), fetched_at="later")
    assert replanned.changes[0].action is Action.UPDATE


def lease_for(port: InMemorySnowflake, store: InMemoryStateStore, policy: LockPolicy, **kwargs: object) -> RunLease:
    return RunLease(store, port, STATE_TABLE, "verify", LockClaim("run-a"), policy, **kwargs)  # type: ignore[arg-type]


def test_releasing_the_lease_frees_the_local_lock_even_when_the_remote_release_fails() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()

    def refuse(*args: object) -> None:
        raise SnowflakePortError("connection reset")

    lease = lease_for(port, store, stepped())
    assert lease.acquire(break_stale=False) == (True, ())
    port.release_run_lock = refuse  # type: ignore[method-assign, assignment]
    with pytest.raises(SnowflakePortError):
        lease.release()
    assert not store.locked and lease.fence is None


def test_a_remote_lock_that_cannot_be_read_releases_the_local_lock() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()

    def unreachable(*args: object, **kwargs: object) -> None:
        raise SnowflakePortError("network down")

    port.acquire_run_lock = unreachable  # type: ignore[method-assign, assignment]
    lease = lease_for(port, store, stepped())
    with pytest.raises(SnowflakePortError):
        lease.acquire(break_stale=False)
    assert not store.locked


def test_a_heartbeat_that_cannot_reach_snowflake_keeps_trying_while_the_lock_outlives_the_next_beat() -> None:
    port, ticker = InMemorySnowflake(), SteppedTicker()
    calls: list[int] = []
    extend = port.extend_run_lock

    def flaky(state_table: object, target_name: str, fence: LockFence, ttl_seconds: int) -> bool:
        calls.append(1)
        if len(calls) == 1:
            raise SnowflakePortError("timeout")
        return extend(STATE_TABLE, target_name, fence, ttl_seconds)

    port.extend_run_lock = flaky  # type: ignore[method-assign]
    lease = lease_for(port, InMemoryStateStore(), stepped(ticker))
    lease.acquire(break_stale=False)
    ticker.step(3)
    lease.release()
    assert len(calls) == 3 and not lease.lost


def test_a_heartbeat_whose_failures_could_let_the_lock_expire_loses_the_lease() -> None:
    port, ticker = InMemorySnowflake(), SteppedTicker()
    lost: list[str] = []

    def unreachable(*args: object) -> bool:
        raise SnowflakePortError("timeout")

    port.extend_run_lock = unreachable  # type: ignore[method-assign, assignment]
    lease = lease_for(port, InMemoryStateStore(), stepped(ticker, ttl=3), on_lost=lost.append)
    lease.acquire(break_stale=False)
    ticker.step()
    assert not lease.lost
    # After a second failed beat, the next one would come only as the lock expires.
    ticker.step()
    assert lease.lost and ticker.stopped and lost == [lease.lost_reason]
    assert lease.lost_reason == "another run, perhaps: the lock may have expired, since extending it failed (timeout)"
    lease.release()


def test_a_heartbeat_that_fails_any_other_way_loses_the_lease_and_never_dies_silently() -> None:
    port, ticker = InMemorySnowflake(), SteppedTicker()

    def bug(*args: object) -> bool:
        raise KeyError("fence")

    def callback_that_fails(reason: str) -> None:
        raise RuntimeError(reason)

    port.extend_run_lock = bug  # type: ignore[method-assign, assignment]
    lease = lease_for(port, InMemoryStateStore(), stepped(ticker), on_lost=callback_that_fails)
    lease.acquire(break_stale=False)
    ticker.step()
    assert lease.lost and ticker.stopped
    assert lease.lost_reason == "another run, perhaps: this run's lock heartbeat failed (KeyError: 'fence')"
    lease.release()
    assert port.run_locks.rows == {}


def test_a_lease_without_a_callback_is_still_marked_lost() -> None:
    port, ticker = InMemorySnowflake(), SteppedTicker()
    lease = lease_for(port, InMemoryStateStore(), stepped(ticker))
    lease.acquire(break_stale=False)
    break_lock(port)
    ticker.step()
    assert lease.lost and lease.lost_reason == "another run, which broke this run's lock"
    lease.release()
    assert port.run_locks.holder("verify") == "rival"


def test_release_stops_and_joins_the_heartbeat_before_releasing_the_lock() -> None:
    port, ticker = InMemorySnowflake(), SteppedTicker()
    lease = lease_for(port, InMemoryStateStore(), stepped(ticker))
    lease.acquire(break_stale=False)
    ticker.step()
    heartbeat = lease._heartbeat
    assert heartbeat is not None and heartbeat.is_alive()
    lease.release()
    assert not heartbeat.is_alive() and ticker.stopped
    assert port.run_locks.extensions == ["run-a"] and port.run_locks.releases == ["run-a"]
    lease.release()
    assert port.run_locks.releases == ["run-a"]


def test_the_real_ticker_stops_every_wait_once_stopped() -> None:
    ticker = EventTicker()
    assert not ticker.wait(0)
    ticker.stop()
    assert ticker.wait(60) and ticker.monotonic() > 0


def test_a_stale_local_lock_broken_is_reported_with_its_holder() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()
    store.locked, store.holder, store.stale = True, "old-run", True
    lease = lease_for(port, store, stepped())
    acquired, reported = lease.acquire(break_stale=True)
    lease.release()
    assert acquired and [(item.code, item.message) for item in reported] == [
        ("SST-APL010", "broke a stale lock held by old-run")
    ]
