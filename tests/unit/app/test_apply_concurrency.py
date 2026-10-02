"""Apply under the run lease, on pooled sessions: contention, heartbeat, a lost lock, and the state delta."""

from __future__ import annotations

import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
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
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import FixedClock, InMemorySnowflake, InMemoryStateStore
from tests.helpers.artifact_builders import change, changeset, manifest, rendered, state, target

STATE_TABLE = rendered("SST_STATE").target
FAST = LockPolicy(ttl_seconds=60, heartbeat_seconds=0.005)


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


def apply_with(
    port: InMemorySnowflake,
    run_id: str,
    *,
    store: InMemoryStateStore | None = None,
    sessions: object = None,
    policy: LockPolicy = FAST,
) -> ApplyArtifacts:
    return ApplyArtifacts(
        port,
        store or InMemoryStateStore(),
        RunClock(run_id),
        state_table=STATE_TABLE,
        actor="ROLE",
        host="host-" + run_id,
        sessions=sessions,  # type: ignore[arg-type]
        lock_policy=policy,
    )


def test_two_concurrent_applies_of_one_target_never_both_run() -> None:
    release = threading.Event()
    first, second = SlowSnowflake(release), SlowSnowflake()
    second.run_locks = first.run_locks  # two machines, one Snowflake lock table
    artifact = rendered()
    results = {}

    def run_first() -> None:
        results["first"] = apply_with(first, "run-a").run(changeset(change(artifact)), state())

    worker = threading.Thread(target=run_first)
    worker.start()
    assert first.entered.wait(timeout=5)
    refused = apply_with(second, "run-b").run(changeset(change(artifact)), state())
    release.set()
    worker.join(timeout=5)

    assert [item.code for item in refused.diagnostics] == ["SST-APL011"]
    assert refused.diagnostics[0].message == "run run-a (ROLE on host-run-a), expires 60.0 holds the apply lock"
    assert not refused.state_written and second.scripts == []
    assert results["first"].success and first.run_locks.rows == {}


def test_two_applies_that_read_a_free_lock_together_leave_exactly_one_running() -> None:
    first, second = InMemorySnowflake(), InMemorySnowflake()
    second.run_locks = first.run_locks
    both_read = threading.Barrier(2)
    first.run_locks.before_claim.append(lambda: both_read.wait(timeout=5))
    results = {}

    def run(port: InMemorySnowflake, run_id: str) -> None:
        results[run_id] = apply_with(port, run_id).run(changeset(change(rendered())), state())

    threads = [threading.Thread(target=run, args=item) for item in ((first, "run-a"), (second, "run-b"))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    codes = sorted(tuple(item.code for item in result.diagnostics) for result in results.values())
    assert codes == [(), ("SST-APL011",)]
    assert len(first.scripts) + len(second.scripts) == 1


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


def test_a_long_apply_extends_its_lock_while_it_runs() -> None:
    release = threading.Event()
    port = SlowSnowflake(release)
    timer = threading.Timer(0.1, release.set)
    timer.start()
    result = apply_with(port, "run-a").run(changeset(change(rendered())), state())
    timer.join()
    assert result.success
    assert len(port.run_locks.extensions) >= 2 and set(port.run_locks.extensions) == {"run-a"}


def test_a_run_whose_lock_was_broken_starts_no_further_wave_and_records_what_it_wrote() -> None:
    release = threading.Event()
    port = SlowSnowflake(release)
    port.run_locks.refuse_extension = True
    first = rendered("A")
    second = rendered("B", depends_on=(first.key,))
    timer = threading.Timer(0.1, release.set)
    timer.start()
    result = apply_with(port, "run-a").run(changeset(change(first), change(second)), state())
    timer.join()
    assert [(item.key, item.status) for item in result.outcomes] == [
        (first.key, OutcomeStatus.APPLIED),
        (second.key, OutcomeStatus.SKIPPED),
    ]
    assert [item.code for item in result.diagnostics] == ["SST-APL011"]
    assert result.state_written and set(port.remote_state or {}) == {first.key}


class ListPool:
    """A session pool over fixed ports, recording which change ran on which port."""

    def __init__(self, ports: Sequence[InMemorySnowflake]) -> None:
        self.idle = list(ports)
        self.guard = threading.Lock()
        self.leases = 0

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


def test_parallel_changes_run_on_their_own_sessions_never_on_the_shared_one() -> None:
    release = threading.Event()
    main = InMemorySnowflake()
    workers = [SlowSnowflake(release) for _ in range(3)]
    pool = ListPool(workers)
    artifacts = [rendered(name) for name in ("A", "B", "C")]
    timer = threading.Timer(0.05, release.set)
    timer.start()
    result = apply_with(main, "run-a", sessions=pool).run(
        changeset(*(change(item) for item in artifacts)), state(), ApplyOptions(parallelism=3)
    )
    timer.join()
    assert result.success and pool.leases == 3
    assert main.scripts == []
    assert sorted(script for worker in workers for script in worker.scripts) == sorted(
        (str(item.statements[0]),) for item in artifacts
    )
    assert all(worker.most_at_once <= 1 for worker in workers)
    assert set(main.remote_state or {}) == {item.key for item in artifacts}


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


def test_releasing_the_lease_frees_the_local_lock_even_when_the_remote_release_fails() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()

    def refuse(*args: object) -> None:
        raise SnowflakePortError("connection reset")

    lease = RunLease(store, port, STATE_TABLE, "verify", LockClaim("run-a"), FAST)
    assert lease.acquire(break_stale=False) == (True, ())
    port.release_run_lock = refuse  # type: ignore[method-assign, assignment]
    with pytest.raises(SnowflakePortError):
        lease.release()
    assert not store.locked


def test_a_remote_lock_that_cannot_be_read_releases_the_local_lock() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()

    def unreachable(*args: object, **kwargs: object) -> None:
        raise SnowflakePortError("network down")

    port.acquire_run_lock = unreachable  # type: ignore[method-assign, assignment]
    lease = RunLease(store, port, STATE_TABLE, "verify", LockClaim("run-a"), FAST)
    with pytest.raises(SnowflakePortError):
        lease.acquire(break_stale=False)
    assert not store.locked


def test_a_heartbeat_that_cannot_reach_snowflake_keeps_trying() -> None:
    port = InMemorySnowflake()
    calls: list[int] = []

    def flaky(*args: object) -> bool:
        calls.append(1)
        if len(calls) == 1:
            raise SnowflakePortError("timeout")
        return True

    port.extend_run_lock = flaky  # type: ignore[method-assign, assignment]
    lease = RunLease(InMemoryStateStore(), port, STATE_TABLE, "verify", LockClaim("run-a"), FAST)
    lease.acquire(break_stale=False)
    deadline = threading.Event()
    while len(calls) < 3 and not deadline.wait(0.005):
        pass
    lease.release()
    assert len(calls) >= 3 and not lease.lost


def test_a_stale_local_lock_broken_is_reported_with_its_holder() -> None:
    port, store = InMemorySnowflake(), InMemoryStateStore()
    store.locked, store.holder, store.stale = True, "old-run", True
    acquired, reported = RunLease(store, port, STATE_TABLE, "verify", LockClaim("run-a"), FAST).acquire(
        break_stale=True
    )
    assert acquired and [(item.code, item.message) for item in reported] == [
        ("SST-APL010", "broke a stale lock held by old-run")
    ]
