"""An in-memory run lock table with the connector's semantics, and a ticker a test steps by hand.

`InMemoryRunLocks` keeps one row per (state table, target). Each operation, a claim, an
extension, a release, or a fence check, is one critical section that reads and then decides,
as each runs in the connector as one transaction that serialises on the lock table's mutex row
before it reads; every claim that wins is issued the next generation. Time is a number of
seconds the test controls through `now`.

`SteppedTicker` is a heartbeat `Ticker` that never waits on the wall clock: each `step` lets
exactly one beat run and returns once the heartbeat waits again or has stopped.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, LockFence, RunLock


@dataclass
class _Row:
    run_id: str
    owner: str
    host: str
    acquired_at: float
    expires_at: float
    generation: int


class InMemoryRunLocks:
    def __init__(self) -> None:
        self.now = 0.0
        self.rows: dict[tuple[str, str], _Row] = {}
        self.extensions: list[str] = []
        self.releases: list[str] = []
        self.claims: list[str] = []
        self.refuse_extension = False
        self.generation = 0
        self._guard = threading.Lock()

    def holder(self, target_name: str, state_table: str = "") -> str | None:
        for (table, target), row in self.rows.items():
            if target == target_name and (not state_table or table == state_table):
                return row.run_id
        return None

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        key = (state_table.sql, target_name)
        with self._guard:
            self.claims.append(claim.run_id)
            current = self._read(key)
            if current is not None and not (current.expired and break_stale):
                return LockAcquisition(False, current)
            self.generation += 1
            self.rows[key] = _Row(
                claim.run_id, claim.owner, claim.host, self.now, self.now + claim.ttl_seconds, self.generation
            )
            fence = LockFence(claim.run_id, self.generation)
        return LockAcquisition(True, current, broke_stale=current is not None, fence=fence)

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence, ttl_seconds: int) -> bool:
        with self._guard:
            self.extensions.append(fence.run_id)
            row = self.rows.get((state_table.sql, target_name))
            if self.refuse_extension or not _fenced(row, fence):
                return False
            assert row is not None
            row.expires_at = self.now + ttl_seconds
            return True

    def release_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence) -> None:
        with self._guard:
            self.releases.append(fence.run_id)
            key = (state_table.sql, target_name)
            if _fenced(self.rows.get(key), fence):
                del self.rows[key]

    def fence_holds(self, state_table: QualifiedName, target_name: str, fence: LockFence) -> bool:
        with self._guard:
            return _fenced(self.rows.get((state_table.sql, target_name)), fence)

    def _read(self, key: tuple[str, str]) -> RunLock | None:
        row = self.rows.get(key)
        if row is None:
            return None
        return RunLock(
            row.run_id, row.owner, row.host, str(row.acquired_at), str(row.expires_at), row.expires_at <= self.now
        )


def _fenced(row: _Row | None, fence: LockFence) -> bool:
    return row is not None and (row.run_id, row.generation) == (fence.run_id, fence.generation)


class SteppedTicker:
    """A `Ticker` whose waits end only when the test steps it, or it is stopped.

    `now` advances by each granted wait's seconds, so the heartbeat's clock moves one interval
    per beat. Every hand-off has a 5-second safety bound that a passing test never reaches.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.beats = 0
        self._condition = threading.Condition()
        self._granted = 0
        self._waiting = False
        self._stopped = False

    def wait(self, seconds: float) -> bool:
        with self._condition:
            self._waiting = True
            self._condition.notify_all()
            while not self._granted and not self._stopped:
                self._condition.wait()
            self._waiting = False
            if self._stopped:
                return True
            self._granted -= 1
            self.now += seconds
            self.beats += 1
            return False

    def stop(self) -> None:
        with self._condition:
            self._stopped = True
            self._condition.notify_all()

    def monotonic(self) -> float:
        return self.now

    def step(self, beats: int = 1) -> None:
        """Let `beats` beats run, one at a time, returning once the last has been handled."""
        for _ in range(beats):
            with self._condition:
                assert self._condition.wait_for(lambda: self._waiting or self._stopped, timeout=5)
                if self._stopped:
                    return
                self._granted += 1
                self._condition.notify_all()
                # Handled once the heartbeat waits again, having taken this grant, or has stopped.
                assert self._condition.wait_for(
                    lambda: (self._waiting and not self._granted) or self._stopped, timeout=5
                )

    @property
    def stopped(self) -> bool:
        return self._stopped
