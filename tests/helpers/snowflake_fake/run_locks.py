"""An in-memory run lock table with the connector's semantics.

`InMemoryRunLocks` keeps one row per (state table, target). Each operation, a claim, an
extension, a release, or a fence check, is one critical section that reads and then decides,
as each runs in the connector as one transaction that serialises on the lock table's mutex row
before it reads; every claim that wins is issued the next generation. Time is a number of
seconds the test controls through `now`.
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
