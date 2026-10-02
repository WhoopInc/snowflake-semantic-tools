"""An in-memory run lock table that behaves like the connector's compare-and-set MERGE.

`InMemoryRunLocks` keeps one row per (state table, target) and makes each claim's MERGE one
atomic step under a lock, as Snowflake serialises MERGEs on one table, so threads contending
for a lock exercise the same read, claim, and read-back the connector performs. Time is a
number of seconds the test controls through `now`.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, RunLock


@dataclass
class _Row:
    run_id: str
    owner: str
    host: str
    acquired_at: float
    expires_at: float


class InMemoryRunLocks:
    def __init__(self) -> None:
        self.now = 0.0
        self.rows: dict[tuple[str, str], _Row] = {}
        self.extensions: list[str] = []
        self.releases: list[str] = []
        self.claims: list[str] = []
        self.refuse_extension = False
        self._guard = threading.Lock()
        # Called between reading the row and claiming it, to stage a race in a test.
        self.before_claim: list[object] = []

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
            current = self._read(key)
        if current is not None and not (current.expired and break_stale):
            return LockAcquisition(False, current)
        for hook in self.before_claim:
            assert callable(hook)
            hook()
        with self._guard:
            # The MERGE: insert when no row matches, replace only the expired row of the expected run.
            self.claims.append(claim.run_id)
            held = self.rows.get(key)
            replaces = current is not None and held is not None and held.run_id == current.run_id
            if held is None or (replaces and held.expires_at <= self.now):
                self.rows[key] = _Row(claim.run_id, claim.owner, claim.host, self.now, self.now + claim.ttl_seconds)
            after = self._read(key)
        if after is not None and after.run_id == claim.run_id:
            return LockAcquisition(True, current, broke_stale=current is not None)
        return LockAcquisition(False, after or current)

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, claim: LockClaim) -> bool:
        with self._guard:
            self.extensions.append(claim.run_id)
            row = self.rows.get((state_table.sql, target_name))
            if self.refuse_extension or row is None or row.run_id != claim.run_id:
                return False
            row.expires_at = self.now + claim.ttl_seconds
            return True

    def release_run_lock(self, state_table: QualifiedName, target_name: str, run_id: str) -> None:
        with self._guard:
            self.releases.append(run_id)
            key = (state_table.sql, target_name)
            row = self.rows.get(key)
            if row is not None and row.run_id == run_id:
                del self.rows[key]

    def _read(self, key: tuple[str, str]) -> RunLock | None:
        row = self.rows.get(key)
        if row is None:
            return None
        return RunLock(
            row.run_id, row.owner, row.host, str(row.acquired_at), str(row.expires_at), row.expires_at <= self.now
        )
