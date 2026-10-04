"""The fake's `StatePort`: the state table's rows for one target and its fenced run lock.

`remote_state` is what `read_state` returns (None: no row was ever written); a write lands
only while its fence still holds the lock in `run_locks`, and each write that lands is kept in
`state_writes`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, LockFence, StateWrite
from tests.helpers.snowflake_fake.world import SnowflakeWorld


class FakeState(SnowflakeWorld):
    """`StatePort` over the shared account, and the session's `halt`."""

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None:
        del state_table, target_name
        self._check("read_state")
        return self.remote_state

    def read_state_manifest(self, state_table: QualifiedName, target_name: str) -> str | None:
        del state_table, target_name
        self._check("read_state_manifest")
        return self.state_manifest

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        write: StateWrite,
        fence: LockFence,
    ) -> bool:
        self._check("write_state")
        if not self.run_locks.fence_holds(state_table, target_name, fence):
            return False
        current = {**(self.remote_state or {}), **write.upserts}
        for key in write.deletes:
            current.pop(key, None)
        self.remote_state = MappingProxyType(current)
        self.state_manifest = manifest_id
        self.state_writes.append(write)
        return True

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        del state_table
        self._check("ensure_state_table")

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        self._check("acquire_run_lock")
        return self.run_locks.acquire_run_lock(state_table, target_name, claim, break_stale=break_stale)

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence, ttl_seconds: int) -> bool:
        self._check("extend_run_lock")
        return self.run_locks.extend_run_lock(state_table, target_name, fence, ttl_seconds)

    def release_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence) -> None:
        self._check("release_run_lock")
        self.run_locks.release_run_lock(state_table, target_name, fence)

    def halt(self, reason: str) -> None:
        self.halted = reason
