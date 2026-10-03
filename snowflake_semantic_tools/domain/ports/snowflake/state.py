"""The port that keeps the state table and the run lock beside it, by target.

The state table holds what apply recorded for each artifact; the lock table holds, per target,
the one run that may apply to it. Both live in the state table's schema. Their table type is
the connector's choice and is described where the connector creates them.

The lock is mutual exclusion by construction: claiming, extending, releasing, and writing
state each run as one transaction that serialises with every other one on the lock table
before it reads anything, so each decides on what the previous one committed. A claim that
wins is issued a `LockFence`; only its holder may extend or release the lock or write state.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, LockFence, StateWrite


class StatePort(Protocol):
    """Keep the state table and the run lock: what apply recorded per artifact, and who may apply.

    Plan trusts a live object only when its entry here matches the object's ownership marker.
    """

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None:
        """Return the entries apply recorded for one target, by artifact key.

        Never writes. An entry that does not decode raises rather than reading as absent.

        Returns:
            The entries; empty when the state table does not exist, and None when it exists
            but cannot be read, which the caller must not mistake for no entries.

        Raises:
            SnowflakePortError: checking whether the state table exists failed.
        """
        ...

    def read_state_manifest(self, state_table: QualifiedName, target_name: str) -> str | None:
        """Return the manifest the last state write for one target recorded; never writes.

        Returns:
            The manifest id; None when the table, the target's rows, or the column is absent,
            or the rows name no single manifest.

        Raises:
            SnowflakePortError: the read failed.
        """
        ...

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        write: StateWrite,
        fence: LockFence,
    ) -> bool:
        """Apply one run's change to a target's entries in one transaction, while `fence` holds the lock.

        The transaction first serialises with every lock operation, then checks that the
        target's lock row still records `fence`; only then does each upsert run as one MERGE
        on the target and artifact key, each delete as one DELETE, and every row of the target
        record `manifest_id`. A takeover therefore lands wholly before the write, which then
        refuses, or wholly after it. Every value is bound, never spliced. Creates or migrates
        the table first, as `ensure_state_table` does. Rows the write does not name are left
        as they are.

        Returns:
            True when the write committed; False when `fence` no longer holds the lock, and the
            transaction rolled back with nothing written.

        Raises:
            SnowflakePortError: the write failed; its transaction rolls back, so the target
                keeps the entries it had.
        """
        ...

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        """Create the state table unless it exists, and add any column an older table lacks.

        Idempotent: every column added since the first table is added with ADD COLUMN IF NOT EXISTS.

        Raises:
            SnowflakePortError: the CREATE or ALTER failed.
        """
        ...

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        """Take the target's run lock for `claim` unless another run holds it, without waiting for it.

        In one transaction that has serialised with every other lock operation, the target's
        row is read and judged: a free lock is taken, an expired one only with `break_stale`,
        and a live one never. Of any number of runs racing for one lock, exactly one wins.
        Expiry is judged by Snowflake's clock, never this machine's. Waiting for a rival's
        lock operation, which lasts a few statements, is not waiting for the lock.

        Returns:
            What the claim found; when it acquired the lock, the fence it was issued.

        Raises:
            SnowflakePortError: the lock table could not be created, read, or written.
        """
        ...

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence, ttl_seconds: int) -> bool:
        """Push the lock's expiry `ttl_seconds` past now, when `fence` still holds it.

        Returns:
            Whether the fence still holds the lock; False when another run broke it.

        Raises:
            SnowflakePortError: the extension failed; the lock is as it was.
        """
        ...

    def release_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence) -> None:
        """Delete the lock row when `fence` holds it; a lock another claim holds is left alone.

        Raises:
            SnowflakePortError: the release failed; the lock then expires on its own.
        """
        ...
