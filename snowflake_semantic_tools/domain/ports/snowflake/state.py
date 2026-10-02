"""The port that keeps the state table and the run lock beside it, by target.

The state table holds what apply recorded for each artifact; the lock table holds, per target,
the one run that may apply to it. Both live in the state table's schema. Their table type is
the connector's choice and is described where the connector creates them.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, StateWrite


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
    ) -> None:
        """Apply one run's change to a target's entries in one transaction, and record its manifest.

        Each upsert is one MERGE on the target and artifact key, each delete one DELETE, and
        every row of the target then records `manifest_id`; every value is bound, never
        spliced. Creates or migrates the table first, as `ensure_state_table` does. Rows the
        write does not name are left as they are.

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
        """Take the target's run lock for `claim` unless another run holds it, without waiting.

        The lock row is read, then claimed with one compare-and-set MERGE: a free lock is
        inserted, and an expired one is replaced only when `break_stale` and the row still
        names the run that was read, so of two runs racing for one lock exactly one wins. A
        live lock is never taken. Expiry is judged by Snowflake's clock, never this machine's.

        Raises:
            SnowflakePortError: the lock table could not be created, read, or written.
        """
        ...

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, claim: LockClaim) -> bool:
        """Push the lock's expiry `claim.ttl_seconds` past now, when `claim.run_id` still holds it.

        Returns:
            Whether the claim still holds the lock; False when another run broke it.

        Raises:
            SnowflakePortError: the UPDATE failed.
        """
        ...

    def release_run_lock(self, state_table: QualifiedName, target_name: str, run_id: str) -> None:
        """Delete the lock row when `run_id` holds it; a lock another run holds is left alone.

        Raises:
            SnowflakePortError: the DELETE failed.
        """
        ...
