"""The port that keeps the state table: what apply recorded for each artifact, by target and key."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state import AppliedEntry


class StatePort(Protocol):
    """Keep the state table: what apply recorded for each artifact, by target and artifact key.

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

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        applied: Mapping[str, AppliedEntry],
    ) -> None:
        """Replace every entry recorded for one target with `applied`, in one transaction.

        Creates or migrates the table first, as `ensure_state_table` does. `manifest_id` is
        not stored: each entry records its own.

        Raises:
            SnowflakePortError: the write failed; its transaction rolls back, so the target
                keeps the entries it had.
        """
        ...

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        """Create the state table unless it exists, and add any column an older table lacks.

        Idempotent.

        Raises:
            SnowflakePortError: the CREATE or ALTER failed.
        """
        ...

    def delete_state(self, state_table: QualifiedName, target_name: str, artifact_key: str) -> int:
        """Delete one artifact's entry for one target.

        Returns:
            The rows deleted: 0 when there was no entry.

        Raises:
            SnowflakePortError: the DELETE failed.
        """
        ...

    def upsert_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        artifact_key: str,
        entry: AppliedEntry,
    ) -> int:
        """Insert or replace one artifact's entry for one target.

        Returns:
            The rows written, as the driver counts them.

        Raises:
            SnowflakePortError: the MERGE failed.
        """
        ...
