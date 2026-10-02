"""The port that keeps CoCo Desktop's profile registry table, one row per profile."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import QualifiedName


class ProfileRegistryPort(Protocol):
    """Keep CoCo Desktop's profile registry: one row per profile, keyed by CONFIG_NAME.

    A row write is guarded by the VERSION the caller observed and returns the rows it
    changed, so a count of 0 reveals a concurrent writer. Rows read back with their column
    names uppercase.
    """

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        """Create the registry table with CoCo Desktop's columns unless it exists.

        Idempotent: an existing table is left as it is, whatever its columns.

        Raises:
            SnowflakePortError: the CREATE failed.
        """
        ...

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        """Return the row for one profile, active or not.

        Never writes.

        Returns:
            The row; None when no row has the name.

        Raises:
            SnowflakePortError: the query failed, or more than one row has the name.
        """
        ...

    def merge_profile_row(
        self,
        registry: QualifiedName,
        row: Mapping[str, object],
        *,
        expected_version: str | None,
    ) -> int:
        """Insert a profile's row, or update it while its VERSION is still `expected_version`.

        The row is keyed by its CONFIG_NAME and left active.

        Args:
            expected_version: The VERSION the caller observed; None when it observed no row,
                so an existing row is left alone.

        Returns:
            The rows written: 0 when a stored row's VERSION is not `expected_version`.

        Raises:
            SnowflakePortError: the MERGE failed.
        """
        ...

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        """Mark a profile's row inactive when it is active at `expected_version`.

        Returns:
            The rows changed: 0 when the row is absent, already inactive, or at another VERSION.

        Raises:
            SnowflakePortError: the UPDATE failed.
        """
        ...

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        """Return the active rows by CONFIG_NAME, exactly as CoCo Desktop reads the registry.

        Never writes.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...
