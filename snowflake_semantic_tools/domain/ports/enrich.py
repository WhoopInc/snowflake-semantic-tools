"""The warehouse reads and the Cortex call `sst enrich` makes.

Enrich reads a relation's columns, a few of their distinct values, and asks Cortex for
synonyms; it never writes to Snowflake. These ports are separate from `SnowflakePort`, which
the publishing commands use, so the doubles of one need not implement the other.
"""

from __future__ import annotations

from typing import Mapping, Protocol, Sequence

from snowflake_semantic_tools.domain.model.enrich import WarehouseColumn
from snowflake_semantic_tools.domain.model.identifier import QualifiedName


class RelationProfilerPort(Protocol):
    """Read a relation's columns and the distinct values they hold."""

    def relation_columns(self, relation: QualifiedName) -> tuple[WarehouseColumn, ...] | None:
        """Return a table's or a view's columns in their ordinal order, as INFORMATION_SCHEMA has them.

        Never writes.

        Returns:
            The columns; None when no relation exists under the name or the role cannot see it.

        Raises:
            SnowflakePortError: the lookup failed for another reason.
        """
        ...

    def distinct_values(
        self, relation: QualifiedName, columns: Sequence[str], limit: int
    ) -> Mapping[str, tuple[str, ...]]:
        """Return the distinct non-null values of each column as text, most frequent first.

        Each column is named as Snowflake stores it. Equally frequent values come in text order,
        so the same data always gives the same answer. Never writes.

        Args:
            limit: The most values returned for one column.

        Returns:
            Each column's values, by its name as given; an all-null column maps to no values.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...


class CortexPort(Protocol):
    """Ask a Cortex model for a structured answer."""

    def complete_json(self, model: str, prompt: str, schema: Mapping[str, object]) -> object:
        """Return the model's answer to one prompt, decoded from the JSON `schema` asks for.

        The call runs as SQL in the session, at temperature 0, so the prompt stays in the account.

        Raises:
            SnowflakePortError: the model name is not a plain name, or the call failed.
        """
        ...


class EnrichPort(RelationProfilerPort, CortexPort, Protocol):
    """Everything enrich reads from Snowflake, through one session."""
