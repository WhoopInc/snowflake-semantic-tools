"""The warehouse reads, the Cortex call, and the file edits `sst enrich` makes.

Enrich reads a relation's columns, a few of their distinct values, and asks Cortex for
synonyms; it never writes to Snowflake, only to the project's YAML files. These ports are
separate from `SnowflakePort`, which the publishing commands use, so the doubles of one need
not implement the other.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.domain.model.enrich import ColumnUpdate, TableSynonymEdit, WarehouseColumn
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


@dataclass(frozen=True, slots=True)
class WrittenFile:
    """A file's text after enrich's edits.

    Attributes:
        text: The whole file as it would be written.
        reformatted: Writing it changes lines enrich did not edit (SST-PRS125).
    """

    text: str
    reformatted: bool


class EnrichFilesPort(Protocol):
    """Read and edit the project's dbt and semantic view YAML; paths are relative to the project."""

    def read(self, path: str) -> str | None:
        """Return a file's text; None when it does not exist.

        Raises:
            ProjectError: The file exists and cannot be read as UTF-8 text.
        """
        ...

    def edit_models(self, text: str | None, path: str, updates: Mapping[str, Sequence[ColumnUpdate]]) -> WrittenFile:
        """Return a dbt YAML file's text with each model's column updates made; nothing is written.

        Args:
            text: The file's current text; None for a file enrich creates.

        Raises:
            ProjectError: The file is not YAML, or a part enrich must edit has another shape.
        """
        ...

    def edit_views(self, text: str, path: str, edits: Sequence[TableSynonymEdit]) -> WrittenFile:
        """Return a semantic view file's text with each table-synonym edit made; nothing is written.

        Raises:
            ProjectError: The file cannot be read as a view file, or lacks a view an edit names.
        """
        ...

    def write(self, path: str, text: str) -> None:
        """Replace a file's text atomically, creating its directory when needed.

        Raises:
            OSError: The file cannot be written.
        """
        ...
