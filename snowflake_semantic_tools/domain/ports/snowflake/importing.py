"""What `sst import` reads beyond plan's catalog: every object of a type in a schema or database.

Nothing here writes. A read the role may not make raises `SnowflakePortError`, which the
import reports for the object it was reading (SST-IMP005), and goes on to the next.
"""

from __future__ import annotations

from typing import Any, Protocol

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagePort


class ImportReadPort(CatalogPort, StagePort, Protocol):
    """Read objects to import: list a schema or a database, read a definition, and read stage files."""

    def show_objects_in(self, object_type: str, database: Identifier, schema: Identifier | None) -> tuple[ShowRow, ...]:
        """Return every object of one type in a schema, or in every schema of a database when `schema` is None.

        Built-in routines are left out. Never writes.

        Raises:
            SnowflakePortError: the SHOW failed, as for a database or schema the role cannot see.
        """
        ...

    def eval_rows(self, table: QualifiedName) -> tuple[tuple[str, Any], ...] | None:
        """Return an eval input table's rows: each question, and its ground truth decoded from VARIANT.

        The columns are the ones Snowflake's evaluations read, `INPUT_QUERY` and `GROUND_TRUTH`.
        Never writes.

        Returns:
            The rows in the order the table returns them; None when no table has the name.

        Raises:
            SnowflakePortError: the read was refused, or the table lacks those columns.
        """
        ...
