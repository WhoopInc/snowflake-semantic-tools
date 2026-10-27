"""What `sst import` reads: every object of a type in a scope, and an eval's input table.

Nothing here writes. A schema or database the role cannot see is an error, not an empty
listing, so an import never reports nothing to import where it could read nothing.
"""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.adapters.snowflake.connector.catalog import CatalogMethods, _object_type, shown_rows
from snowflake_semantic_tools.adapters.snowflake.connector.session import _variant_value
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.importing import ImportReadPort
from snowflake_semantic_tools.domain.sql import ident, keyword, qname, sql


class ImportMethods(CatalogMethods, ImportReadPort):
    """List the objects to import, and read an eval's input table."""

    def show_objects_in(self, object_type: str, database: Identifier, schema: Identifier | None) -> tuple[ShowRow, ...]:
        if schema is not None:
            return self.show_objects(object_type, SchemaScope(database, schema))
        kind = _object_type(object_type)
        rows = self._dict_rows(
            sql("SHOW {kind} IN DATABASE {database}", kind=keyword(kind, plural=True), database=ident(database))
        )
        return shown_rows(rows, kind, database.folded, "")

    def eval_rows(self, table: QualifiedName) -> tuple[tuple[str, Any], ...] | None:
        if not self.object_exists("TABLE", table):
            return None
        result = self.query(sql("SELECT INPUT_QUERY, GROUND_TRUTH FROM {table}", table=qname(table)))
        return tuple((str(row[0] or ""), _variant_value(row[1], None)) for row in result.rows)
