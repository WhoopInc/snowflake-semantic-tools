"""A relation's columns and the distinct values they hold, read for `sst enrich`.

Nothing here writes. Columns come from the database's INFORMATION_SCHEMA, which describes views
as well as tables, under the names Snowflake stores. Distinct values come from one query per
group of columns: each column is a parenthesized branch of a UNION ALL that groups its non-null
values, counts them, and keeps the most frequent, so the whole sample costs one scan per column.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session
from snowflake_semantic_tools.domain.diagnostics.signatures import SessionFailure, session_failure
from snowflake_semantic_tools.domain.enrich import WarehouseColumn
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.ports.enrich import RelationProfilerPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, ident, join, number, qname, sql

# Columns sampled by one statement; a relation with more is sampled in several.
COLUMNS_PER_QUERY = 50


def columns_sql(relation: QualifiedName) -> Sql:
    """Return the statement reading a relation's columns; it binds the schema and the name."""
    return sql(
        "SELECT COLUMN_NAME, DATA_TYPE "
        "FROM {database}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
        "ORDER BY ORDINAL_POSITION",
        database=ident(relation.database),
    )


def distinct_values_sql(relation: QualifiedName, columns: Sequence[str], limit: int) -> Sql:
    """Return the statement sampling each column's most frequent distinct values.

    Each column is named as Snowflake stores it and quoted when it must be. A row is the
    column's position in `columns`, one value as text, and how many rows hold it.
    """
    branches = []
    for index, column in enumerate(columns):
        name = ident(Identifier.shown(column))
        branches.append(
            sql(
                "(SELECT {index} AS COLUMN_INDEX, TO_VARCHAR({name}) AS VALUE, COUNT(*) AS ROW_COUNT "
                "FROM {relation} WHERE {name} IS NOT NULL GROUP BY {name} "
                "ORDER BY ROW_COUNT DESC, VALUE LIMIT {limit})",
                index=number(index),
                name=name,
                relation=qname(relation),
                limit=number(int(limit)),
            )
        )
    return sql(
        "{branches}\nORDER BY COLUMN_INDEX, ROW_COUNT DESC, VALUE",
        branches=join("\nUNION ALL\n", branches),
    )


def _not_visible(error: SnowflakePortError) -> bool:
    """Report whether the signature table reads the error as a database or object the role cannot see."""
    return session_failure(str(error), errno=error.errno, sqlstate=error.sqlstate) is SessionFailure.NOT_VISIBLE


class ProfilerMethods(Session, RelationProfilerPort):
    """Read relations' columns and sample their values, without writing."""

    def relation_columns(self, relation: QualifiedName) -> tuple[WarehouseColumn, ...] | None:
        try:
            result = self.query(columns_sql(relation), (relation.schema.folded, relation.name.folded))
        except SnowflakePortError as error:
            if _not_visible(error):
                return None
            raise
        columns = tuple(WarehouseColumn(str(name), str(data_type)) for name, data_type in result.rows)
        return columns or None

    def distinct_values(
        self, relation: QualifiedName, columns: Sequence[str], limit: int
    ) -> Mapping[str, tuple[str, ...]]:
        values: dict[str, list[str]] = {column: [] for column in columns}
        for start in range(0, len(columns), COLUMNS_PER_QUERY):
            chunk = tuple(columns[start : start + COLUMNS_PER_QUERY])
            result = self.query(distinct_values_sql(relation, chunk, limit))
            for index, value, _ in result.rows:
                if value is not None:
                    values[chunk[int(str(index))]].append(str(value))
        return {column: tuple(found) for column, found in values.items()}
