"""The column-tag read enrich and the connected validate share, and what its answer says.

`TAG_REFERENCES_ALL_COLUMNS` lists, for each column of one table or view, every tag on it: one
set on the column, and one set on its table, schema or database, which every column inherits.
Each counts, whatever its `LEVEL` and `APPLY_METHOD` (docs/design/snowflake-behaviour.md). The
statement only reads; a relation that is missing or not visible fails it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace

from snowflake_semantic_tools.domain.enrich.columns import WarehouseColumn
from snowflake_semantic_tools.domain.model.governance import TagPattern, matching_tags
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.sql import Sql, ident, literal, sql

# The headings of the answer each tag is read from, casefolded.
_TAG_HEADINGS = ("tag_database", "tag_schema", "tag_name", "column_name")


def column_tags_sql(relation: QualifiedName) -> Sql:
    """Return the statement listing the tags on each of a relation's columns.

    The relation is named by a string constant, as the table function takes it, and its
    database's INFORMATION_SCHEMA is the one asked. The domain is `'table'`, spelled as the
    measured read spelled it (docs/design/snowflake-behaviour.md); a view was not measured.
    """
    return sql(
        "SELECT * FROM TABLE({database}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS({name}, 'table'))",
        database=ident(relation.database),
        name=literal(relation.sql),
    )


def parse_column_tags(result: QueryResult) -> dict[str, tuple[QualifiedName, ...]] | None:
    """Read each column's tags from the answer to `column_tags_sql`, keeping every level and method.

    Returns:
        The tags on each column, by the column's casefolded name, each tag once, in the order
        listed; a row with an empty part is skipped. None when the answer lacks a tag or column
        heading, so nothing can be read from it.
    """
    headings = [str(heading).casefold() for heading in result.columns]
    if any(heading not in headings for heading in _TAG_HEADINGS):
        return None
    positions = [headings.index(heading) for heading in _TAG_HEADINGS]
    tags: dict[str, list[QualifiedName]] = {}
    for row in result.rows:
        parts = [str(row[index] or "") if index < len(row) else "" for index in positions]
        if not all(parts):
            continue
        database, schema, name, column = parts
        tag = QualifiedName(Identifier.shown(database), Identifier.shown(schema), Identifier.shown(name))
        found = tags.setdefault(column.casefold(), [])
        if tag not in found:
            found.append(tag)
    return {column: tuple(found) for column, found in tags.items()}


def governed_columns(
    columns: Sequence[WarehouseColumn],
    tags: Mapping[str, Sequence[QualifiedName]],
    patterns: Sequence[TagPattern],
) -> tuple[WarehouseColumn, ...]:
    """Return the columns with the sensitive tags Snowflake lists on each, as `patterns` match them.

    Args:
        tags: The tags on each column, by casefolded name, as `parse_column_tags` reads them.
    """
    return tuple(
        replace(column, governance_tags=matching_tags(tags.get(column.name.casefold(), ()), patterns))
        for column in columns
    )
