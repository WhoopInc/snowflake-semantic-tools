"""The statement connected validate EXPLAINs a member expression as, its columns typed.

A semantic view's metric, dimension, and filter expressions read its tables' columns, and
Snowflake refuses one whose argument types do not fit, such as `SUM` over a timestamp, when it
compiles the view. The check reproduces that without reading a row, and without the view or its
tables having to hold data: each `TABLE.COLUMN` reference becomes a column of a projection that
holds no row (`WHERE FALSE`), a NULL of the column's type. That type is the one `DESCRIBE TABLE`
reports in the target, else the one the dbt manifest declares; with neither, or one no NULL can
be written as, the reference is an untyped NULL. A reference to a metric or a filter, and a
variable or a metric named bare, is an untyped NULL too: their types are known only once
Snowflake compiles the view itself.

Nothing here writes; `describe_columns` is the only statement that reads the account.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import TypeAlias

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind, SemanticView
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import (
    AuthoredExpression,
    Sql,
    datatype,
    expr,
    guard_expression,
    ident,
    is_datatype,
    join,
    qname,
    sql,
)

# What `DESCRIBE TABLE` reported for each table the views read, by the table's FQN: each
# column's type by its upper-cased name, or None when the table could not be described.
ObservedTypes: TypeAlias = Mapping[str, Mapping[str, str] | None]

# A qualified reference as authored: `TABLE.COLUMN`, each part one unquoted identifier.
_QUALIFIED = r"\b[A-Za-z_][A-Za-z0-9_$]*\s*\.\s*[A-Za-z_][A-Za-z0-9_$]*\b"
# A collation is part of a column's type as DESCRIBE reports it, but CAST refuses one.
_COLLATION = re.compile(r"\s+COLLATE\s+'(?:[^']|'')*'\s*$", re.IGNORECASE)
# A structured ARRAY or OBJECT is typed as its semi-structured base, which `datatype` accepts.
_STRUCTURED = re.compile(r"(ARRAY|OBJECT)\s*\(.*\)", re.IGNORECASE | re.DOTALL)


def describe_columns(port: ExecutionPort, fqn: str) -> Mapping[str, str] | None:
    """Return each column's type as `DESCRIBE TABLE` reports it, for a table or a view.

    Never writes, and never raises for a relation Snowflake cannot describe.

    Returns:
        The types by upper-cased column name; None when the relation is absent or unreadable,
        or the answer has no `name` and `type` columns.
    """
    try:
        result = port.query(sql("DESCRIBE TABLE {table}", table=qname(QualifiedName.parse(fqn))))
    except SnowflakePortError:
        return None
    headings = [str(heading).casefold() for heading in result.columns]
    if "name" not in headings or "type" not in headings:
        return None
    name, kind = headings.index("name"), headings.index("type")
    return MappingProxyType(
        {str(row[name]).upper(): str(row[kind]) for row in result.rows if len(row) > max(name, kind)}
    )


def typed_null(type_text: str) -> Sql | None:
    """Return a NULL of the type `type_text` names, or None when it names none SST can write.

    A collation is dropped and a structured ARRAY or OBJECT becomes its base type, neither of
    which changes what an expression over the column compiles to. A GEOGRAPHY or GEOMETRY is
    converted from a NULL string, since Snowflake casts no NULL to either. A type outside
    `datatype`'s grammar, such as MAP, gives None.
    """
    text = _COLLATION.sub("", type_text).strip()
    structured = _STRUCTURED.fullmatch(text)
    if structured is not None:
        text = structured.group(1)
    if not is_datatype(text):
        return None
    if text.upper() == "GEOGRAPHY":
        return sql("TO_GEOGRAPHY(CAST(NULL AS VARCHAR))")
    if text.upper() == "GEOMETRY":
        return sql("TO_GEOMETRY(CAST(NULL AS VARCHAR))")
    return sql("CAST(NULL AS {type})", type=datatype(text))


def column_types(view: SemanticView, observed: ObservedTypes) -> Mapping[str, Sql]:
    """Return the typed NULL each column of the view's tables stands for, by `TABLE.COLUMN` upper-cased.

    A column `observed` describes is typed as described; else a fact or dimension is typed as
    the dbt manifest declares it. A name a metric or a filter of the same table also has is
    left out, since a reference to it names that member, as is a type `typed_null` refuses.
    """
    texts = {
        column.qualified_name.upper(): column.data_type
        for column in view.columns
        if column.kind is not ColumnKind.FILTER and column.data_type
    }
    for table in view.tables:
        described = observed.get(table.fqn) or {}
        texts.update({f"{table.logical_name}.{name}".upper(): kind for name, kind in described.items()})
    members = {metric.qualified_name.upper() for metric in view.metrics if metric.table is not None}
    members.update(column.qualified_name.upper() for column in view.columns if column.kind is ColumnKind.FILTER)
    typed = {key: typed_null(text) for key, text in texts.items() if key not in members}
    return MappingProxyType({key: null for key, null in sorted(typed.items()) if null is not None})


def explain_expression(expression: AuthoredExpression, view: SemanticView, types: Mapping[str, Sql]) -> Sql:
    """EXPLAIN one member expression over a projection of the view's columns that holds no row.

    The projection holds an untyped NULL for each column name the view declares, which an
    unqualified reference reads, and a typed NULL for each typed `TABLE.COLUMN` the expression
    reads. The rewritten expression is guarded again, so a statement it builds still holds one
    expression; a rewrite cannot fail that guard, since it only swaps words for NULL or for a
    quoted name.
    """
    text, used = _test_expression(expression.text, view, types)
    names = sorted({column.name.upper() for column in view.columns})
    nulls = [sql("NULL AS {column}", column=ident(Identifier.parse(name))) for name in names]
    nulls.extend(sql("{value} AS {column}", value=types[key], column=ident(_alias(key))) for key in used)
    projection = (
        sql("SELECT {columns} WHERE FALSE", columns=join(", ", nulls))
        if nulls
        else sql("SELECT 1 AS SST_VALUE WHERE FALSE")
    )
    return sql(
        "EXPLAIN SELECT {expression} FROM ({projection}) AS SST_VALIDATE",
        expression=expr(guard_expression(text)),
        projection=projection,
    )


def _test_expression(expression: str, view: SemanticView, types: Mapping[str, Sql]) -> tuple[str, tuple[str, ...]]:
    """Rewrite an expression to read the projection, returning it and the typed references it reads.

    Each `TABLE.COLUMN` becomes the projection's typed column when `types` has one, else NULL,
    and each variable and metric named bare becomes NULL; one pass makes every swap, so no
    swapped-in name is rewritten again. A `PARTITION BY EXCLUDING` list names the dimensions a
    query chooses, so it is collapsed to `PARTITION BY NULL` first.
    """
    names = "|".join(
        re.escape(name) for name in (*(item.name for item in view.variables), *(m.name for m in view.metrics))
    )
    bare = rf"|\b(?:{names})\b" if names else ""
    item = rf"(?:{_QUALIFIED}|\bNULL\b{bare})"
    text = re.sub(
        rf"PARTITION\s+BY\s+EXCLUDING\s+{item}(?:\s*,\s*{item})*", "PARTITION BY NULL", expression, flags=re.IGNORECASE
    )
    used: dict[str, None] = {}

    def swap(match: re.Match[str]) -> str:
        reference = match.group("qualified")
        key = re.sub(r"\s+", "", reference).upper() if reference is not None else None
        if key is None or key not in types:
            return "NULL"
        used[key] = None
        return str(ident(_alias(key)))

    return re.sub(rf"(?P<qualified>{_QUALIFIED}){bare}", swap, text, flags=re.IGNORECASE), tuple(sorted(used))


def _alias(key: str) -> Identifier:
    """Name the projection's column for one `TABLE.COLUMN`: the reference itself, quoted."""
    return Identifier(key, quoted=True)
