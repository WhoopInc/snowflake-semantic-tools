"""Connected validation's sample-value check: each value cast to the type its column has.

Snowflake casts every `SAMPLE_VALUES` entry to its member's type when it creates a view, as
`TRY_CAST` would, and refuses the view when one fails (docs/design/snowflake-behavior.md). The
offline check (SST-VAL341) refuses only what no reading of a type accepts; this one asks
Snowflake, with the type `DESCRIBE TABLE` reports, else the declared one, so a date the session
cannot parse, or a value a stale declared type let through, is found before apply. It reads no
table: each statement casts a list of literals, one statement per table.

A column is checked once however many views hold it, and its finding names them all.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.explain_projection import ObservedTypes
from snowflake_semantic_tools.app.verify_schema import plain_reference
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.enrich import same_data_type, semantic_data_type
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.sql import Sql, datatype, is_datatype, join, literal, number, sql
from snowflake_semantic_tools.domain.sql.datatype import type_family
from snowflake_semantic_tools.domain.validate.column_metadata import printable

# The families whose values Snowflake parses, so a cast can refuse one; text and the rest
# (binary, semi-structured, geospatial, vector) take every sample value as written.
_CAST_FAMILIES = frozenset(("fixed", "float", "boolean", "date", "time", "timestamp"))


@dataclass(frozen=True, slots=True)
class SampleColumn:
    """One published dbt column, as every view that holds it reads it.

    Attributes:
        fqn: The relation the column is read from.
        model: The dbt model, as its logical table names it, casefolded.
        column: The column, as its model's YAML names it: exactly when quoted, else casefolded.
        declared: The type the dbt manifest declares; None when it declares none.
        observed: The type `DESCRIBE TABLE` reports; None when the relation was not described.
        values: Its distinct sample values, in the order written; empty when it has none.
        views: The views that hold it, in compile order.
    """

    fqn: str
    model: str
    column: str
    declared: str | None
    observed: str | None
    values: tuple[str, ...]
    views: tuple[str, ...]

    @property
    def cast_type(self) -> str | None:
        """The type to cast to: the observed one, else the declared; None when neither parses one."""
        for text in (self.observed, self.declared):
            if text and is_datatype(text):
                return text
        return None

    @property
    def subject(self) -> str:
        """The column's artifact key, as the offline checks of its model's YAML name it."""
        return f"dbt_column:{self.model}.{self.column}"


def published_columns(views: Sequence[CompiledView], observed: ObservedTypes) -> tuple[SampleColumn, ...]:
    """Return each dbt column the views publish, once, by relation then upper-cased column name.

    A fact or dimension that is one plain `TABLE.COLUMN` reference is a dbt column; only those
    carry sample values, and only their declared types can drift from the relation's.
    """
    found: dict[tuple[str, str], SampleColumn] = {}
    for item in views:
        tables = {table.logical_name.casefold(): table for table in item.view.tables}
        for member in (*item.view.facts, *item.view.dimensions):
            reference = plain_reference(member)
            table = tables.get(reference[0].casefold()) if reference is not None else None
            if reference is None or table is None:
                continue
            key = (table.fqn, reference[1].upper())
            seen = found.get(key)
            if seen is not None:
                if item.name.casefold() not in seen.views:
                    found[key] = replace(seen, views=(*seen.views, item.name.casefold()))
                continue
            described = observed.get(table.fqn) or {}
            found[key] = SampleColumn(
                fqn=table.fqn,
                model=table.logical_name.casefold(),
                column=reference[1] if reference[2] else reference[1].casefold(),
                declared=member.data_type,
                observed=described.get(reference[1].upper()),
                values=tuple(dict.fromkeys(member.sample_values)),
                views=(item.name.casefold(),),
            )
    return tuple(found[key] for key in sorted(found))


def cast_reads(columns: Sequence[SampleColumn]) -> Mapping[str, Sql]:
    """Return, by relation, the statement that lists each sample value its type cannot hold.

    Each row of the answer is `(K, V)`: the column's index in that relation's
    `casted_columns`, and a value `TRY_CAST` refused. A relation none of whose columns has a
    type that parses values has no statement.
    """
    reads: dict[str, Sql] = {}
    for fqn in dict.fromkeys(column.fqn for column in columns):
        typed = casted_columns(columns, fqn)
        if not typed:
            continue
        rows = [
            sql("({index}, {value})", index=number(index), value=literal(value))
            for index, column in enumerate(typed)
            for value in column.values
            if _literal_safe(value)
        ]
        tests = [
            sql(
                "(K = {index} AND TRY_CAST(V AS {type}) IS NULL)",
                index=number(index),
                type=datatype(str(column.cast_type)),
            )
            for index, column in enumerate(typed)
        ]
        if rows:
            reads[fqn] = sql(
                "SELECT K, V FROM (VALUES {rows}) AS SST_SAMPLES (K, V) WHERE {tests}",
                rows=join(", ", rows),
                tests=join(" OR ", tests),
            )
    return reads


def casted_columns(columns: Sequence[SampleColumn], fqn: str) -> tuple[SampleColumn, ...]:
    """Return the columns of one relation whose type parses values, in `columns` order."""
    return tuple(
        column
        for column in columns
        if column.fqn == fqn
        and column.values
        and column.cast_type is not None
        and type_family(column.cast_type) in _CAST_FAMILIES
    )


def cast_findings(columns: Sequence[SampleColumn], fqn: str, result: QueryResult | None) -> list[Diagnostic]:
    """Report each value the cast of one relation refused, in the order its columns were cast.

    A read Snowflake refused is not reported here: the relation's EXPLAINs report one they
    cannot reach, and a value a cast cannot even be written for is left to apply.

    Diagnostics:
        SST-VAL419: once per column and value, naming every view that holds the column.
    """
    if result is None:
        return []
    typed = casted_columns(columns, fqn)
    refused: dict[int, list[str]] = {}
    for row in result.rows:
        index = _index(row[0]) if len(row) >= 2 else None
        if index is not None and index < len(typed):
            refused.setdefault(index, []).append(str(row[1]))
    found: list[Diagnostic] = []
    for index, column in enumerate(typed):
        bad = set(refused.get(index, ()))
        found.extend(
            D(
                "SST-VAL419",
                model=column.model,
                column=column.column,
                value=printable(value),
                type=str(column.cast_type),
                view=", ".join(column.views),
                subject=column.subject,
            )
            for value in column.values
            if value in bad
        )
    return found


def type_drift(columns: Sequence[SampleColumn]) -> list[Diagnostic]:
    """Report each column whose declared type names another family than the relation's.

    Diagnostics:
        SST-VAL327: once per column whose declared and described types differ, as enrich
            compares them.
    """
    return [
        D(
            "SST-VAL327",
            model=column.model,
            column=column.column,
            declared=column.declared,
            found=semantic_data_type(column.observed or ""),
            subject=column.subject,
        )
        for column in columns
        if column.declared and column.observed and not same_data_type(column.declared, column.observed)
    ]


def _literal_safe(value: str) -> bool:
    """Report whether a value can be written as a literal; one that cannot is SST-LOD022's."""
    try:
        literal(value)
    except ValueError:
        return False
    return True


def _index(value: object) -> int | None:
    """Read a row's column index, which the driver may return as an int or a Decimal."""
    try:
        index = int(str(value))
    except ValueError:
        return None
    return index if index >= 0 else None
