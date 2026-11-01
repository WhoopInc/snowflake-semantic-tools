"""Read the document `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` returns, for every reader of a live view.

`sst plan` compares a live view with what SST renders, and `sst import` maps it to project files;
both read the same document. This module reads its structure into records, so the two agree on
what a live view holds, and leaves each to interpret the values it compares or writes: a name's
case, a window, a variable's default.

Each value is the document's own text, or empty when the key is missing or empty; a list entry that is
not a mapping is skipped, as is any key read here does not name. Only a variable's default and
a verified query's timestamp keep their type, since each reader converts them its own way.

The document's keys are Snowflake's semantic view YAML (`docs.SEMANTIC_VIEW_YAML`), read back by
`docs.READ_YAML_FROM_SEMANTIC_VIEW`. Only the parsed mapping is read here; parsing the text is
the adapter's (`adapters.bounded_yaml`).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# The column sections of a table, in the order both readers walk them.
COLUMN_SECTIONS = ("facts", "dimensions", "time_dimensions")


@dataclass(frozen=True, slots=True)
class YamlColumn:
    """One fact, dimension, time dimension or filter of a table.

    Attributes:
        section: `facts`, `dimensions` or `time_dimensions`: the list it is in.
        name: Its name.
        expr: Its expression; a plain column is `TABLE.NAME`, or `NAME` alone for some views.
        labels: Its labels; `filter` makes it a filter, whatever its section.
        synonyms: Its synonyms.
        description: Its comment.
        sample_values: Its sample values, as text.
        is_enum: The document says `is_enum: true`.
        access_modifier: `private_access` for a private one; empty otherwise.
    """

    section: str
    name: str
    expr: str
    labels: tuple[str, ...]
    synonyms: tuple[str, ...]
    description: str
    sample_values: tuple[str, ...]
    is_enum: bool
    access_modifier: str

    @property
    def is_filter(self) -> bool:
        """Whether a `filter` label makes it a filter."""
        return "filter" in {label.casefold() for label in self.labels}


@dataclass(frozen=True, slots=True)
class YamlSortKey:
    """One non-additive dimension of a metric.

    Attributes:
        table: The dimension's table.
        dimension: The dimension.
        sort_direction: `ascending` or `descending`, as the document spells it; empty when unset.
        null_order: `first` or `last`; empty when unset.
    """

    table: str
    dimension: str
    sort_direction: str
    null_order: str


@dataclass(frozen=True, slots=True)
class YamlMetric:
    """One metric, of a table or of the view.

    Attributes:
        name: Its name, unqualified.
        expr: Its expression, a window metric's `OVER (...)` included.
        using_relationships: The relationships its `USING` names.
        non_additive_dimensions: Its non-additive dimensions, in order.
        synonyms: Its synonyms.
        description: Its comment.
        access_modifier: `private_access` for a private one; empty otherwise.
    """

    name: str
    expr: str
    using_relationships: tuple[str, ...]
    non_additive_dimensions: tuple[YamlSortKey, ...]
    synonyms: tuple[str, ...]
    description: str
    access_modifier: str


@dataclass(frozen=True, slots=True)
class YamlConstraint:
    """A table's distinct-range constraint.

    Attributes:
        name: Its name.
        start_column: The range's start column.
        end_column: The range's end column.
    """

    name: str
    start_column: str
    end_column: str


@dataclass(frozen=True, slots=True)
class YamlTable:
    """One logical table of the view.

    Attributes:
        name: Its logical name.
        base_table: Its base table's `(database, schema, table)`, each empty when missing.
        primary_key: Its primary key's columns.
        unique_keys: Each unique key's columns.
        synonyms: Its synonyms.
        description: Its comment.
        constraints: Its distinct-range constraints.
        columns: Its facts, dimensions, time dimensions and filters, by section in
            `COLUMN_SECTIONS` order.
        metrics: Its metrics.
    """

    name: str
    base_table: tuple[str, str, str]
    primary_key: tuple[str, ...]
    unique_keys: tuple[tuple[str, ...], ...]
    synonyms: tuple[str, ...]
    description: str
    constraints: tuple[YamlConstraint, ...]
    columns: tuple[YamlColumn, ...]
    metrics: tuple[YamlMetric, ...]


@dataclass(frozen=True, slots=True)
class YamlRelationshipColumn:
    """One column pair of a relationship.

    Attributes:
        type: `asof` or `range` for those kinds; empty for an equality.
        left_column: The left table's column.
        right_column: The right table's column; empty for a range.
        range_start: A range's start column on the right table; empty otherwise.
        range_end: A range's end column on the right table; empty otherwise.
    """

    type: str
    left_column: str
    right_column: str
    range_start: str
    range_end: str


@dataclass(frozen=True, slots=True)
class YamlRelationship:
    """One relationship.

    Attributes:
        name: Its name.
        left_table: The table that references the other.
        right_table: The table referenced.
        columns: Its column pairs, in order.
    """

    name: str
    left_table: str
    right_table: str
    columns: tuple[YamlRelationshipColumn, ...]


@dataclass(frozen=True, slots=True)
class YamlVariable:
    """One variable.

    Attributes:
        name: Its name.
        data_type: Its type, as the document spells it, such as `NUMBER(38,0)`.
        default_value: Its default as the document holds it: text, a number, or None.
        description: Its comment.
    """

    name: str
    data_type: str
    default_value: object
    description: str


@dataclass(frozen=True, slots=True)
class YamlVerifiedQuery:
    """One verified query.

    Attributes:
        name: Its name.
        question: The question it answers.
        sql: Its SQL.
        verified_at: When it was verified, as the document holds it: seconds, or None.
        verified_by: Who verified it.
        use_as_onboarding_question: The document says `use_as_onboarding_question: true`.
    """

    name: str
    question: str
    sql: str
    verified_at: object
    verified_by: str
    use_as_onboarding_question: bool


@dataclass(frozen=True, slots=True)
class YamlView:
    """A live semantic view, as `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` shows it.

    Attributes:
        lists_tables: The document holds a `tables` list, as every semantic view's does.
        tables: Its logical tables.
        relationships: Its relationships.
        metrics: Its view-level metrics; each table's are on the table.
        variables: Its variables.
        verified_queries: Its verified queries.
        sql_generation: Its `AI_SQL_GENERATION` instruction text.
        question_categorization: Its `AI_QUESTION_CATEGORIZATION` instruction text.
        description: Its comment, with SST's ownership marker when SST published it.
        max_staleness: Its `max_staleness`, as written, such as `1 hour`.
    """

    lists_tables: bool
    tables: tuple[YamlTable, ...]
    relationships: tuple[YamlRelationship, ...]
    metrics: tuple[YamlMetric, ...]
    variables: tuple[YamlVariable, ...]
    verified_queries: tuple[YamlVerifiedQuery, ...]
    sql_generation: str
    question_categorization: str
    description: str
    max_staleness: str


def read_view(document: Mapping[str, Any]) -> YamlView:
    """Read the structure of a parsed `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` document."""
    instructions = _entry(document.get("module_custom_instructions"))
    return YamlView(
        isinstance(document.get("tables"), list),
        tuple(_table(item) for item in _items(document.get("tables"))),
        tuple(_relationship(item) for item in _items(document.get("relationships"))),
        tuple(_metric(item) for item in _items(document.get("metrics"))),
        tuple(
            YamlVariable(
                _text(item.get("name")),
                _text(item.get("data_type")),
                item.get("default_value"),
                _text(item.get("description")),
            )
            for item in _items(document.get("variables"))
        ),
        tuple(_query(item) for item in _items(document.get("verified_queries"))),
        _text(instructions.get("sql_generation")),
        _text(instructions.get("question_categorization")),
        _text(document.get("description")),
        _text(document.get("max_staleness")),
    )


def _table(table: Mapping[str, Any]) -> YamlTable:
    base = _entry(table.get("base_table"))
    database, schema, name = (_text(base.get(key)) for key in ("database", "schema", "table"))
    return YamlTable(
        _text(table.get("name")),
        (database, schema, name),
        _strings(_entry(table.get("primary_key")).get("columns")),
        tuple(_strings(key.get("columns")) for key in _items(table.get("unique_keys"))),
        _strings(table.get("synonyms")),
        _text(table.get("description")),
        tuple(
            YamlConstraint(_text(item.get("name")), _text(span.get("start_column")), _text(span.get("end_column")))
            for item in _items(table.get("constraints"))
            if isinstance(span := item.get("distinct_range"), Mapping)
        ),
        tuple(_column(section, item) for section in COLUMN_SECTIONS for item in _items(table.get(section))),
        tuple(_metric(item) for item in _items(table.get("metrics"))),
    )


def _column(section: str, item: Mapping[str, Any]) -> YamlColumn:
    return YamlColumn(
        section,
        _text(item.get("name")),
        _text(item.get("expr")),
        _strings(item.get("labels")),
        _strings(item.get("synonyms")),
        _text(item.get("description")),
        _strings(item.get("sample_values")),
        item.get("is_enum") is True,
        _text(item.get("access_modifier")),
    )


def _metric(item: Mapping[str, Any]) -> YamlMetric:
    return YamlMetric(
        _text(item.get("name")),
        _text(item.get("expr")),
        _strings(item.get("using_relationships")),
        tuple(
            YamlSortKey(
                _text(entry.get("table")),
                _text(entry.get("dimension")),
                _text(entry.get("sort_direction")),
                _text(entry.get("null_order")),
            )
            for entry in _items(item.get("non_additive_dimensions"))
        ),
        _strings(item.get("synonyms")),
        _text(item.get("description")),
        _text(item.get("access_modifier")),
    )


def _relationship(item: Mapping[str, Any]) -> YamlRelationship:
    columns = []
    for entry in _items(item.get("relationship_columns")):
        span = _entry(entry.get("right_range"))
        columns.append(
            YamlRelationshipColumn(
                _text(entry.get("type")),
                _text(entry.get("left_column")),
                _text(entry.get("right_column")),
                _text(span.get("start_column")),
                _text(span.get("end_column")),
            )
        )
    return YamlRelationship(
        _text(item.get("name")), _text(item.get("left_table")), _text(item.get("right_table")), tuple(columns)
    )


def _query(item: Mapping[str, Any]) -> YamlVerifiedQuery:
    return YamlVerifiedQuery(
        _text(item.get("name")),
        _text(item.get("question")),
        _text(item.get("sql")),
        item.get("verified_at"),
        _text(item.get("verified_by")),
        item.get("use_as_onboarding_question") is True,
    )


def _text(value: object) -> str:
    """The value as text; empty when missing, or empty or zero as the document holds it."""
    return str(value or "")


def _items(value: object) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in value if isinstance(item, Mapping)) if isinstance(value, list) else ()


def _entry(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in value) if isinstance(value, list) else ()
