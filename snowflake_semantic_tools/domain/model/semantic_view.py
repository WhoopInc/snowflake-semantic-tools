"""Frozen dataclasses for a semantic view and its members.

PURE RING. Nothing here reads a file, a clock or the environment; everything the
model needs arrives already resolved. In particular the target database and schema
are baked into `Table.fqn` by the loader, because a model that resolved
`{{ target.database }}` itself would need the dbt profile, which would need the
filesystem, which would make rendering non-reproducible and golden testing
impossible.

`ref()` is likewise already resolved: the loader turns `{{ ref('products') }}` into
`SST_REF_DEV.JAFFLE.PRODUCTS` before constructing anything here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from snowflake_semantic_tools.domain.sql import AuthoredExpression, AuthoredQuery, Sql


class ColumnKind(Enum):
    """Which clause a column renders into.

    A column is a FACT or a DIMENSION, never both, and the choice is authored
    rather than inferred -- `column_type:` in the dbt model's `meta.sst`. FILTER is
    a DIMENSION carrying `LABELS = (FILTER)`, not a clause of its own: the golden
    folds filters into `DIMENSIONS`, and `FILTERS (...)` appears in no grammar.
    """

    FACT = "fact"
    DIMENSION = "dimension"
    TIME_DIMENSION = "time_dimension"
    FILTER = "filter"


@dataclass(frozen=True, slots=True)
class Column:
    """One entry inside `FACTS (...)` or `DIMENSIONS (...)`.

    `table` and `name` are already upper-cased by the loader, and each is one identifier.
    `expr` is the right side of `AS`, guarded: for a plain column that is `TABLE.COLUMN`,
    and for a filter it is a predicate such as `ORDERS.ORDER_STATE = 'completed'`.
    """

    table: str
    name: str
    kind: ColumnKind
    expr: AuthoredExpression
    comment: str | None = None
    synonyms: tuple[str, ...] = ()
    sample_values: tuple[str, ...] = ()
    is_enum: bool = False

    @property
    def qualified_name(self) -> str:
        """`TABLE.NAME`: how the DDL names the column, and what columns sort by when rendered."""
        return f"{self.table}.{self.name}"


@dataclass(frozen=True, slots=True)
class Table:
    """One entry inside `TABLES (...)`.

    `logical_name` is the name members qualify against; `fqn` is the physical table
    it resolves to. They differ in case only for now -- role-playing, which would
    let one physical table appear under two logical names, is not supported in 1.0,
    so a view cannot contain the same physical table twice.
    """

    logical_name: str
    fqn: str
    primary_key: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    synonyms: tuple[str, ...] = ()
    comment: str | None = None
    distinct_range: tuple[str, str] | None = None


@dataclass(frozen=True, slots=True)
class Relationship:
    """One entry inside `RELATIONSHIPS (...)`, including temporal modifiers."""

    name: str
    from_table: str
    from_columns: tuple[str, ...]
    to_table: str
    to_columns: tuple[str, ...]
    asof_index: int | None = None
    range_bounds: tuple[str, str] | None = None


@dataclass(frozen=True, slots=True)
class Variable:
    """One entry inside `VARIABLES (...)`.

    Declares a name a metric or filter `expr` may use as a bare identifier.
    `data_type` has matched Snowflake's type grammar, and `default` is the SQL literal the
    loader made of the YAML value -- `False` into `FALSE`, not `False`.
    """

    name: str
    data_type: Sql
    default: Sql
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class SortKey:
    """One ordered expression: a `NON ADDITIVE BY` dimension or a window `ORDER BY` entry.

    `descending` and `nulls_first` are None when the author left them unstated, so
    the DDL says exactly what was written and Snowflake's defaults decide the rest.
    """

    expr: AuthoredExpression
    descending: bool | None = None
    nulls_first: bool | None = None


@dataclass(frozen=True, slots=True)
class Window:
    """The `OVER (...)` of a window function metric.

    At most one of `partition_by` and `partition_excluding` is set: EXCLUDING
    partitions by every dimension a query requests except those named. `frame` is a
    frame clause the loader has already checked against Snowflake's grammar.
    """

    partition_by: tuple[AuthoredExpression, ...] = ()
    partition_excluding: tuple[AuthoredExpression, ...] = ()
    order_by: tuple[SortKey, ...] = ()
    frame: AuthoredExpression | None = None


@dataclass(frozen=True, slots=True)
class Metric:
    """One entry inside `METRICS (...)`.

    `table` is None for a cross-table metric, which renders unqualified -- for
    example `REVENUE_PER_CUSTOMER AS DIV0(...)` drawing on two tables at once.
    """

    name: str
    expr: AuthoredExpression
    table: str | None = None
    comment: str | None = None
    synonyms: tuple[str, ...] = ()
    using_relationships: tuple[str, ...] = ()
    # Order is significant: rows sort by these keys and the last row's value counts.
    non_additive_by: tuple[SortKey, ...] = ()
    access_modifier: str = "public_access"
    # Set for a window function metric, whose `expr` is the call the window applies to.
    window: Window | None = None

    @property
    def qualified_name(self) -> str:
        """`TABLE.NAME`, or the bare name of a cross-table metric: how the DDL names the metric."""
        return self.name if self.table is None else f"{self.table}.{self.name}"


@dataclass(frozen=True, slots=True)
class VerifiedQuery:
    """One entry inside `AI_VERIFIED_QUERIES (...)`.

    NOT `VERIFIED QUERIES (...)`, which appears in no grammar. `verified_at` is a
    Unix timestamp supplied by the loader; domain never asks a clock for it.
    """

    name: str
    question: str
    sql: AuthoredQuery
    verified_at: int | None = None
    verified_by: str | None = None
    onboarding_question: bool | None = None


@dataclass(frozen=True, slots=True)
class Tag:
    """One entry inside `WITH TAG (...)`. `name` is already fully qualified."""

    name: str
    value: str


@dataclass(frozen=True, slots=True)
class SemanticView:
    """A complete semantic view, resolved and ready to render.

    Field order here mirrors the clause order in the DDL, which is the order the
    golden was verified against Snowflake in -- see `domain/render/semantic_view.py`
    for the authority on that order and for what is deliberately absent from it.
    """

    fqn: str
    tables: tuple[Table, ...]
    relationships: tuple[Relationship, ...] = ()
    variables: tuple[Variable, ...] = ()
    columns: tuple[Column, ...] = ()
    metrics: tuple[Metric, ...] = ()
    comment: str | None = None
    ai_sql_generation: str | None = None
    ai_question_categorization: str | None = None
    custom_instruction_names: tuple[str, ...] = ()
    verified_queries: tuple[VerifiedQuery, ...] = ()
    max_staleness: str | None = None
    tags: tuple[Tag, ...] = ()
    or_replace: bool = True
    if_not_exists: bool = False
    source_path: str | None = None
    source_files: tuple[str, ...] = ()
    referenced_models: tuple[str, ...] = ()
    ownership_marker: str | None = None

    facts: tuple[Column, ...] = field(init=False, repr=False, compare=False)
    dimensions: tuple[Column, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        # A filter is a dimension carrying LABELS = (FILTER), so it is partitioned
        # here rather than in the renderer -- the renderer should not have to know
        # that FILTER is not its own clause.
        facts = tuple(c for c in self.columns if c.kind is ColumnKind.FACT)
        dims = tuple(c for c in self.columns if c.kind is not ColumnKind.FACT)
        object.__setattr__(self, "facts", facts)
        object.__setattr__(self, "dimensions", dims)
