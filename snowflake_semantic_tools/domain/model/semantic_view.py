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

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.setting import Setting
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

    `table` and `name` are already upper-cased by the loader, and each is one identifier; a
    column dbt quotes keeps its exact case and its double quotes in `name`.
    `expr` is the right side of `AS`, guarded: for a plain column that is `TABLE.COLUMN`,
    and for a filter it is a predicate such as `ORDERS.ORDER_STATE = 'completed'`.

    Attributes:
        table: The logical name of the table the column belongs to, upper-cased.
        name: The column's name, upper-cased unless dbt quotes it.
        kind: Which clause the column renders into; a filter renders as a dimension.
        expr: The expression after `AS`: the column reference, or a filter's predicate.
        comment: The column's description, rendered as its COMMENT; None when it has none.
        synonyms: The names rendered in `WITH SYNONYMS`; empty when none.
        sample_values: The values rendered in `SAMPLE_VALUES`; empty when none, and always empty for a
            sensitive column or one whose values look like personal data.
        is_enum: Whether the column renders `IS_ENUM`; never true when its sample values are withheld.
        data_type: The type the dbt manifest declares for the column, as written; None for a
            filter and for a column that declares none. Not rendered: connected validation
            types the column by it when the warehouse cannot describe the table.
    """

    table: str
    name: str
    kind: ColumnKind
    expr: AuthoredExpression
    comment: str | None = None
    synonyms: tuple[str, ...] = ()
    sample_values: tuple[str, ...] = ()
    is_enum: bool = False
    data_type: str | None = None

    @property
    def qualified_name(self) -> str:
        """`TABLE.NAME`, a quoted name without its quotes: what columns sort, match and are keyed by."""
        name = self.name[1:-1].replace('""', '"') if self.name.startswith('"') else self.name
        return f"{self.table}.{name}"


@dataclass(frozen=True, slots=True)
class Table:
    """One entry inside `TABLES (...)`.

    `logical_name` is the name members qualify against; `fqn` is the physical table
    it resolves to. They differ in case only for now -- role-playing, which would
    let one physical table appear under two logical names, is not supported in 1.0,
    so a view cannot contain the same physical table twice.

    Attributes:
        logical_name: The upper-cased name members qualify against.
        fqn: The fully qualified physical table, the dbt model's relation name.
        primary_key: The columns rendered in `PRIMARY KEY (...)`; empty when the model declares none.
        unique_keys: Each column set rendered as its own `UNIQUE (...)`; empty when none.
        synonyms: The names rendered in `WITH SYNONYMS`; empty when none.
        comment: The model's description, rendered as the table's COMMENT; None when it has none.
        distinct_range: The start and end columns of the table's `DISTINCT RANGE` constraint; None when
            the model declares no range.
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
    """One entry inside `RELATIONSHIPS (...)`, including temporal modifiers.

    Attributes:
        name: The relationship's name, upper-cased.
        from_table, to_table: The logical names of the referencing and the referenced table, upper-cased.
        from_columns, to_columns: The paired join columns on each side, in the same order.
        asof_index: The position in `to_columns` of the column rendered with `ASOF`; None for a
            relationship that is not ASOF.
        range_bounds: The start and end columns of the referenced table that replace `to_columns` with
            `BETWEEN ... EXCLUSIVE`; None for a relationship that is not a range.
    """

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

    Attributes:
        name: The variable's name, unique in the view whatever its case.
        data_type: The variable's SQL type.
        default: The SQL literal rendered after `DEFAULT`.
        comment: The variable's description, rendered as its COMMENT; None when it has none.
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

    Attributes:
        expr: The expression ordered by.
        descending: True renders `DESC`, False `ASC`, None neither.
        nulls_first: True renders `NULLS FIRST`, False `NULLS LAST`, None neither.
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

    Attributes:
        partition_by: The expressions rendered after `PARTITION BY`; empty when none.
        partition_excluding: The expressions rendered after `PARTITION BY EXCLUDING`; empty when none.
        order_by: The keys rendered after `ORDER BY`; empty when none.
        frame: The frame clause rendered last; None when the window has none.
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

    Attributes:
        name: The metric's name.
        expr: The expression after `AS`; for a window function metric, the call the window applies to.
        table: The logical name of the table the metric qualifies against; None for a cross-table metric.
        comment: The metric's description, rendered as its COMMENT; None when it has none.
        synonyms: The names rendered in `WITH SYNONYMS`; empty when none.
        using_relationships: The relationships rendered in `USING (...)`; empty when none.
        non_additive_by: The keys rendered in `NON ADDITIVE BY (...)`, in significant order; empty when
            the metric is additive.
        access_modifier: `public_access` by default; `private_access` renders the metric `PRIVATE` and
            leaves it out of smoke probes.
        window: The `OVER (...)` of a window function metric; None for any other metric.
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

    Attributes:
        name: The query's name.
        question: The natural-language question rendered as `QUESTION`.
        sql: The query as authored, rendered as `SQL` inside a string literal.
        verified_at: The Unix timestamp rendered as `VERIFIED_AT`; None leaves it out.
        verified_by: Who verified the query, rendered as `VERIFIED_BY`; None leaves it out.
        onboarding_question: The value rendered as `ONBOARDING_QUESTION`; None leaves it out.
    """

    name: str
    question: str
    sql: AuthoredQuery
    verified_at: int | None = None
    verified_by: str | None = None
    onboarding_question: bool | None = None


@dataclass(frozen=True, slots=True)
class Tag:
    """One entry inside `WITH TAG (...)`. `name` is already fully qualified.

    Attributes:
        name: The tag's fully qualified name.
        value: The tag's value, rendered as a string literal.
    """

    name: str
    value: str


# Each scope kind's include key, exclude key, and the word a diagnostic names its members by.
SCOPE_KINDS: tuple[tuple[str, str, str], ...] = (
    ("columns", "exclude_columns", "column"),
    ("metrics", "exclude_metrics", "metric"),
    ("relationships", "exclude_relationships", "relationship"),
)


@dataclass(frozen=True, slots=True)
class ViewScope:
    """Which of its tables' members a view exposes: its scope lists, include or exclude.

    The lists are `columns:`, `metrics:` and `relationships:`, or their `exclude_`
    counterparts. For each kind, an include list of None admits every member and an exclude
    list removes the members it names; validation refuses a view that sets both for one
    kind. Names are upper-cased: a column is `TABLE.COLUMN` under its logical table, a metric
    or a relationship its name. A filter is a dimension the scope never removes: filters are
    attached by their own tables, not listed.

    Attributes:
        columns, metrics, relationships: The include lists, one per kind; None admits every member of
            that kind.
        exclude_columns, exclude_metrics, exclude_relationships: The exclude lists, one per kind; empty
            removes nothing.
    """

    columns: tuple[str, ...] | None = None
    exclude_columns: tuple[str, ...] = ()
    metrics: tuple[str, ...] | None = None
    exclude_metrics: tuple[str, ...] = ()
    relationships: tuple[str, ...] | None = None
    exclude_relationships: tuple[str, ...] = ()

    def admits_column(self, column: Column) -> bool:
        """Report whether the view exposes `column`; a filter it always does."""
        return column.kind is ColumnKind.FILTER or self.admits_column_name(column.qualified_name)

    def admits_column_name(self, name: str) -> bool:
        """Report whether the view exposes the fact or dimension `TABLE.COLUMN`, compared upper-cased."""
        return _admits(name.upper(), self.columns, self.exclude_columns)

    def admits_metric(self, name: str) -> bool:
        """Report whether the view exposes the metric named `name`, compared upper-cased."""
        return _admits(name.upper(), self.metrics, self.exclude_metrics)

    def admits_relationship(self, name: str) -> bool:
        """Report whether the view keeps the relationship named `name`, compared upper-cased."""
        return _admits(name.upper(), self.relationships, self.exclude_relationships)


def _admits(name: str, included: tuple[str, ...] | None, excluded: tuple[str, ...]) -> bool:
    return (included is None or name in included) and name not in excluded


@dataclass(frozen=True, slots=True)
class SemanticView:
    """A complete semantic view, resolved and ready to render.

    Field order here mirrors the clause order in the DDL, which is the order the
    golden was verified against Snowflake in -- see `domain/render/semantic_view.py`
    for the authority on that order and for what is deliberately absent from it.

    Authored keys: `authored_schema.SEMANTIC_KINDS`.

    Attributes:
        fqn: The view's fully qualified name under the target database and schema.
        tables: The view's tables, one per model or source it reads.
        relationships: The view's attached relationships.
        variables: The view's variables, in declaration order.
        columns: The facts, dimensions and filters the view holds.
        metrics: The view's attached metrics.
        comment: The view's description, rendered as its COMMENT; None when it has none.
        ai_sql_generation, ai_question_categorization: The text of each AI clause, composed by the loader
            from the attached custom instructions; None when no instruction supplies it.
        custom_instruction_names: The names of the custom instructions composed into the AI text.
        verified_queries: The entries rendered in `AI_VERIFIED_QUERIES`; empty when none.
        max_staleness: The `MAX_STALENESS` value, such as `3600 seconds`; None when unset.
        tags: The entries rendered in `WITH TAG`; empty when none.
        or_replace, if_not_exists: Which CREATE modifier renders; `or_replace` also renders
            `COPY GRANTS`. Setting both is a caller's bug.
        source_path: The project-relative path of the view's own file; None when not loaded from one.
        source_files: Every project file the view is built from, sorted: its own file, its members'
            files, verified query SQL files and its models' dbt files.
        referenced_models: The lower-cased names of the dbt models the view's tables read, sorted.
        ownership_marker: The marker text compile appends to the COMMENT when publishing; None
            otherwise.
        scope: Which of its tables' members the view exposes.
        member_origins: Where each attached member is declared, by member key such as
            `metric:order_count`; a member the loader recorded no origin for is absent. Not
            rendered and not compared: it locates a connected check's finding.
        settings: The view's resolved settings, each with where its value came from, sorted by
            key, as `sst list --long` reports them. Not rendered and not compared: the values
            that render are the fields above.
        folder: The folder its `semantic_views:` routes fold along: its file's directory below
            the semantic-model directory, one segment per directory. Not rendered and not
            compared, like `settings`.
        table_models: The dbt model each table reads, by the table's logical name; a `source()`
            table has none. Not rendered and not compared: the connected tag check (SST-VAL338)
            reads what each model's YAML records.
        facts, dimensions: `columns` split by kind on construction: the facts, and everything else,
            filters included.
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
    scope: ViewScope = ViewScope()
    member_origins: Mapping[str, Origin] = field(
        default_factory=lambda: MappingProxyType({}), repr=False, compare=False
    )
    settings: tuple[Setting, ...] = field(default=(), repr=False, compare=False)
    folder: tuple[str, ...] = field(default=(), repr=False, compare=False)
    table_models: Mapping[str, DbtModel] = field(
        default_factory=lambda: MappingProxyType({}), repr=False, compare=False
    )

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
