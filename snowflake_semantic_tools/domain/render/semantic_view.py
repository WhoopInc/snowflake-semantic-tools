"""Pure `render(view) -> Sql` for `CREATE SEMANTIC VIEW`.

THE CLAUSE ORDER IS THE CONTRACT, and it is Snowflake's:

    CREATE [OR REPLACE] SEMANTIC VIEW <fqn>
      TABLES (...)
      RELATIONSHIPS (...)
      VARIABLES (...)
      FACTS (...)
      DIMENSIONS (...)          <- filters live HERE, as LABELS = (FILTER)
      METRICS (...)
      COMMENT = '...'
      AI_SQL_GENERATION '...'
      AI_QUESTION_CATEGORIZATION '...'
      AI_VERIFIED_QUERIES (...)
      MAX_STALENESS = '...'
      WITH TAG (...)
      COPY GRANTS

WHAT IS DELIBERATELY ABSENT. `FILTERS (...)`, `VERIFIED QUERIES (...)` and
`CUSTOM INSTRUCTIONS (...)` appear in no grammar: filters fold into `DIMENSIONS`
with `LABELS = (FILTER)`, verified queries become `AI_VERIFIED_QUERIES`, and
custom instructions become `AI_SQL_GENERATION` and `AI_QUESTION_CATEGORIZATION`
after `COMMENT`. The goldens, each created in Snowflake, hold this shape.

SCOPE. A view's `scope` decides which of its columns, metrics and relationships render; a
member it does not admit is left out of its clause, so a view built with every attached
member still publishes only the subset its author listed.

ORDERING. `TABLES` renders in DECLARATION order, because that is the order the
author wrote and the golden preserves it. Every other member list renders SORTED
by its qualified name. The difference is not an inconsistency: table order is
authored information, member order is not, and sorting members is what keeps a
re-render byte-identical when an unrelated column is added.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    SortKey,
    Table,
    Variable,
    VerifiedQuery,
    ViewScope,
    Window,
)
from snowflake_semantic_tools.domain.sql import (
    Sql,
    boolean,
    expr,
    ident,
    join,
    literal,
    number,
    qname,
    sql,
)


def _name(text: str) -> Sql:
    """One member or table name, which the loader has already checked is an identifier."""
    return ident(Identifier.parse(text))


def _names(values: tuple[str, ...]) -> Sql:
    return join(", ", (_name(value) for value in values))


def _qualified(table: str | None, name: str) -> Sql:
    """`TABLE.NAME`, or the bare name when there is no table: how the DDL names a member."""
    if table is None:
        return _name(name)
    return sql("{table}.{name}", table=_name(table), name=_name(name))


def _quoted_list(values: tuple[str, ...]) -> Sql:
    return join(", ", (literal(v) for v in values))


def _synonyms(values: tuple[str, ...]) -> Sql:
    return sql(" WITH SYNONYMS ({values})", values=_quoted_list(values)) if values else sql("")


def _comment(text: str | None) -> Sql:
    return sql(" COMMENT = {text}", text=literal(text)) if text else sql("")


def render_table(table: Table) -> Sql:
    """Render one TABLES entry.

    Modifier order is fixed, and an absent part renders nothing:
        <logical_name> AS <fqn> [PRIMARY KEY (...)] [UNIQUE (...)]...
        [CONSTRAINT <logical_name>_DISTINCT_RANGE DISTINCT RANGE BETWEEN <start> AND <end> EXCLUSIVE]
        [WITH SYNONYMS (...)] [COMMENT = '...']

    Raises:
        ValueError: a name is not an identifier, or `fqn` is not a three-part name.

    Example:
        P AS DB.S.P PRIMARY KEY (ID) CONSTRAINT P_DISTINCT_RANGE DISTINCT RANGE BETWEEN A AND B EXCLUSIVE
    """
    parts = [sql("{name} AS {fqn}", name=_name(table.logical_name), fqn=qname(QualifiedName.parse(table.fqn)))]
    if table.primary_key:
        parts.append(sql(" PRIMARY KEY ({columns})", columns=_names(table.primary_key)))
    parts.extend(sql(" UNIQUE ({columns})", columns=_names(unique_key)) for unique_key in table.unique_keys)
    if table.distinct_range:
        start, end = table.distinct_range
        parts.append(
            sql(
                " CONSTRAINT {constraint} DISTINCT RANGE BETWEEN {start} AND {end} EXCLUSIVE",
                constraint=_name(f"{table.logical_name}_DISTINCT_RANGE"),
                start=_name(start),
                end=_name(end),
            )
        )
    parts.append(_synonyms(table.synonyms))
    parts.append(_comment(table.comment))
    return join("", parts)


def render_relationship(rel: Relationship) -> Sql:
    """Render one RELATIONSHIPS entry: `<name> AS <from> (<columns>) REFERENCES <to> (<columns>)`.

    An ASOF relationship prefixes the referenced column at `asof_index` with `ASOF`; a range
    relationship replaces the referenced columns with `BETWEEN <start> AND <end> EXCLUSIVE`.

    Raises:
        ValueError: a name is not an identifier.

    Example:
        ORDER_ITEMS_TO_ORDERS AS ORDER_ITEMS (ORDER_ID, OCCURRED_AT) REFERENCES ORDERS (ORDER_ID, ASOF ORDERED_AT)
    """
    to_columns = [_name(column) for column in rel.to_columns]
    if rel.asof_index is not None:
        to_columns[rel.asof_index] = sql("ASOF {column}", column=to_columns[rel.asof_index])
    if rel.range_bounds is not None:
        start, end = rel.range_bounds
        to_columns = [sql("BETWEEN {start} AND {end} EXCLUSIVE", start=_name(start), end=_name(end))]
    return sql(
        "{name} AS {from_table} ({from_columns}) REFERENCES {to_table} ({to_columns})",
        name=_name(rel.name),
        from_table=_name(rel.from_table),
        from_columns=_names(rel.from_columns),
        to_table=_name(rel.to_table),
        to_columns=join(", ", to_columns),
    )


def render_variable(var: Variable) -> Sql:
    """Render one VARIABLES entry: `<name> <data_type> DEFAULT <default> [COMMENT = '...']`.

    The type and default are already SQL: the loader checked the type and made the default
    a literal.

    Raises:
        ValueError: the name is not an identifier.
    """
    return sql(
        "{name} {data_type} DEFAULT {default}{comment}",
        name=_name(var.name),
        data_type=var.data_type,
        default=var.default,
        comment=_comment(var.comment),
    )


def render_column(col: Column) -> Sql:
    """Render one FACTS or DIMENSIONS entry.

    Modifier order is fixed and observed from the golden:
        <qualified> [LABELS = (FILTER)] AS <expr> [WITH SYNONYMS] [COMMENT] [SAMPLE_VALUES] [IS_ENUM]

    `LABELS = (FILTER)` sits before `AS`, not after -- it qualifies the name rather
    than the expression.

    Raises:
        ValueError: the table or column name is not an identifier.
    """
    parts = [_qualified(col.table, col.name)]
    if col.kind is ColumnKind.FILTER:
        parts.append(sql(" LABELS = (FILTER)"))
    parts.append(sql(" AS {expression}", expression=expr(col.expr)))
    parts.append(_synonyms(col.synonyms))
    parts.append(_comment(col.comment))
    if col.sample_values:
        parts.append(sql(" SAMPLE_VALUES ({values})", values=_quoted_list(col.sample_values)))
    if col.is_enum:
        parts.append(sql(" IS_ENUM"))
    return join("", parts)


def render_sort_key(key: SortKey) -> Sql:
    """`<expr> [ASC | DESC] [NULLS FIRST | NULLS LAST]`, with only what was authored."""
    parts = [expr(key.expr)]
    if key.descending is not None:
        parts.append(sql(" DESC") if key.descending else sql(" ASC"))
    if key.nulls_first is not None:
        parts.append(sql(" NULLS FIRST") if key.nulls_first else sql(" NULLS LAST"))
    return join("", parts)


def render_window(window: Window) -> Sql:
    """`OVER ([PARTITION BY [EXCLUDING] ...] [ORDER BY ...] [<frame>])`."""
    parts: list[Sql] = []
    if window.partition_excluding:
        parts.append(
            sql("PARTITION BY EXCLUDING {keys}", keys=join(", ", (expr(key) for key in window.partition_excluding)))
        )
    elif window.partition_by:
        parts.append(sql("PARTITION BY {keys}", keys=join(", ", (expr(key) for key in window.partition_by))))
    if window.order_by:
        parts.append(sql("ORDER BY {keys}", keys=join(", ", (render_sort_key(key) for key in window.order_by))))
    if window.frame:
        parts.append(expr(window.frame))
    return sql("OVER ({clauses})", clauses=join(" ", parts))


def render_metric(metric: Metric) -> Sql:
    """Render one METRICS entry.

    Modifier order is fixed, and an absent part renders nothing:
        [PRIVATE] <qualified> [USING (...)] [NON ADDITIVE BY (...)] AS <expr> [WITH SYNONYMS] [COMMENT]

    A window function metric renders `AS <expr> OVER (...)` instead, and never USING or NON
    ADDITIVE BY. PRIVATE marks a metric whose access modifier is `private_access`.

    Raises:
        ValueError: a name is not an identifier.

    Example:
        SUPPLIES.TOTAL_SUPPLY_COST NON ADDITIVE BY (SNAPSHOT_MONTH) AS SUM(SUPPLIES.SUPPLY_COST)
    """
    parts = [sql("PRIVATE ") if metric.access_modifier == "private_access" else sql("")]
    parts.append(_qualified(metric.table, metric.name))
    if metric.window is not None:
        # Snowflake's grammar for a window function metric has no USING or
        # NON ADDITIVE BY; the loader refuses a metric that sets either.
        parts.append(
            sql(" AS {expression} {window}", expression=expr(metric.expr), window=render_window(metric.window))
        )
        return join("", (*parts, _synonyms(metric.synonyms), _comment(metric.comment)))
    if metric.using_relationships:
        parts.append(sql(" USING ({relationships})", relationships=_names(metric.using_relationships)))
    if metric.non_additive_by:
        parts.append(
            sql(" NON ADDITIVE BY ({keys})", keys=join(", ", (render_sort_key(key) for key in metric.non_additive_by)))
        )
    parts.append(sql(" AS {expression}", expression=expr(metric.expr)))
    parts.append(_synonyms(metric.synonyms))
    parts.append(_comment(metric.comment))
    return join("", parts)


def render_verified_query(vq: VerifiedQuery) -> Sql:
    """Render one AI_VERIFIED_QUERIES entry, which is itself a parenthesised block.

    Optional parts are omitted entirely when absent rather than rendered empty --
    the golden's TOTAL_REVENUE_ALL_TIME carries only QUESTION and SQL. The SQL is the query
    as authored, inside a string literal.

    Raises:
        ValueError: the name is not an identifier.
    """
    lines = [
        sql("    {name} AS (", name=_name(vq.name)),
        sql("      QUESTION {question}", question=literal(vq.question)),
    ]
    if vq.verified_at is not None:
        lines.append(sql("      VERIFIED_AT {at}", at=number(vq.verified_at)))
    if vq.onboarding_question is not None:
        lines.append(sql("      ONBOARDING_QUESTION {value}", value=boolean(vq.onboarding_question)))
    if vq.verified_by is not None:
        lines.append(sql("      VERIFIED_BY {by}", by=literal(vq.verified_by)))
    lines.append(sql("      SQL {query}", query=literal(vq.sql.text)))
    lines.append(sql("    )"))
    return join("\n", lines)


def _block(head: Sql, members: list[Sql]) -> list[Sql]:
    """A parenthesised clause: head, comma-separated members one per line, close."""
    if not members:
        return []
    body = join(",\n", (sql("    {member}", member=member) for member in members))
    return [sql("  {head} (", head=head), body, sql("  )")]


def render(view: SemanticView) -> Sql:
    """Render a complete `CREATE SEMANTIC VIEW` statement.

    A pure function of `view`: same input, same bytes, every time. No trailing
    newline -- the caller decides how the statement is terminated.

    Raises:
        ValueError: the view's or a table's name is not a three-part name, or a member name is
            not an identifier.
    """
    head = [sql("CREATE")]
    if view.or_replace:
        head.append(sql(" OR REPLACE"))
    head.append(sql(" SEMANTIC VIEW"))
    if view.if_not_exists:
        head.append(sql(" IF NOT EXISTS"))

    lines: list[Sql] = [sql("{head} {fqn}", head=join("", head), fqn=qname(QualifiedName.parse(view.fqn)))]

    scope = view.scope
    relationships = sorted((r for r in view.relationships if scope.admits_relationship(r.name)), key=lambda r: r.name)
    lines += _block(sql("TABLES"), [render_table(t) for t in view.tables])
    lines += _block(sql("RELATIONSHIPS"), [render_relationship(r) for r in relationships])
    lines += _block(sql("VARIABLES"), [render_variable(v) for v in view.variables])
    lines += _block(sql("FACTS"), [render_column(c) for c in _sorted_columns(view.facts, scope)])
    lines += _block(sql("DIMENSIONS"), [render_column(c) for c in _sorted_columns(view.dimensions, scope)])
    lines += _block(sql("METRICS"), [render_metric(m) for m in _sorted_metrics(view.metrics, scope)])

    comment = view.comment
    if view.ownership_marker:
        comment = f"{comment.rstrip()} {view.ownership_marker}" if comment else view.ownership_marker
    if comment:
        lines.append(sql("  COMMENT = {comment}", comment=literal(comment)))
    if view.ai_sql_generation:
        lines.append(sql("  AI_SQL_GENERATION {text}", text=literal(view.ai_sql_generation)))
    if view.ai_question_categorization:
        lines.append(sql("  AI_QUESTION_CATEGORIZATION {text}", text=literal(view.ai_question_categorization)))

    if view.verified_queries:
        lines.append(sql("  AI_VERIFIED_QUERIES ("))
        lines.append(join(",\n", (render_verified_query(q) for q in view.verified_queries)))
        lines.append(sql("  )"))

    if view.max_staleness:
        lines.append(sql("  MAX_STALENESS = {staleness}", staleness=literal(view.max_staleness)))

    if view.tags:
        # Leading-comma layout, matching the golden. The indent is deliberately
        # deeper than a member's here; that is what was verified.
        tag_lines = (
            sql("{name} = {value}", name=qname(QualifiedName.parse(t.name)), value=literal(t.value)) for t in view.tags
        )
        lines.append(sql("  WITH TAG ("))
        lines.append(sql("      {tags}", tags=join("\n    , ", tag_lines)))
        lines.append(sql("  )"))

    lines.append(sql("  COPY GRANTS"))

    return join("\n", lines)


def _sorted_columns(columns: tuple[Column, ...], scope: ViewScope) -> list[Column]:
    return sorted((c for c in columns if scope.admits_column(c)), key=lambda c: c.qualified_name)


def _sorted_metrics(metrics: tuple[Metric, ...], scope: ViewScope) -> list[Metric]:
    return sorted(
        (m for m in metrics if scope.admits_metric(m.name)), key=lambda m: (m.table is None, m.qualified_name)
    )
