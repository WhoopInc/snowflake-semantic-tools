"""Pure `render(view) -> str` for `CREATE SEMANTIC VIEW`.

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

ORDERING. `TABLES` renders in DECLARATION order, because that is the order the
author wrote and the golden preserves it. Every other member list renders SORTED
by its qualified name. The difference is not an inconsistency: table order is
authored information, member order is not, and sorting members is what keeps a
re-render byte-identical when an unrelated column is added.
"""

from __future__ import annotations

from ..model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    SortKey,
    Table,
    Variable,
    VerifiedQuery,
    Window,
)
from ..model.sql import string_literal

CLAUSE_INDENT = "  "
MEMBER_INDENT = "    "


def _quoted_list(values: tuple[str, ...]) -> str:
    return ", ".join(string_literal(v) for v in values)


def _synonyms(values: tuple[str, ...]) -> str:
    return f" WITH SYNONYMS ({_quoted_list(values)})" if values else ""


def _comment(text: str | None) -> str:
    return f" COMMENT = {string_literal(text)}" if text else ""


def render_table(table: Table) -> str:
    """Render one TABLES entry.

    Modifier order is fixed, and an absent part renders nothing:
        <logical_name> AS <fqn> [PRIMARY KEY (...)] [UNIQUE (...)]...
        [CONSTRAINT <logical_name>_DISTINCT_RANGE DISTINCT RANGE BETWEEN <start> AND <end> EXCLUSIVE]
        [WITH SYNONYMS (...)] [COMMENT = '...']

    Example:
        P AS DB.S.P PRIMARY KEY (ID) CONSTRAINT P_DISTINCT_RANGE DISTINCT RANGE BETWEEN A AND B EXCLUSIVE
    """
    out = f"{table.logical_name} AS {table.fqn}"
    if table.primary_key:
        out += f" PRIMARY KEY ({', '.join(table.primary_key)})"
    for unique_key in table.unique_keys:
        out += f" UNIQUE ({', '.join(unique_key)})"
    if table.distinct_range:
        start, end = table.distinct_range
        out += f" CONSTRAINT {table.logical_name}_DISTINCT_RANGE DISTINCT RANGE " f"BETWEEN {start} AND {end} EXCLUSIVE"
    out += _synonyms(table.synonyms)
    out += _comment(table.comment)
    return out


def render_relationship(rel: Relationship) -> str:
    """Render one RELATIONSHIPS entry: `<name> AS <from> (<columns>) REFERENCES <to> (<columns>)`.

    An ASOF relationship prefixes the referenced column at `asof_index` with `ASOF`; a range
    relationship replaces the referenced columns with `BETWEEN <start> AND <end> EXCLUSIVE`.

    Example:
        ORDER_ITEMS_TO_ORDERS AS ORDER_ITEMS (ORDER_ID, OCCURRED_AT) REFERENCES ORDERS (ORDER_ID, ASOF ORDERED_AT)
    """
    to_columns = list(rel.to_columns)
    if rel.asof_index is not None:
        to_columns[rel.asof_index] = f"ASOF {to_columns[rel.asof_index]}"
    if rel.range_bounds is not None:
        start, end = rel.range_bounds
        to_columns = [f"BETWEEN {start} AND {end} EXCLUSIVE"]
    return (
        f"{rel.name} AS {rel.from_table} ({', '.join(rel.from_columns)}) "
        f"REFERENCES {rel.to_table} ({', '.join(to_columns)})"
    )


def render_variable(var: Variable) -> str:
    """Render one VARIABLES entry: `<name> <data_type> DEFAULT <default> [COMMENT = '...']`.

    The default renders verbatim: the loader has already made it a SQL literal.
    """
    return f"{var.name} {var.data_type} DEFAULT {var.default}{_comment(var.comment)}"


def render_column(col: Column) -> str:
    """Render one FACTS or DIMENSIONS entry.

    Modifier order is fixed and observed from the golden:
        <qualified> [LABELS = (FILTER)] AS <expr> [WITH SYNONYMS] [COMMENT] [SAMPLE_VALUES] [IS_ENUM]

    `LABELS = (FILTER)` sits before `AS`, not after -- it qualifies the name rather
    than the expression.
    """
    out = col.qualified_name
    if col.kind is ColumnKind.FILTER:
        out += " LABELS = (FILTER)"
    out += f" AS {col.expr}"
    out += _synonyms(col.synonyms)
    out += _comment(col.comment)
    if col.sample_values:
        out += f" SAMPLE_VALUES ({_quoted_list(col.sample_values)})"
    if col.is_enum:
        out += " IS_ENUM"
    return out


def render_sort_key(key: SortKey) -> str:
    """`<expr> [ASC | DESC] [NULLS FIRST | NULLS LAST]`, with only what was authored."""
    out = key.expr
    if key.descending is not None:
        out += " DESC" if key.descending else " ASC"
    if key.nulls_first is not None:
        out += " NULLS FIRST" if key.nulls_first else " NULLS LAST"
    return out


def render_window(window: Window) -> str:
    """`OVER ([PARTITION BY [EXCLUDING] ...] [ORDER BY ...] [<frame>])`."""
    parts: list[str] = []
    if window.partition_excluding:
        parts.append(f"PARTITION BY EXCLUDING {', '.join(window.partition_excluding)}")
    elif window.partition_by:
        parts.append(f"PARTITION BY {', '.join(window.partition_by)}")
    if window.order_by:
        parts.append(f"ORDER BY {', '.join(render_sort_key(key) for key in window.order_by)}")
    if window.frame:
        parts.append(window.frame)
    return f"OVER ({' '.join(parts)})"


def render_metric(metric: Metric) -> str:
    """Render one METRICS entry.

    Modifier order is fixed, and an absent part renders nothing:
        [PRIVATE] <qualified> [USING (...)] [NON ADDITIVE BY (...)] AS <expr> [WITH SYNONYMS] [COMMENT]

    A window function metric renders `AS <expr> OVER (...)` instead, and never USING or NON
    ADDITIVE BY. PRIVATE marks a metric whose access modifier is `private_access`.

    Example:
        SUPPLIES.TOTAL_SUPPLY_COST NON ADDITIVE BY (SNAPSHOT_MONTH) AS SUM(SUPPLIES.SUPPLY_COST)
    """
    prefix = "PRIVATE " if metric.access_modifier == "private_access" else ""
    out = prefix + metric.qualified_name
    if metric.window is not None:
        # Snowflake's grammar for a window function metric has no USING or
        # NON ADDITIVE BY; the loader refuses a metric that sets either.
        out += f" AS {metric.expr} {render_window(metric.window)}"
        return out + _synonyms(metric.synonyms) + _comment(metric.comment)
    if metric.using_relationships:
        out += f" USING ({', '.join(metric.using_relationships)})"
    if metric.non_additive_by:
        out += f" NON ADDITIVE BY ({', '.join(render_sort_key(key) for key in metric.non_additive_by)})"
    out += f" AS {metric.expr}"
    out += _synonyms(metric.synonyms)
    out += _comment(metric.comment)
    return out


def render_verified_query(vq: VerifiedQuery) -> str:
    """Render one AI_VERIFIED_QUERIES entry, which is itself a parenthesised block.

    Optional parts are omitted entirely when absent rather than rendered empty --
    the golden's TOTAL_REVENUE_ALL_TIME carries only QUESTION and SQL.
    """
    lines = [f"{MEMBER_INDENT}{vq.name} AS (", f"{MEMBER_INDENT}  QUESTION {string_literal(vq.question)}"]
    if vq.verified_at is not None:
        lines.append(f"{MEMBER_INDENT}  VERIFIED_AT {vq.verified_at}")
    if vq.onboarding_question is not None:
        lines.append(f"{MEMBER_INDENT}  ONBOARDING_QUESTION {'TRUE' if vq.onboarding_question else 'FALSE'}")
    if vq.verified_by is not None:
        lines.append(f"{MEMBER_INDENT}  VERIFIED_BY {string_literal(vq.verified_by)}")
    lines.append(f"{MEMBER_INDENT}  SQL {string_literal(vq.sql)}")
    lines.append(f"{MEMBER_INDENT})")
    return "\n".join(lines)


def _block(head: str, members: list[str]) -> list[str]:
    """A parenthesised clause: head, comma-separated members one per line, close."""
    if not members:
        return []
    body = ",\n".join(f"{MEMBER_INDENT}{m}" for m in members)
    return [f"{CLAUSE_INDENT}{head} (", body, f"{CLAUSE_INDENT})"]


def render(view: SemanticView) -> str:
    """Render a complete `CREATE SEMANTIC VIEW` statement.

    A pure function of `view`: same input, same bytes, every time. No trailing
    newline -- the caller decides how the statement is terminated.
    """
    head = "CREATE"
    if view.or_replace:
        head += " OR REPLACE"
    head += " SEMANTIC VIEW"
    if view.if_not_exists:
        head += " IF NOT EXISTS"

    lines: list[str] = [f"{head} {view.fqn}"]

    lines += _block("TABLES", [render_table(t) for t in view.tables])
    lines += _block("RELATIONSHIPS", [render_relationship(r) for r in sorted(view.relationships, key=lambda r: r.name)])
    lines += _block("VARIABLES", [render_variable(v) for v in view.variables])
    lines += _block("FACTS", [render_column(c) for c in _sorted_columns(view.facts)])
    lines += _block("DIMENSIONS", [render_column(c) for c in _sorted_columns(view.dimensions)])
    lines += _block("METRICS", [render_metric(m) for m in _sorted_metrics(view.metrics)])

    comment = view.comment
    if view.ownership_marker:
        comment = f"{comment.rstrip()} {view.ownership_marker}" if comment else view.ownership_marker
    if comment:
        lines.append(f"{CLAUSE_INDENT}COMMENT = {string_literal(comment)}")
    if view.ai_sql_generation:
        lines.append(f"{CLAUSE_INDENT}AI_SQL_GENERATION {string_literal(view.ai_sql_generation)}")
    if view.ai_question_categorization:
        lines.append(f"{CLAUSE_INDENT}AI_QUESTION_CATEGORIZATION {string_literal(view.ai_question_categorization)}")

    if view.verified_queries:
        lines.append(f"{CLAUSE_INDENT}AI_VERIFIED_QUERIES (")
        lines.append(",\n".join(render_verified_query(q) for q in view.verified_queries))
        lines.append(f"{CLAUSE_INDENT})")

    if view.max_staleness:
        lines.append(f"{CLAUSE_INDENT}MAX_STALENESS = {string_literal(view.max_staleness)}")

    if view.tags:
        # Leading-comma layout, matching the golden. The indent is deliberately
        # deeper than MEMBER_INDENT here; that is what was verified.
        tag_lines = [f"{t.name} = {string_literal(t.value)}" for t in view.tags]
        lines.append(f"{CLAUSE_INDENT}WITH TAG (")
        lines.append("      " + "\n    , ".join(tag_lines))
        lines.append(f"{CLAUSE_INDENT})")

    lines.append(f"{CLAUSE_INDENT}COPY GRANTS")

    return "\n".join(lines)


def _sorted_columns(columns: tuple[Column, ...]) -> list[Column]:
    return sorted(columns, key=lambda c: c.qualified_name)


def _sorted_metrics(metrics: tuple[Metric, ...]) -> list[Metric]:
    return sorted(metrics, key=lambda m: (m.table is None, m.qualified_name))
