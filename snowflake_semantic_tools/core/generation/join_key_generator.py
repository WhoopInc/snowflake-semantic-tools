"""
Join Key Dimension Generator

Auto-generates synthetic dimension definitions for SQL expressions used in
relationship join conditions. When a user writes an expression like
DATE({{ column('events', 'event_timestamp') }}) in a relationship condition,
this generator creates a corresponding dimension that can be referenced by the
RELATIONSHIPS clause of a semantic view.

The detection is GENERIC — any SQL expression wrapping a single column template
is supported. SST does not whitelist specific functions; instead, it detects that
the join condition side contains more than a bare column reference. Snowflake
validates the SQL expression at semantic view creation time.

See: https://github.com/WhoopInc/snowflake-semantic-tools/issues/106
"""

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

from snowflake_semantic_tools.shared import get_logger

logger = get_logger("core.generation.join_key_generator")

TEMPLATE_PATTERN = re.compile(r"{{\s*(?:column|ref)\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")

RESOLVED_COL_PATTERN = re.compile(r"([A-Z_][A-Z0-9_]*)\.([A-Z_][A-Z0-9_]*)", re.IGNORECASE)


def has_wrapping_text(side_expr: str) -> bool:
    """Check if a join condition side has text wrapping a column reference.

    Returns True if the side contains a column template or TABLE.COL but also has
    additional text around it (i.e., it's not just a bare column reference).
    """
    side_expr = side_expr.strip()
    if not side_expr:
        return False

    tmpl_matches = list(TEMPLATE_PATTERN.finditer(side_expr))
    if tmpl_matches:
        if len(tmpl_matches) == 1 and side_expr == tmpl_matches[0].group(0).strip():
            return False
        return True

    resolved_matches = list(RESOLVED_COL_PATTERN.finditer(side_expr))
    if resolved_matches:
        if len(resolved_matches) == 1 and side_expr == resolved_matches[0].group(0).strip():
            return False
        return True

    return False


def detect_expression(side_expr: str) -> Optional[Dict[str, str]]:
    """Detect if a join condition side contains a SQL expression wrapping a column reference.

    Uses a GENERIC approach: if the side string is anything other than a bare
    column template (or bare TABLE.COL), it's treated as an expression. This
    supports any valid Snowflake SQL expression (DATE, DATE_TRUNC, UPPER,
    COALESCE, CAST, ::, arithmetic, etc.).

    Supports both:
    - Template format: DATE({{ column('events', 'ts') }})
    - Resolved format: DATE(EVENTS.TS)

    Args:
        side_expr: One side of a join condition

    Returns:
        Dict with keys {table, column, sql_expression} if an expression is detected, else None.
        sql_expression contains the SQL form with only the column name
        (e.g., "DATE(EVENT_TIMESTAMP)").
    """
    side_expr = side_expr.strip()
    if not side_expr:
        return None

    result = _detect_template_expression(side_expr)
    if result:
        return result

    return _detect_resolved_expression(side_expr)


def _detect_template_expression(side_expr: str) -> Optional[Dict[str, str]]:
    """Detect expression in template format.

    If the side contains exactly one {{ column/ref(...) }} template AND has
    additional text around it, it's an expression. If the side IS just the
    template with no surrounding text, it's a bare column reference (not an expression).
    """
    matches = list(TEMPLATE_PATTERN.finditer(side_expr))
    if len(matches) != 1:
        return None

    m = matches[0]
    table = m.group(1).upper()
    column = m.group(2).upper()

    bare_template = m.group(0).strip()
    if side_expr.strip() == bare_template:
        return None

    sql_expr = side_expr.replace(m.group(0), column)
    sql_expr = _normalize_sql_expression(sql_expr)

    return {"table": table, "column": column, "sql_expression": sql_expr}


def _detect_resolved_expression(side_expr: str) -> Optional[Dict[str, str]]:
    """Detect expression in resolved format (post-template resolution).

    If the side contains exactly one TABLE.COLUMN reference AND has additional
    text around it, it's an expression. A bare TABLE.COLUMN is not an expression.
    """
    matches = list(RESOLVED_COL_PATTERN.finditer(side_expr))
    if len(matches) != 1:
        return None

    m = matches[0]
    table = m.group(1).upper()
    column = m.group(2).upper()

    if side_expr.strip() == m.group(0).strip():
        return None

    sql_expr = side_expr.replace(m.group(0), column)
    sql_expr = _normalize_sql_expression(sql_expr)

    return {"table": table, "column": column, "sql_expression": sql_expr}


def _normalize_sql_expression(sql_expr: str) -> str:
    """Normalize a SQL expression: strip outer whitespace and collapse internal whitespace."""
    sql_expr = sql_expr.strip()
    sql_expr = re.sub(r"\s+", " ", sql_expr)
    return sql_expr


class JoinKeyDimensionGenerator:
    """Generates synthetic dimensions for expression-based join keys."""

    def __init__(self) -> None:
        self._generated: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def register_join_key(self, table: str, column: str, sql_expression: str) -> str:
        """Register a join key expression and return the generated dimension name.

        Deduplicates: if the same (table, expression) pair is registered twice, the
        same dimension name is returned.

        Args:
            table: Source table name (uppercased).
            column: Base column name from the template (uppercased).
            sql_expression: The SQL expression for the dimension (e.g., "DATE(EVENT_TIMESTAMP)").

        Returns:
            Generated dimension name (e.g., "_JK_EVENT_TIMESTAMP_A1B2").
        """
        key = (table.upper(), sql_expression.upper())
        if key not in self._generated:
            dim_name = self._generate_name(column, sql_expression)
            self._generated[key] = {
                "name": dim_name,
                "table": table.upper(),
                "expression": sql_expression,
                "base_column": column.upper(),
                "comment": f"Auto-generated join key: {sql_expression}",
            }
            logger.info(f"Registered join key dimension: {table.upper()}.{dim_name} AS {sql_expression}")
        return self._generated[key]["name"]

    def get_dimensions_for_table(self, table: str) -> List[Dict[str, Any]]:
        """Return all generated dimensions for a specific table."""
        return [d for d in self._generated.values() if d["table"] == table.upper()]

    def get_all_dimensions(self) -> List[Dict[str, Any]]:
        """Return all generated dimensions across all tables."""
        return list(self._generated.values())

    def has_dimensions(self) -> bool:
        """Return True if any join key dimensions have been generated."""
        return bool(self._generated)

    def clear(self) -> None:
        """Clear all generated dimensions (call between semantic views)."""
        self._generated.clear()

    @staticmethod
    def _generate_name(column: str, sql_expression: str) -> str:
        """Generate a deterministic, unique dimension name."""
        hash_suffix = hashlib.md5(sql_expression.upper().encode()).hexdigest()[:4].upper()
        return f"_JK_{column.upper()}_{hash_suffix}"
