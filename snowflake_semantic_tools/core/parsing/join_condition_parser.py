"""
Parser for relationship join conditions - supports both template and resolved formats.

Handles two formats:
1. Template format (before resolution):
   - {{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}
   - {{ column('events', 'timestamp') }} >= {{ column('sessions', 'start_time') }}

2. Resolved format (after template resolution):
   - ORDERS.CUSTOMER_ID = CUSTOMERS.ID
   - EVENTS.TIMESTAMP >= SESSIONS.START_TIME

The parser automatically detects which format is used and parses accordingly,
enabling it to work at any stage of the processing pipeline.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple


class JoinType(Enum):
    """Types of join conditions."""

    EQUALITY = "equality"
    ASOF = "asof"
    RANGE = "range"
    UNKNOWN = "unknown"


@dataclass
class ParsedCondition:
    """Represents a parsed join condition."""

    join_condition: str  # Original condition
    condition_type: JoinType
    left_expression: str  # Full left template expression
    right_expression: str  # Full right template expression
    left_table: str  # Extracted table name from left
    left_column: str  # Extracted column name from left
    right_table: str  # Extracted table name from right
    right_column: str  # Extracted column name from right
    operator: str  # = (equality), >= (ASOF), or BETWEEN (range)
    right_column_end: str = ""  # End column for BETWEEN...AND...EXCLUSIVE range joins


class JoinConditionParser:
    """
    Format-agnostic parser for join conditions.

    Automatically detects and parses both:
    - Template format: {{ column('table', 'col') }} = {{ column('table2', 'col2') }}
    - Resolved format: TABLE.COL = TABLE2.COL2
    """

    # Pattern to extract {{ column('table', 'column') }} templates (legacy)
    COLUMN_TEMPLATE_PATTERN = re.compile(r"{{\s*column\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")

    # Pattern to extract {{ table('name') }} templates (legacy)
    TABLE_TEMPLATE_PATTERN = re.compile(r"{{\s*table\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")

    # Pattern to extract {{ ref('table', 'column') }} templates (unified syntax)
    REF_TEMPLATE_PATTERN = re.compile(r"{{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")

    # Pattern to extract {{ ref('table') }} templates (unified syntax for tables)
    REF_TABLE_PATTERN = re.compile(r"{{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")

    # Pattern to extract resolved SQL format: TABLE.COLUMN
    RESOLVED_COLUMN_PATTERN = re.compile(r"([A-Z_][A-Z0-9_]*)\.([A-Z_][A-Z0-9_]*)", re.IGNORECASE)

    @classmethod
    def parse(cls, condition: str) -> ParsedCondition:
        """
        Parse a join condition into its components.

        Automatically detects format (template vs resolved) and parses accordingly.

        Args:
            condition: Join condition in either format:
                      - Template: "{{ column('table', 'col') }} = {{ column('table2', 'col2') }}"
                      - Resolved: "TABLE.COL = TABLE2.COL2"

        Returns:
            ParsedCondition with all extracted components
        """
        # Detect operator and type
        operator = cls._detect_operator(condition)
        condition_type = cls._detect_join_type(operator)

        # Split condition on operator
        left_expr, right_expr = cls._split_on_operator(condition, operator)

        # Detect format and extract table/column accordingly
        is_template_format = "{{" in condition

        if is_template_format:
            # Template format: {{ column('table', 'col') }}
            left_table, left_column = cls._extract_table_column_from_template(left_expr)
            right_table, right_column = cls._extract_table_column_from_template(right_expr)
        else:
            # Resolved format: TABLE.COLUMN
            left_table, left_column = cls._extract_table_column_from_resolved(left_expr)
            right_table, right_column = cls._extract_table_column_from_resolved(right_expr)

        # For BETWEEN range joins, extract the end column
        right_column_end = ""
        if operator == "BETWEEN":
            end_expr, _ = cls._extract_range_end(condition)
            if is_template_format:
                _, right_column_end = cls._extract_table_column_from_template(end_expr)
            else:
                _, right_column_end = cls._extract_table_column_from_resolved(end_expr)

        return ParsedCondition(
            join_condition=condition,
            condition_type=condition_type,
            left_expression=left_expr.strip(),
            right_expression=right_expr.strip(),
            left_table=left_table,
            left_column=left_column,
            right_table=right_table,
            right_column=right_column,
            operator=operator,
            right_column_end=right_column_end,
        )

    @classmethod
    def parse_multiple(cls, conditions: List[str]) -> List[ParsedCondition]:
        """Parse multiple join conditions."""
        return [cls.parse(cond) for cond in conditions]

    @classmethod
    def _detect_operator(cls, condition: str) -> str:
        """Detect the operator in the condition."""
        # Check for BETWEEN first (special case)
        if "BETWEEN" in condition.upper():
            return "BETWEEN"

        # Check for comparison operators
        for op in [">=", "<=", "!=", "<>", "=", ">", "<"]:
            if op in condition:
                return op

        return "UNKNOWN"

    @classmethod
    def _detect_join_type(cls, operator: str) -> JoinType:
        """Detect join type based on operator.

        Note: Only = and >= operators are valid in Snowflake semantic views.
        - = : Equality join (standard FK relationship)
        - >= : ASOF join (temporal relationship)
        - BETWEEN, <=, >, < : Not supported, will be rejected in validation
        """
        if operator == "=":
            return JoinType.EQUALITY
        elif operator == ">=":
            return JoinType.ASOF
        elif operator == "BETWEEN":
            return JoinType.RANGE
        else:
            return JoinType.UNKNOWN

    @classmethod
    def _split_on_operator(cls, condition: str, operator: str) -> Tuple[str, str]:
        """Split condition on operator, handling special cases."""
        if operator == "BETWEEN":
            # Handle BETWEEN x AND y [EXCLUSIVE]
            parts = re.split(r"\s+BETWEEN\s+|\s+AND\s+", condition, flags=re.IGNORECASE)
            if len(parts) >= 2:
                return parts[0], parts[1]  # Return first two parts
        else:
            # Split on operator
            parts = condition.split(operator, 1)
            if len(parts) == 2:
                return parts[0], parts[1]

        # Fallback
        return condition, ""

    @classmethod
    def _extract_range_end(cls, condition: str) -> Tuple[str, str]:
        """Extract the end column from a BETWEEN...AND...EXCLUSIVE condition."""
        parts = re.split(r"\s+BETWEEN\s+|\s+AND\s+", condition, flags=re.IGNORECASE)
        if len(parts) >= 3:
            end_expr = re.sub(r"\s+EXCLUSIVE\s*$", "", parts[2], flags=re.IGNORECASE).strip()
            return end_expr, end_expr
        return "", ""

    @classmethod
    def _extract_table_column_from_template(cls, expression: str) -> Tuple[str, str]:
        """
        Extract table and column from template format.

        Supports both legacy and unified syntax:
        - {{ column('orders', 'customer_id') }} → ('ORDERS', 'CUSTOMER_ID')
        - {{ ref('orders', 'customer_id') }} → ('ORDERS', 'CUSTOMER_ID')

        Note: Uppercases table and column names to match Snowflake's identifier behavior.
        """
        # Try unified ref() syntax first
        match = cls.REF_TEMPLATE_PATTERN.search(expression)
        if match:
            return match.group(1).upper(), match.group(2).upper()

        # Fall back to legacy column() syntax
        match = cls.COLUMN_TEMPLATE_PATTERN.search(expression)
        if match:
            return match.group(1).upper(), match.group(2).upper()

        return "", ""

    @classmethod
    def _extract_table_column_from_resolved(cls, expression: str) -> Tuple[str, str]:
        """
        Extract table and column from resolved SQL format.

        Example: "ORDERS.CUSTOMER_ID" → ('ORDERS', 'CUSTOMER_ID')
        """
        # Strip quotes and whitespace
        expression = expression.strip().strip('"').strip("'")

        # Match TABLE.COLUMN pattern
        match = cls.RESOLVED_COLUMN_PATTERN.search(expression)
        if match:
            return match.group(1).upper(), match.group(2).upper()
        return "", ""

    @classmethod
    def validate_condition(cls, condition: str) -> Tuple[bool, str]:
        """
        Validate a join condition (works with both template and resolved formats).

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            parsed = cls.parse(condition)

            # Check for unknown operators
            if parsed.operator == "UNKNOWN":
                return False, f"Unknown or unsupported operator in condition: {condition}"

            # Reject unsupported temporal operators FIRST (before checking join type)
            if parsed.operator in ["<=", ">", "<"]:
                return False, (
                    f"Operator '{parsed.operator}' is not supported for temporal relationships in Snowflake semantic views. "
                    f"Only '>=' operator is supported for ASOF joins. "
                    f"See: https://docs.snowflake.com/en/user-guide/views-semantic/sql"
                )

            # BETWEEN operator is valid for range joins (preview feature)
            if parsed.operator == "BETWEEN":
                if not parsed.right_column_end:
                    return False, (f"BETWEEN condition must include AND <end_column> EXCLUSIVE: {condition}")
                if not parsed.left_table or not parsed.left_column:
                    return False, f"Could not extract left table/column from: {parsed.left_expression}"
                if not parsed.right_table or not parsed.right_column:
                    return False, f"Could not extract right table/column from: {parsed.right_expression}"
                return True, ""

            # Check for unknown join type
            if parsed.condition_type == JoinType.UNKNOWN:
                return False, f"Unknown join type for operator '{parsed.operator}'"

            # Check that we extracted tables and columns
            if not parsed.left_table or not parsed.left_column:
                return False, f"Could not extract left table/column from: {parsed.left_expression}"

            if not parsed.right_table or not parsed.right_column:
                return False, f"Could not extract right table/column from: {parsed.right_expression}"

            # Specific validations for ASOF joins
            if parsed.condition_type == JoinType.ASOF:
                if parsed.operator != ">=":
                    return False, f"ASOF joins require >= operator, got: {parsed.operator}"

            return True, ""

        except Exception as e:
            return False, f"Error parsing condition: {str(e)}"

    @classmethod
    def generate_sql_references(
        cls, parsed_conditions: List[ParsedCondition], left_table_alias: str, right_table_alias: str
    ) -> str:
        """
        Generate SQL REFERENCES clause from parsed conditions.

        For semantic views:
        - Equality: table(col1, col2) REFERENCES table(col1, col2)
        - ASOF: table(col1, time_col) REFERENCES table(col1, ASOF time_col)
        - Mixed: table(join_col, time_col) REFERENCES table(join_col, ASOF time_col)

        Args:
            parsed_conditions: List of parsed conditions
            left_table_alias: Alias for left table
            right_table_alias: Alias for right table

        Returns:
            SQL REFERENCES clause with correct ASOF syntax
        """
        if not parsed_conditions:
            return ""

        # Build left column list (all conditions in original order)
        left_cols = [c.left_column for c in parsed_conditions]

        # Check if any condition is a range join
        has_range = any(c.condition_type == JoinType.RANGE for c in parsed_conditions)

        if has_range:
            non_range = [c for c in parsed_conditions if c.condition_type != JoinType.RANGE]
            if non_range:
                from snowflake_semantic_tools.shared import get_logger

                _logger = get_logger("core.parsing.join_condition_parser")
                _logger.warning(
                    f"Range join has {len(non_range)} non-range condition(s) that will be ignored. "
                    f"Snowflake range joins only support a single BETWEEN condition per relationship."
                )
            range_cond = next(c for c in parsed_conditions if c.condition_type == JoinType.RANGE)
            sql = (
                f"{left_table_alias} ({range_cond.left_column}) REFERENCES "
                f"{right_table_alias} (BETWEEN {range_cond.right_column} AND {range_cond.right_column_end} EXCLUSIVE)"
            )
            return sql

        # Build right column list maintaining same order as left
        # ASOF columns get the ASOF prefix, equality columns are unchanged
        right_cols = []
        for c in parsed_conditions:
            if c.condition_type == JoinType.ASOF:
                right_cols.append(f"ASOF {c.right_column}")
            else:
                right_cols.append(c.right_column)

        # Generate SQL
        sql = f"{left_table_alias} ({', '.join(left_cols)}) REFERENCES {right_table_alias} ({', '.join(right_cols)})"

        return sql
