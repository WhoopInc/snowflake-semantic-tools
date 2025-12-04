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
    operator: str  # =, >=, <=, BETWEEN, etc.
    match_condition: Optional[str] = None  # For ASOF: the SQL MATCH CONDITION clause


class JoinConditionParser:
    """
    Format-agnostic parser for join conditions.

    Automatically detects and parses both:
    - Template format: {{ column('table', 'col') }} = {{ column('table2', 'col2') }}
    - Resolved format: TABLE.COL = TABLE2.COL2
    """

    # Pattern to extract {{ column('table', 'column') }} templates
    COLUMN_TEMPLATE_PATTERN = re.compile(r"{{\s*column\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")

    # Pattern to extract {{ table('name') }} templates
    TABLE_TEMPLATE_PATTERN = re.compile(r"{{\s*table\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}")

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

        # Generate match condition for ASOF joins
        match_condition = None
        if condition_type == JoinType.ASOF:
            # Use raw column names (without table prefix) in MATCH CONDITION
            match_condition = f"{left_column} {operator} {right_column}"

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
            match_condition=match_condition,
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
        """Detect join type based on operator."""
        if operator == "=":
            return JoinType.EQUALITY
        elif operator in [">=", "<=", ">", "<"]:
            return JoinType.ASOF
        elif operator == "BETWEEN":
            return JoinType.RANGE
        else:
            return JoinType.UNKNOWN

    @classmethod
    def _split_on_operator(cls, condition: str, operator: str) -> Tuple[str, str]:
        """Split condition on operator, handling special cases."""
        if operator == "BETWEEN":
            # Handle BETWEEN x AND y
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
    def _extract_table_column_from_template(cls, expression: str) -> Tuple[str, str]:
        """
        Extract table and column from template format.

        Example: "{{ column('orders', 'customer_id') }}" → ('orders', 'customer_id')
        """
        match = cls.COLUMN_TEMPLATE_PATTERN.search(expression)
        if match:
            return match.group(1), match.group(2)
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
                if parsed.operator not in [">=", "<=", ">", "<"]:
                    return False, f"ASOF joins require >=, <=, >, or < operators, got: {parsed.operator}"

            return True, ""

        except Exception as e:
            return False, f"Error parsing condition: {str(e)}"

    @classmethod
    def generate_sql_references(
        cls, parsed_conditions: List[ParsedCondition], left_table_alias: str, right_table_alias: str
    ) -> str:
        """
        Generate SQL REFERENCES clause from parsed conditions.

        Args:
            parsed_conditions: List of parsed conditions
            left_table_alias: Alias for left table
            right_table_alias: Alias for right table

        Returns:
            SQL REFERENCES clause
        """
        if not parsed_conditions:
            return ""

        # Check if any condition is ASOF type
        has_asof = any(c.condition_type == JoinType.ASOF for c in parsed_conditions)

        # Extract column lists
        left_cols = [c.left_column for c in parsed_conditions]
        right_cols = [c.right_column for c in parsed_conditions]

        # Build basic REFERENCES clause
        sql = f"{left_table_alias} ({', '.join(left_cols)}) REFERENCES {right_table_alias} ({', '.join(right_cols)})"

        # Add MATCH CONDITION for ASOF joins
        if has_asof:
            match_conditions = []
            for c in parsed_conditions:
                if c.condition_type == JoinType.ASOF:
                    # Use the match_condition which has raw column names
                    match_conditions.append(c.match_condition)

            if match_conditions:
                sql += f"\n      MATCH CONDITION ({' AND '.join(match_conditions)})"

        return sql
