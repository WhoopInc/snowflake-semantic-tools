"""
Unit tests for JoinConditionParser - ASOF JOIN SQL generation.

Tests for Issue #40: Fix ASOF JOIN support in semantic views.
"""

import pytest

from snowflake_semantic_tools.core.parsing.join_condition_parser import JoinConditionParser, JoinType, ParsedCondition


class TestGenerateSqlReferencesAsof:
    """Test generate_sql_references() for ASOF relationships."""

    def test_asof_single_condition(self):
        """Test ASOF relationship with only time condition."""
        condition = "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
        parsed = JoinConditionParser.parse(condition)

        sql = JoinConditionParser.generate_sql_references([parsed], "ORDERS", "ORDERS")

        expected = "ORDERS (ORDERED_AT) REFERENCES ORDERS (ASOF ORDERED_AT)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"
        assert "MATCH CONDITION" not in sql, "MATCH CONDITION should not appear in output"

    def test_asof_mixed_equality_and_temporal(self):
        """Test ASOF relationship with equality + temporal conditions."""
        conditions = [
            "{{ column('orders', 'customer_id') }} = {{ column('orders', 'customer_id') }}",
            "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}",
        ]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "ORDERS", "ORDERS")

        expected = "ORDERS (CUSTOMER_ID, ORDERED_AT) REFERENCES ORDERS (CUSTOMER_ID, ASOF ORDERED_AT)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"
        assert "MATCH CONDITION" not in sql, "MATCH CONDITION should not appear in output"

    def test_asof_different_tables(self):
        """Test ASOF relationship between different tables."""
        conditions = [
            "{{ column('events', 'user_id') }} = {{ column('sessions', 'user_id') }}",
            "{{ column('events', 'timestamp') }} >= {{ column('sessions', 'start_time') }}",
        ]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "EVENTS", "SESSIONS")

        expected = "EVENTS (USER_ID, TIMESTAMP) REFERENCES SESSIONS (USER_ID, ASOF START_TIME)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"

    def test_asof_multiple_equality_columns(self):
        """Test ASOF with multiple equality columns + one temporal column."""
        conditions = [
            "{{ column('orders', 'customer_id') }} = {{ column('orders', 'customer_id') }}",
            "{{ column('orders', 'product_id') }} = {{ column('orders', 'product_id') }}",
            "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}",
        ]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "ORDERS", "ORDERS")

        expected = (
            "ORDERS (CUSTOMER_ID, PRODUCT_ID, ORDERED_AT) REFERENCES ORDERS (CUSTOMER_ID, PRODUCT_ID, ASOF ORDERED_AT)"
        )
        assert sql == expected, f"Expected: {expected}, Got: {sql}"

    def test_asof_with_greater_than_equal_operator(self):
        """Test ASOF with >= operator (the only supported operator)."""
        condition = "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
        parsed = JoinConditionParser.parse(condition)

        assert parsed.condition_type == JoinType.ASOF

        sql = JoinConditionParser.generate_sql_references([parsed], "ORDERS", "ORDERS")
        expected = "ORDERS (ORDERED_AT) REFERENCES ORDERS (ASOF ORDERED_AT)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"


class TestGenerateSqlReferencesEquality:
    """Test generate_sql_references() for equality-only relationships (regression)."""

    def test_equality_single_condition(self):
        """Test equality relationship with single condition."""
        condition = "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
        parsed = JoinConditionParser.parse(condition)

        sql = JoinConditionParser.generate_sql_references([parsed], "ORDERS", "CUSTOMERS")

        expected = "ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (ID)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"
        assert "ASOF" not in sql, "ASOF should not appear in equality-only relationships"

    def test_equality_multiple_conditions(self):
        """Test equality relationship with multiple conditions."""
        conditions = [
            "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}",
            "{{ column('orders', 'region') }} = {{ column('customers', 'region') }}",
        ]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "ORDERS", "CUSTOMERS")

        expected = "ORDERS (CUSTOMER_ID, REGION) REFERENCES CUSTOMERS (ID, REGION)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"
        assert "ASOF" not in sql, "ASOF should not appear in equality-only relationships"


class TestGenerateSqlReferencesResolvedFormat:
    """Test generate_sql_references() with resolved SQL format (TABLE.COLUMN)."""

    def test_resolved_format_equality(self):
        """Test equality with resolved format."""
        condition = "ORDERS.CUSTOMER_ID = CUSTOMERS.ID"
        parsed = JoinConditionParser.parse(condition)

        sql = JoinConditionParser.generate_sql_references([parsed], "ORDERS", "CUSTOMERS")

        expected = "ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (ID)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"

    def test_resolved_format_asof(self):
        """Test ASOF with resolved format."""
        condition = "ORDERS.ORDERED_AT >= ORDERS.ORDERED_AT"
        parsed = JoinConditionParser.parse(condition)

        sql = JoinConditionParser.generate_sql_references([parsed], "ORDERS", "ORDERS")

        expected = "ORDERS (ORDERED_AT) REFERENCES ORDERS (ASOF ORDERED_AT)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"

    def test_resolved_format_mixed(self):
        """Test mixed equality + ASOF with resolved format."""
        conditions = ["ORDERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID", "ORDERS.ORDERED_AT >= ORDERS.ORDERED_AT"]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "ORDERS", "ORDERS")

        expected = "ORDERS (CUSTOMER_ID, ORDERED_AT) REFERENCES ORDERS (CUSTOMER_ID, ASOF ORDERED_AT)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"


class TestGenerateSqlReferencesEdgeCases:
    """Test edge cases for generate_sql_references()."""

    def test_empty_conditions_list(self):
        """Test with empty conditions list."""
        sql = JoinConditionParser.generate_sql_references([], "ORDERS", "CUSTOMERS")
        assert sql == "", "Empty conditions should return empty string"

    def test_no_column_duplication(self):
        """Test that columns are not duplicated in output."""
        conditions = [
            "{{ column('orders', 'customer_id') }} = {{ column('orders', 'customer_id') }}",
            "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}",
        ]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "ORDERS", "ORDERS")

        # Check no duplication
        assert sql.count("CUSTOMER_ID") == 2, "CUSTOMER_ID should appear exactly twice (left and right)"
        assert sql.count("ORDERED_AT") == 2, "ORDERED_AT should appear exactly twice (left and right)"

    def test_column_ordering_preserved(self):
        """Test that column ordering is preserved when ASOF comes before equality.

        Regression test for Greptile issue: if conditions are given in order
        (ASOF, equality), left and right columns must maintain same order.
        """
        # ASOF condition BEFORE equality condition (unusual but valid)
        conditions = [
            "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}",
            "{{ column('orders', 'customer_id') }} = {{ column('orders', 'customer_id') }}",
        ]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "ORDERS", "ORDERS")

        # Left side should be: (ORDERED_AT, CUSTOMER_ID) - original order
        # Right side should be: (ASOF ORDERED_AT, CUSTOMER_ID) - same order with ASOF prefix
        expected = "ORDERS (ORDERED_AT, CUSTOMER_ID) REFERENCES ORDERS (ASOF ORDERED_AT, CUSTOMER_ID)"
        assert sql == expected, f"Expected: {expected}, Got: {sql}"


class TestParsedConditionDataclass:
    """Test that ParsedCondition no longer has match_condition field."""

    def test_no_match_condition_field(self):
        """Verify match_condition field was removed from ParsedCondition."""
        condition = "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
        parsed = JoinConditionParser.parse(condition)

        assert not hasattr(
            parsed, "match_condition"
        ), "ParsedCondition should not have match_condition field (removed in Issue #40 fix)"

    def test_parsed_condition_has_required_fields(self):
        """Verify ParsedCondition has all required fields."""
        condition = "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
        parsed = JoinConditionParser.parse(condition)

        # Check all required fields exist
        assert hasattr(parsed, "join_condition")
        assert hasattr(parsed, "condition_type")
        assert hasattr(parsed, "left_expression")
        assert hasattr(parsed, "right_expression")
        assert hasattr(parsed, "left_table")
        assert hasattr(parsed, "left_column")
        assert hasattr(parsed, "right_table")
        assert hasattr(parsed, "right_column")
        assert hasattr(parsed, "operator")


class TestJoinTypeDetection:
    """Test that ASOF join type is correctly detected."""

    def test_greater_than_equal_is_asof(self):
        """Test >= operator is detected as ASOF."""
        condition = "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
        parsed = JoinConditionParser.parse(condition)
        assert parsed.condition_type == JoinType.ASOF

    def test_equals_is_equality(self):
        """Test = operator is detected as EQUALITY."""
        condition = "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
        parsed = JoinConditionParser.parse(condition)
        assert parsed.condition_type == JoinType.EQUALITY

    def test_less_than_equal_is_unknown(self):
        """Test <= operator is NOT detected as ASOF (unsupported)."""
        condition = "{{ column('orders', 'ordered_at') }} <= {{ column('orders', 'ordered_at') }}"
        parsed = JoinConditionParser.parse(condition)
        assert parsed.condition_type == JoinType.UNKNOWN

    def test_less_than_is_unknown(self):
        """Test < operator is NOT detected as ASOF (unsupported)."""
        condition = "{{ column('orders', 'ordered_at') }} < {{ column('orders', 'ordered_at') }}"
        parsed = JoinConditionParser.parse(condition)
        assert parsed.condition_type == JoinType.UNKNOWN

    def test_greater_than_is_unknown(self):
        """Test > operator is NOT detected as ASOF (unsupported)."""
        condition = "{{ column('orders', 'ordered_at') }} > {{ column('orders', 'ordered_at') }}"
        parsed = JoinConditionParser.parse(condition)
        assert parsed.condition_type == JoinType.UNKNOWN


class TestValidationUnsupportedOperators:
    """Test that unsupported temporal operators are rejected with clear errors."""

    def test_validate_less_than_equal_operator_rejected(self):
        """Test that <= operator is rejected with helpful error message."""
        condition = "{{ column('orders', 'ordered_at') }} <= {{ column('orders', 'ordered_at') }}"
        is_valid, error_msg = JoinConditionParser.validate_condition(condition)

        assert is_valid is False
        assert "not supported" in error_msg
        assert "<=" in error_msg

    def test_validate_less_than_operator_rejected(self):
        """Test that < operator is rejected with helpful error message."""
        condition = "{{ column('orders', 'ordered_at') }} < {{ column('orders', 'ordered_at') }}"
        is_valid, error_msg = JoinConditionParser.validate_condition(condition)

        assert is_valid is False
        assert "not supported" in error_msg
        assert "<" in error_msg

    def test_validate_greater_than_operator_rejected(self):
        """Test that > operator is rejected with helpful error message."""
        condition = "{{ column('orders', 'ordered_at') }} > {{ column('orders', 'ordered_at') }}"
        is_valid, error_msg = JoinConditionParser.validate_condition(condition)

        assert is_valid is False
        assert "not supported" in error_msg
        assert ">" in error_msg

    def test_validate_between_operator_rejected(self):
        """Test that BETWEEN operator is rejected with helpful error message."""
        condition = "{{ column('orders', 'ordered_at') }} BETWEEN {{ column('orders', 'start_date') }} AND {{ column('orders', 'end_date') }}"
        is_valid, error_msg = JoinConditionParser.validate_condition(condition)

        assert is_valid is False
        assert "BETWEEN" in error_msg
        assert "not supported" in error_msg

    def test_validate_less_than_operator_rejected(self):
        """Test that < operator is rejected with helpful error message."""
        condition = "{{ column('orders', 'ordered_at') }} < {{ column('orders', 'ordered_at') }}"
        is_valid, error_msg = JoinConditionParser.validate_condition(condition)

        assert not is_valid, "< operator should be rejected"
        assert "<" in error_msg
        assert "not supported" in error_msg.lower()

    def test_validate_greater_than_operator_rejected(self):
        """Test that > operator is rejected with helpful error message."""
        condition = "{{ column('orders', 'ordered_at') }} > {{ column('orders', 'ordered_at') }}"
        is_valid, error_msg = JoinConditionParser.validate_condition(condition)

        assert not is_valid, "> operator should be rejected"
        assert ">" in error_msg
        assert "not supported" in error_msg.lower()

    def test_validate_greater_than_equal_operator_accepted(self):
        """Test that >= operator is accepted (the only valid ASOF operator)."""
        condition = "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
        is_valid, error_msg = JoinConditionParser.validate_condition(condition)

        assert is_valid, f">= operator should be accepted. Error: {error_msg}"
        assert error_msg == "", "No error message should be returned for valid condition"


class TestUnifiedRefSyntax:
    """Test unified {{ ref() }} syntax in join conditions (Issue #97)."""

    def test_ref_equality_condition(self):
        """Test equality condition using unified ref() syntax."""
        condition = "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'id') }}"
        parsed = JoinConditionParser.parse(condition)

        assert parsed.left_table == "ORDERS"
        assert parsed.left_column == "CUSTOMER_ID"
        assert parsed.right_table == "CUSTOMERS"
        assert parsed.right_column == "ID"
        assert parsed.condition_type == JoinType.EQUALITY

        sql = JoinConditionParser.generate_sql_references([parsed], "ORDERS", "CUSTOMERS")
        expected = "ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (ID)"
        assert sql == expected

    def test_ref_asof_condition(self):
        """Test ASOF condition using unified ref() syntax."""
        condition = "{{ ref('events', 'timestamp') }} >= {{ ref('sessions', 'start_time') }}"
        parsed = JoinConditionParser.parse(condition)

        assert parsed.left_table == "EVENTS"
        assert parsed.left_column == "TIMESTAMP"
        assert parsed.right_table == "SESSIONS"
        assert parsed.right_column == "START_TIME"
        assert parsed.condition_type == JoinType.ASOF

        sql = JoinConditionParser.generate_sql_references([parsed], "EVENTS", "SESSIONS")
        expected = "EVENTS (TIMESTAMP) REFERENCES SESSIONS (ASOF START_TIME)"
        assert sql == expected

    def test_ref_mixed_equality_and_asof(self):
        """Test mixed equality and ASOF conditions using ref() syntax."""
        conditions = [
            "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'id') }}",
            "{{ ref('orders', 'ordered_at') }} >= {{ ref('customers', 'created_at') }}",
        ]
        parsed_list = [JoinConditionParser.parse(c) for c in conditions]

        sql = JoinConditionParser.generate_sql_references(parsed_list, "ORDERS", "CUSTOMERS")
        expected = "ORDERS (CUSTOMER_ID, ORDERED_AT) REFERENCES CUSTOMERS (ID, ASOF CREATED_AT)"
        assert sql == expected

    def test_ref_with_double_quotes(self):
        """Test ref() syntax with double quotes."""
        condition = '{{ ref("orders", "customer_id") }} = {{ ref("customers", "id") }}'
        parsed = JoinConditionParser.parse(condition)

        assert parsed.left_table == "ORDERS"
        assert parsed.left_column == "CUSTOMER_ID"
        assert parsed.right_table == "CUSTOMERS"
        assert parsed.right_column == "ID"

    def test_ref_mixed_with_legacy_column_syntax(self):
        """Test mixing ref() with legacy column() syntax."""
        condition = "{{ ref('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
        parsed = JoinConditionParser.parse(condition)

        assert parsed.left_table == "ORDERS"
        assert parsed.left_column == "CUSTOMER_ID"
        assert parsed.right_table == "CUSTOMERS"
        assert parsed.right_column == "ID"

    def test_ref_case_insensitive(self):
        """Test ref() handles case-insensitive table/column names."""
        condition = "{{ ref('ORDERS', 'CUSTOMER_ID') }} = {{ ref('CUSTOMERS', 'ID') }}"
        parsed = JoinConditionParser.parse(condition)

        assert parsed.left_table == "ORDERS"
        assert parsed.left_column == "CUSTOMER_ID"
        assert parsed.right_table == "CUSTOMERS"
        assert parsed.right_column == "ID"

    def test_ref_with_whitespace(self):
        """Test ref() handles whitespace variations."""
        condition = "{{  ref  (  'orders'  ,  'customer_id'  )  }} = {{  ref  (  'customers'  ,  'id'  )  }}"
        parsed = JoinConditionParser.parse(condition)

        assert parsed.left_table == "ORDERS"
        assert parsed.left_column == "CUSTOMER_ID"
        assert parsed.right_table == "CUSTOMERS"
        assert parsed.right_column == "ID"
