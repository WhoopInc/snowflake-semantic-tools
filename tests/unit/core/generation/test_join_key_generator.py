"""
Unit tests for JoinKeyDimensionGenerator.

Tests cover:
- Expression detection (DATE, DATE_TRUNC, ::DATE, CAST, TO_DATE, TO_TIMESTAMP)
- Dimension name generation (deterministic, unique)
- Deduplication (same expression reuses same dimension)
- get_dimensions_for_table filtering
"""

import pytest

from snowflake_semantic_tools.core.generation.join_key_generator import JoinKeyDimensionGenerator, detect_expression


class TestDetectExpression:
    """Test detect_expression() for various SQL transformation patterns."""

    def test_date_function_with_column_template(self):
        expr = "DATE({{ column('events', 'event_timestamp') }})"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "DATE(EVENT_TIMESTAMP)"

    def test_date_function_with_ref_template(self):
        expr = "DATE({{ ref('events', 'event_timestamp') }})"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "DATE(EVENT_TIMESTAMP)"

    def test_date_trunc_month(self):
        expr = "DATE_TRUNC('month', {{ column('events', 'event_timestamp') }})"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "DATE_TRUNC('month', EVENT_TIMESTAMP)"

    def test_date_trunc_quarter(self):
        expr = "DATE_TRUNC('quarter', {{ ref('orders', 'order_date') }})"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "ORDERS"
        assert result["column"] == "ORDER_DATE"
        assert result["sql_expression"] == "DATE_TRUNC('quarter', ORDER_DATE)"

    def test_type_cast_double_colon(self):
        expr = "{{ column('events', 'event_timestamp') }}::DATE"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "EVENT_TIMESTAMP::DATE"

    def test_cast_as_date(self):
        expr = "CAST({{ column('events', 'event_timestamp') }} AS DATE)"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "CAST(EVENT_TIMESTAMP AS DATE)"

    def test_to_date_function(self):
        expr = "TO_DATE({{ column('events', 'event_timestamp') }})"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "TO_DATE(EVENT_TIMESTAMP)"

    def test_to_timestamp_function(self):
        expr = "TO_TIMESTAMP({{ ref('events', 'date_str') }})"
        result = detect_expression(expr)
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "DATE_STR"
        assert result["sql_expression"] == "TO_TIMESTAMP(DATE_STR)"

    def test_plain_column_template_not_detected(self):
        expr = "{{ column('events', 'user_id') }}"
        result = detect_expression(expr)
        assert result is None

    def test_plain_ref_template_not_detected(self):
        expr = "{{ ref('events', 'user_id') }}"
        result = detect_expression(expr)
        assert result is None

    def test_empty_string_not_detected(self):
        assert detect_expression("") is None

    def test_arbitrary_text_not_detected(self):
        assert detect_expression("some random text") is None

    def test_case_insensitive_date(self):
        expr = "date({{ column('events', 'ts') }})"
        result = detect_expression(expr)
        assert result is not None
        assert result["sql_expression"] == "date(TS)"

    def test_case_insensitive_date_trunc(self):
        expr = "date_trunc('month', {{ column('events', 'ts') }})"
        result = detect_expression(expr)
        assert result is not None
        assert "TS" in result["sql_expression"]

    def test_whitespace_tolerance(self):
        expr = "DATE(  {{ column('events', 'ts') }}  )"
        result = detect_expression(expr)
        assert result is not None
        assert "TS" in result["sql_expression"]
        assert result["column"] == "TS"


class TestJoinKeyDimensionGenerator:
    """Test JoinKeyDimensionGenerator class."""

    def test_register_returns_dimension_name(self):
        gen = JoinKeyDimensionGenerator()
        name = gen.register_join_key("EVENTS", "EVENT_TIMESTAMP", "DATE(EVENT_TIMESTAMP)")
        assert name.startswith("_JK_EVENT_TIMESTAMP_")
        assert len(name) > len("_JK_EVENT_TIMESTAMP_")

    def test_same_expression_deduplicated(self):
        gen = JoinKeyDimensionGenerator()
        name1 = gen.register_join_key("EVENTS", "EVENT_TIMESTAMP", "DATE(EVENT_TIMESTAMP)")
        name2 = gen.register_join_key("EVENTS", "EVENT_TIMESTAMP", "DATE(EVENT_TIMESTAMP)")
        assert name1 == name2
        assert len(gen.get_all_dimensions()) == 1

    def test_different_expressions_unique_names(self):
        gen = JoinKeyDimensionGenerator()
        name1 = gen.register_join_key("EVENTS", "EVENT_TIMESTAMP", "DATE(EVENT_TIMESTAMP)")
        name2 = gen.register_join_key("EVENTS", "EVENT_TIMESTAMP", "DATE_TRUNC('month', EVENT_TIMESTAMP)")
        assert name1 != name2
        assert len(gen.get_all_dimensions()) == 2

    def test_different_tables_same_expression(self):
        gen = JoinKeyDimensionGenerator()
        name1 = gen.register_join_key("EVENTS", "TS", "DATE(TS)")
        name2 = gen.register_join_key("ORDERS", "TS", "DATE(TS)")
        assert name1 == name2 or name1 != name2
        assert len(gen.get_all_dimensions()) == 2

    def test_get_dimensions_for_table(self):
        gen = JoinKeyDimensionGenerator()
        gen.register_join_key("EVENTS", "TS", "DATE(TS)")
        gen.register_join_key("ORDERS", "ORDER_DATE", "DATE_TRUNC('month', ORDER_DATE)")
        gen.register_join_key("EVENTS", "TS", "DATE_TRUNC('quarter', TS)")

        events_dims = gen.get_dimensions_for_table("EVENTS")
        assert len(events_dims) == 2
        orders_dims = gen.get_dimensions_for_table("ORDERS")
        assert len(orders_dims) == 1

    def test_has_dimensions_empty(self):
        gen = JoinKeyDimensionGenerator()
        assert not gen.has_dimensions()

    def test_has_dimensions_after_register(self):
        gen = JoinKeyDimensionGenerator()
        gen.register_join_key("EVENTS", "TS", "DATE(TS)")
        assert gen.has_dimensions()

    def test_clear(self):
        gen = JoinKeyDimensionGenerator()
        gen.register_join_key("EVENTS", "TS", "DATE(TS)")
        assert gen.has_dimensions()
        gen.clear()
        assert not gen.has_dimensions()

    def test_generated_dimension_metadata(self):
        gen = JoinKeyDimensionGenerator()
        gen.register_join_key("EVENTS", "EVENT_TIMESTAMP", "DATE(EVENT_TIMESTAMP)")
        dims = gen.get_all_dimensions()
        assert len(dims) == 1
        dim = dims[0]
        assert dim["table"] == "EVENTS"
        assert dim["base_column"] == "EVENT_TIMESTAMP"
        assert dim["expression"] == "DATE(EVENT_TIMESTAMP)"
        assert "Auto-generated join key" in dim["comment"]
        assert dim["name"].startswith("_JK_")

    def test_deterministic_naming(self):
        gen1 = JoinKeyDimensionGenerator()
        gen2 = JoinKeyDimensionGenerator()
        name1 = gen1.register_join_key("EVENTS", "TS", "DATE(TS)")
        name2 = gen2.register_join_key("EVENTS", "TS", "DATE(TS)")
        assert name1 == name2

    def test_case_insensitive_dedup(self):
        gen = JoinKeyDimensionGenerator()
        name1 = gen.register_join_key("events", "ts", "DATE(TS)")
        name2 = gen.register_join_key("EVENTS", "ts", "date(ts)")
        assert name1 == name2
        assert len(gen.get_all_dimensions()) == 1


class TestDetectResolvedExpression:
    """Test detect_expression() with resolved (post-template-resolution) format."""

    def test_date_resolved(self):
        result = detect_expression("DATE(ORDERS.ORDERED_AT)")
        assert result is not None
        assert result["table"] == "ORDERS"
        assert result["column"] == "ORDERED_AT"
        assert result["sql_expression"] == "DATE(ORDERED_AT)"

    def test_date_trunc_resolved(self):
        result = detect_expression("DATE_TRUNC('day', ORDER_ITEMS.ORDERED_AT)")
        assert result is not None
        assert result["table"] == "ORDER_ITEMS"
        assert result["column"] == "ORDERED_AT"
        assert result["sql_expression"] == "DATE_TRUNC('day', ORDERED_AT)"

    def test_date_trunc_month_resolved(self):
        result = detect_expression("DATE_TRUNC('month', EVENTS.EVENT_TIMESTAMP)")
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "DATE_TRUNC('month', EVENT_TIMESTAMP)"

    def test_type_cast_resolved(self):
        result = detect_expression("EVENTS.EVENT_TIMESTAMP::DATE")
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "EVENT_TIMESTAMP::DATE"

    def test_cast_as_date_resolved(self):
        result = detect_expression("CAST(EVENTS.EVENT_TIMESTAMP AS DATE)")
        assert result is not None
        assert result["table"] == "EVENTS"
        assert result["column"] == "EVENT_TIMESTAMP"
        assert result["sql_expression"] == "CAST(EVENT_TIMESTAMP AS DATE)"

    def test_to_date_resolved(self):
        result = detect_expression("TO_DATE(EVENTS.EVENT_TIMESTAMP)")
        assert result is not None
        assert result["sql_expression"] == "TO_DATE(EVENT_TIMESTAMP)"

    def test_plain_resolved_column_not_detected(self):
        result = detect_expression("ORDERS.CUSTOMER_ID")
        assert result is None

    def test_plain_column_name_not_detected(self):
        result = detect_expression("CUSTOMER_ID")
        assert result is None


class TestGenericExpressionDetection:
    """With the generic approach, ANY expression wrapping a column template is detected."""

    def test_upper_detected(self):
        result = detect_expression("UPPER({{ column('users', 'email') }})")
        assert result is not None
        assert result["table"] == "USERS"
        assert result["column"] == "EMAIL"
        assert result["sql_expression"] == "UPPER(EMAIL)"

    def test_lower_detected(self):
        result = detect_expression("LOWER({{ column('users', 'email') }})")
        assert result is not None
        assert result["sql_expression"] == "LOWER(EMAIL)"

    def test_trim_detected(self):
        result = detect_expression("TRIM({{ column('users', 'name') }})")
        assert result is not None
        assert result["sql_expression"] == "TRIM(NAME)"

    def test_double_colon_timestamp_detected(self):
        result = detect_expression("{{ column('events', 'ts') }}::TIMESTAMP")
        assert result is not None
        assert result["sql_expression"] == "TS::TIMESTAMP"

    def test_double_colon_varchar_detected(self):
        result = detect_expression("{{ column('events', 'id') }}::VARCHAR")
        assert result is not None
        assert result["sql_expression"] == "ID::VARCHAR"

    def test_cast_as_timestamp_detected(self):
        result = detect_expression("CAST({{ column('events', 'ts') }} AS TIMESTAMP)")
        assert result is not None
        assert result["sql_expression"] == "CAST(TS AS TIMESTAMP)"

    def test_upper_resolved_detected(self):
        result = detect_expression("UPPER(USERS.EMAIL)")
        assert result is not None
        assert result["table"] == "USERS"
        assert result["column"] == "EMAIL"
        assert result["sql_expression"] == "UPPER(EMAIL)"

    def test_to_timestamp_resolved_detected(self):
        result = detect_expression("TO_TIMESTAMP(EVENTS.DATE_STR)")
        assert result is not None
        assert result["sql_expression"] == "TO_TIMESTAMP(DATE_STR)"


class TestExpressionEdgeCasesReturnNone:
    """Edge cases that should NOT be detected as expressions."""

    def test_plain_column_template_not_detected(self):
        assert detect_expression("{{ column('events', 'user_id') }}") is None

    def test_plain_ref_template_not_detected(self):
        assert detect_expression("{{ ref('events', 'user_id') }}") is None

    def test_empty_string_not_detected(self):
        assert detect_expression("") is None

    def test_arbitrary_text_not_detected(self):
        assert detect_expression("some random text") is None

    def test_plain_resolved_column_not_detected(self):
        assert detect_expression("ORDERS.CUSTOMER_ID") is None

    def test_plain_column_name_not_detected(self):
        assert detect_expression("CUSTOMER_ID") is None

    def test_date_literal_not_detected(self):
        assert detect_expression("DATE('2024-01-01')") is None

    def test_multi_column_template_not_detected(self):
        expr = "{{ column('a', 'col1') }} + {{ column('a', 'col2') }}"
        assert detect_expression(expr) is None

    def test_multi_resolved_column_not_detected(self):
        expr = "A.COL1 + A.COL2"
        assert detect_expression(expr) is None

    def test_coalesce_with_literal_multi_arg_detected_as_single_col(self):
        result = detect_expression("COALESCE({{ column('orders', 'user_id') }}, 0)")
        assert result is not None
        assert result["column"] == "USER_ID"
        assert result["sql_expression"] == "COALESCE(USER_ID, 0)"

    def test_nvl_with_literal_detected(self):
        result = detect_expression("NVL({{ column('orders', 'user_id') }}, 0)")
        assert result is not None
        assert result["sql_expression"] == "NVL(USER_ID, 0)"

    def test_arithmetic_with_literal_detected(self):
        result = detect_expression("{{ column('orders', 'amount') }} + 1")
        assert result is not None
        assert result["sql_expression"] == "AMOUNT + 1"

    def test_substring_with_literal_args_detected(self):
        result = detect_expression("SUBSTRING({{ column('users', 'name') }}, 1, 3)")
        assert result is not None
        assert result["sql_expression"] == "SUBSTRING(NAME, 1, 3)"

    def test_nested_expression_with_two_templates_not_detected(self):
        expr = "COALESCE({{ column('a', 'col1') }}, {{ column('a', 'col2') }})"
        assert detect_expression(expr) is None
