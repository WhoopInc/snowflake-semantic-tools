"""
Tests for the JoinConditionParser.

Tests parsing of join conditions with template expressions,
operator detection, and SQL generation for ASOF joins.
"""

import pytest
from snowflake_semantic_tools.core.parsing.join_condition_parser import (
    JoinConditionParser,
    JoinType,
    ParsedCondition
)


class TestJoinConditionParser:
    """Test the JoinConditionParser functionality."""
    
    def test_parse_equality_condition(self):
        """Test parsing a standard equality join condition."""
        condition = "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.condition_type == JoinType.EQUALITY
        assert parsed.operator == '='
        assert parsed.left_table == 'orders'
        assert parsed.left_column == 'customer_id'
        assert parsed.right_table == 'customers'
        assert parsed.right_column == 'id'
        assert parsed.match_condition is None
    
    def test_parse_asof_greater_equal_condition(self):
        """Test parsing an ASOF join with >= operator."""
        condition = "{{ column('events', 'event_time') }} >= {{ column('sessions', 'start_time') }}"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.condition_type == JoinType.ASOF
        assert parsed.operator == '>='
        assert parsed.left_table == 'events'
        assert parsed.left_column == 'event_time'
        assert parsed.right_table == 'sessions'
        assert parsed.right_column == 'start_time'
        assert parsed.match_condition == "event_time >= start_time"
    
    def test_parse_asof_less_equal_condition(self):
        """Test parsing an ASOF join with <= operator."""
        condition = "{{ column('events', 'event_time') }} <= {{ column('sessions', 'end_time') }}"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.condition_type == JoinType.ASOF
        assert parsed.operator == '<='
        assert parsed.left_table == 'events'
        assert parsed.left_column == 'event_time'
        assert parsed.right_table == 'sessions'
        assert parsed.right_column == 'end_time'
        assert parsed.match_condition == "event_time <= end_time"
    
    def test_parse_range_condition(self):
        """Test parsing a BETWEEN range condition."""
        condition = "{{ column('metrics', 'value') }} BETWEEN {{ column('thresholds', 'min_val') }} AND {{ column('thresholds', 'max_val') }}"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.condition_type == JoinType.RANGE
        assert parsed.operator == 'BETWEEN'
        assert parsed.left_table == 'metrics'
        assert parsed.left_column == 'value'
        assert parsed.right_table == 'thresholds'
        assert parsed.right_column == 'min_val'
        # Note: BETWEEN parsing only captures first two parts
    
    def test_parse_with_whitespace_variations(self):
        """Test parsing with various whitespace patterns."""
        conditions = [
            "{{column('a','b')}}={{column('c','d')}}",
            "{{ column( 'a' , 'b' ) }} = {{ column( 'c' , 'd' ) }}",
            "{{  column('a',  'b')  }}  =  {{  column('c',  'd')  }}"
        ]
        
        for condition in conditions:
            parsed = JoinConditionParser.parse(condition)
            assert parsed.left_table == 'a'
            assert parsed.left_column == 'b'
            assert parsed.right_table == 'c'
            assert parsed.right_column == 'd'
    
    def test_validate_condition_valid(self):
        """Test validation of valid conditions."""
        valid_conditions = [
            "{{ column('orders', 'id') }} = {{ column('items', 'order_id') }}",
            "{{ column('events', 'time') }} >= {{ column('sessions', 'start') }}",
            "{{ column('events', 'time') }} <= {{ column('sessions', 'end') }}",
        ]
        
        for condition in valid_conditions:
            is_valid, error_msg = JoinConditionParser.validate_condition(condition)
            assert is_valid, f"Condition should be valid: {condition}"
            assert error_msg == ""
    
    def test_validate_condition_invalid(self):
        """Test validation of invalid conditions."""
        # Invalid: Missing table.column structure
        is_valid, error_msg = JoinConditionParser.validate_condition("invalid_format")
        assert not is_valid
        # Either "Unknown" operator or "Could not extract"
        assert ("Unknown" in error_msg or "Could not extract" in error_msg)
        
        # Unknown operator
        is_valid, error_msg = JoinConditionParser.validate_condition(
            "{{ column('a', 'b') }} UNKNOWN {{ column('c', 'd') }}"
        )
        assert not is_valid
        assert "Unknown" in error_msg
    
    def test_generate_sql_references_equality(self):
        """Test SQL generation for equality joins."""
        condition = "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
        parsed = JoinConditionParser.parse(condition)
        
        sql = JoinConditionParser.generate_sql_references(
            [parsed], "ORDERS", "CUSTOMERS"
        )
        
        assert sql == "ORDERS (customer_id) REFERENCES CUSTOMERS (id)"
    
    def test_generate_sql_references_asof(self):
        """Test SQL generation for ASOF joins."""
        conditions = [
            "{{ column('events', 'session_id') }} = {{ column('sessions', 'id') }}",
            "{{ column('events', 'event_time') }} >= {{ column('sessions', 'start_time') }}"
        ]
        parsed_conditions = [JoinConditionParser.parse(c) for c in conditions]
        
        sql = JoinConditionParser.generate_sql_references(
            parsed_conditions, "EVENTS", "SESSIONS"
        )
        
        expected = "EVENTS (session_id, event_time) REFERENCES SESSIONS (id, start_time)\n" \
                  "      MATCH CONDITION (event_time >= start_time)"
        assert sql == expected
    
    def test_generate_sql_references_multiple_asof(self):
        """Test SQL generation with multiple ASOF conditions."""
        conditions = [
            "{{ column('events', 'user_id') }} = {{ column('sessions', 'user_id') }}",
            "{{ column('events', 'event_time') }} >= {{ column('sessions', 'start_time') }}",
            "{{ column('events', 'event_time') }} <= {{ column('sessions', 'end_time') }}"
        ]
        parsed_conditions = [JoinConditionParser.parse(c) for c in conditions]
        
        sql = JoinConditionParser.generate_sql_references(
            parsed_conditions, "EVENTS", "SESSIONS"
        )
        
        assert "MATCH CONDITION" in sql
        assert "event_time >= start_time" in sql
        assert "event_time <= end_time" in sql
        assert " AND " in sql  # Multiple conditions joined with AND
    
    def test_parse_multiple(self):
        """Test parsing multiple conditions at once."""
        conditions = [
            "{{ column('a', 'x') }} = {{ column('b', 'y') }}",
            "{{ column('a', 'time') }} >= {{ column('b', 'start') }}"
        ]
        
        parsed_list = JoinConditionParser.parse_multiple(conditions)
        
        assert len(parsed_list) == 2
        assert parsed_list[0].condition_type == JoinType.EQUALITY
        assert parsed_list[1].condition_type == JoinType.ASOF
    
    def test_operator_detection(self):
        """Test detection of various operators."""
        test_cases = [
            ("{{ column('a', 'b') }} = {{ column('c', 'd') }}", '='),
            ("{{ column('a', 'b') }} >= {{ column('c', 'd') }}", '>='),
            ("{{ column('a', 'b') }} <= {{ column('c', 'd') }}", '<='),
            ("{{ column('a', 'b') }} > {{ column('c', 'd') }}", '>'),
            ("{{ column('a', 'b') }} < {{ column('c', 'd') }}", '<'),
            ("{{ column('a', 'b') }} != {{ column('c', 'd') }}", '!='),
            ("{{ column('a', 'b') }} <> {{ column('c', 'd') }}", '<>'),
            ("{{ column('a', 'b') }} BETWEEN {{ column('c', 'd') }} AND {{ column('c', 'e') }}", 'BETWEEN'),
        ]
        
        for condition, expected_operator in test_cases:
            parsed = JoinConditionParser.parse(condition)
            assert parsed.operator == expected_operator, f"Failed for condition: {condition}"
    
    def test_join_type_detection(self):
        """Test detection of join types based on operators."""
        test_cases = [
            ('=', JoinType.EQUALITY),
            ('>=', JoinType.ASOF),
            ('<=', JoinType.ASOF),
            ('>', JoinType.ASOF),
            ('<', JoinType.ASOF),
            ('BETWEEN', JoinType.RANGE),
            ('!=', JoinType.UNKNOWN),  # Not supported for joins
            ('<>', JoinType.UNKNOWN),  # Not supported for joins
        ]
        
        for operator, expected_type in test_cases:
            join_type = JoinConditionParser._detect_join_type(operator)
            assert join_type == expected_type, f"Failed for operator: {operator}"
    
    def test_empty_conditions(self):
        """Test handling of empty condition lists."""
        sql = JoinConditionParser.generate_sql_references([], "A", "B")
        assert sql == ""
    
    def test_complex_table_names(self):
        """Test parsing with complex table names (with underscores, numbers)."""
        condition = "{{ column('order_items_2024', 'product_id') }} = {{ column('products_v2', 'id') }}"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.left_table == 'order_items_2024'
        assert parsed.left_column == 'product_id'
        assert parsed.right_table == 'products_v2'
        assert parsed.right_column == 'id'
    
    def test_quoted_variations(self):
        """Test with single and double quotes."""
        conditions = [
            "{{ column('orders', 'id') }} = {{ column('items', 'order_id') }}",
            '{{ column("orders", "id") }} = {{ column("items", "order_id") }}'
        ]
        
        for condition in conditions:
            parsed = JoinConditionParser.parse(condition)
            assert parsed.left_table == 'orders'
            assert parsed.left_column == 'id'
            assert parsed.right_table == 'items'
            assert parsed.right_column == 'order_id'
    
    # =================== RESOLVED FORMAT TESTS ===================
    
    def test_parse_resolved_equality(self):
        """Test parsing resolved SQL format with equality."""
        condition = "ORDERS.CUSTOMER_ID = CUSTOMERS.ID"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.condition_type == JoinType.EQUALITY
        assert parsed.operator == '='
        assert parsed.left_table == 'ORDERS'
        assert parsed.left_column == 'CUSTOMER_ID'
        assert parsed.right_table == 'CUSTOMERS'
        assert parsed.right_column == 'ID'
        assert parsed.match_condition is None
    
    def test_parse_resolved_asof(self):
        """Test parsing resolved ASOF join."""
        condition = "EVENTS.EVENT_TIME >= SESSIONS.START_TIME"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.condition_type == JoinType.ASOF
        assert parsed.operator == '>='
        assert parsed.left_table == 'EVENTS'
        assert parsed.left_column == 'EVENT_TIME'
        assert parsed.right_table == 'SESSIONS'
        assert parsed.right_column == 'START_TIME'
        assert parsed.match_condition == "EVENT_TIME >= START_TIME"
    
    def test_parse_resolved_mixed_case(self):
        """Test resolved format handles mixed case (normalizes to uppercase)."""
        condition = "Orders.customer_id = Customers.id"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.left_table == 'ORDERS'
        assert parsed.left_column == 'CUSTOMER_ID'
        assert parsed.right_table == 'CUSTOMERS'
        assert parsed.right_column == 'ID'
    
    def test_parse_resolved_with_underscores(self):
        """Test resolved format with underscores in names."""
        condition = "ORDER_ITEMS_2024.ORDER_ID = ORDERS_ARCHIVE.ID"
        parsed = JoinConditionParser.parse(condition)
        
        assert parsed.left_table == 'ORDER_ITEMS_2024'
        assert parsed.left_column == 'ORDER_ID'
        assert parsed.right_table == 'ORDERS_ARCHIVE'
        assert parsed.right_column == 'ID'
    
    def test_generate_sql_from_resolved(self):
        """Test SQL generation from resolved format."""
        condition = "ORDERS.CUSTOMER_ID = CUSTOMERS.ID"
        parsed = JoinConditionParser.parse(condition)
        
        sql = JoinConditionParser.generate_sql_references(
            [parsed], "ORDERS", "CUSTOMERS"
        )
        
        assert sql == "ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (ID)"
    
    def test_generate_sql_asof_from_resolved(self):
        """Test ASOF SQL generation from resolved format."""
        conditions = [
            "EVENTS.SESSION_ID = SESSIONS.ID",
            "EVENTS.EVENT_TIME >= SESSIONS.START_TIME"
        ]
        parsed_conditions = [JoinConditionParser.parse(c) for c in conditions]
        
        sql = JoinConditionParser.generate_sql_references(
            parsed_conditions, "EVENTS", "SESSIONS"
        )
        
        assert "MATCH CONDITION" in sql
        assert "EVENT_TIME >= START_TIME" in sql
    
    def test_validate_resolved_condition(self):
        """Test validation of resolved conditions."""
        # Valid resolved conditions
        valid_conditions = [
            "ORDERS.ID = CUSTOMERS.CUSTOMER_ID",
            "EVENTS.TIMESTAMP >= SESSIONS.START_TIME",
            "METRICS.VALUE <= THRESHOLDS.MAX_VALUE"
        ]
        
        for condition in valid_conditions:
            is_valid, error_msg = JoinConditionParser.validate_condition(condition)
            assert is_valid, f"Condition should be valid: {condition}, Error: {error_msg}"
    
    def test_mixed_template_and_resolved_parsing(self):
        """Test that both formats can be parsed in same session."""
        template_condition = "{{ column('orders', 'id') }} = {{ column('customers', 'id') }}"
        resolved_condition = "ORDERS.ID = CUSTOMERS.ID"
        
        parsed_template = JoinConditionParser.parse(template_condition)
        parsed_resolved = JoinConditionParser.parse(resolved_condition)
        
        # Both should extract the same logical structure
        assert parsed_template.left_table.upper() == parsed_resolved.left_table
        assert parsed_template.left_column.upper() == parsed_resolved.left_column
        assert parsed_template.right_table.upper() == parsed_resolved.right_table
        assert parsed_template.right_column.upper() == parsed_resolved.right_column
        assert parsed_template.operator == parsed_resolved.operator
