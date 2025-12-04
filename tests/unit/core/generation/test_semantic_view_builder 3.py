#!/usr/bin/env python3
"""
Unit tests for SemanticViewBuilder

Tests the table reference extraction and validation logic in semantic view generation.
"""

import pytest
from snowflake_semantic_tools.core.generation.semantic_view_builder import SemanticViewBuilder
from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig


class TestSemanticViewBuilder:
    """Test cases for SemanticViewBuilder table reference validation."""
    
    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test",
            password="test",
            role="test",
            warehouse="test",
            database="test",
            schema="test"
        )
        return SemanticViewBuilder(config)
    
    def test_extract_table_references_simple(self, builder):
        """Test extraction of simple table.column references."""
        expression = "COUNT(DISTINCT AI_MESSAGES.CONVERSATION_ID)"
        result = builder._extract_table_references_from_expression(expression)
        assert result == ['ai_messages'], f"Expected ['ai_messages'], got {result}"
    
    def test_extract_table_references_multiple_tables(self, builder):
        """Test extraction when multiple tables are referenced."""
        expression = """
            (COUNT(AI_CONTEXT.CONTEXT_ID)) / 
            NULLIF((COUNT(DISTINCT AI_MESSAGES_MEMBER_FACING.CONVERSATION_ID)), 0)
        """
        result = builder._extract_table_references_from_expression(expression)
        assert 'ai_context' in result
        assert 'ai_messages_member_facing' in result
        assert len(result) == 2, f"Expected 2 tables, got {len(result)}: {result}"
    
    def test_extract_table_references_complex_expression(self, builder):
        """Test extraction from complex nested expressions."""
        expression = """
            CASE 
                WHEN MEMBERSHIP_STATUS_DAILY.IS_ACTIVE = TRUE 
                THEN SUM(AI_COSTS.AMOUNT_VALUE)
                ELSE 0 
            END / NULLIF(COUNT(DISTINCT SINGLE_CUSTOMER_VIEW.USER_ID), 0)
        """
        result = builder._extract_table_references_from_expression(expression)
        assert 'membership_status_daily' in result
        assert 'ai_costs' in result
        assert 'single_customer_view' in result
        assert len(result) == 3, f"Expected 3 tables, got {len(result)}: {result}"
    
    def test_extract_table_references_no_references(self, builder):
        """Test extraction when no table references exist."""
        expression = "COUNT(*)"
        result = builder._extract_table_references_from_expression(expression)
        assert result == [], f"Expected empty list, got {result}"
    
    def test_extract_table_references_with_spaces(self, builder):
        """Test extraction with various spacing patterns."""
        expression = "TABLE_A . COLUMN_A + TABLE_B.COLUMN_B"
        result = builder._extract_table_references_from_expression(expression)
        assert 'table_a' in result
        assert 'table_b' in result
    
    def test_extract_table_references_mixed_case(self, builder):
        """Test that extraction normalizes to lowercase."""
        expression = "AI_Messages.id + ai_messages.count + Ai_Messages.value"
        result = builder._extract_table_references_from_expression(expression)
        assert result == ['ai_messages'], f"Expected ['ai_messages'], got {result}"
    
    def test_extract_table_references_ignores_sql_keywords(self, builder):
        """Test that SQL keywords are not treated as table names."""
        expression = "CAST(value AS INTEGER) + EXTRACT(year FROM date)"
        result = builder._extract_table_references_from_expression(expression)
        # CAST and EXTRACT should be filtered out
        assert 'cast' not in result
        assert 'extract' not in result
    
    def test_extract_table_references_underscores_and_numbers(self, builder):
        """Test extraction of table names with underscores and numbers."""
        expression = """
            TABLE_123.column_a + 
            my_table_v2.column_b + 
            _private_table.column_c
        """
        result = builder._extract_table_references_from_expression(expression)
        assert 'table_123' in result
        assert 'my_table_v2' in result
        assert '_private_table' in result


class TestMetricValidationScenarios:
    """Test realistic scenarios for metric validation in semantic views."""
    
    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test", 
            password="test",
            role="test",
            warehouse="test",
            database="test",
            schema="test"
        )
        return SemanticViewBuilder(config)
    
    def test_scenario_missing_table_in_view(self, builder):
        """
        Test the scenario from the bug report:
        - Semantic view has: [ai_context, single_customer_view]
        - Metric references: AI_MESSAGES_MEMBER_FACING.CONVERSATION_ID
        - Should extract 'ai_messages_member_facing' as missing table
        """
        metric_expression = """
            (COUNT(AI_CONTEXT.CONTEXT_ID)) / 
            NULLIF((COUNT(DISTINCT AI_MESSAGES_MEMBER_FACING.CONVERSATION_ID)), 0)
        """
        
        # Extract table references
        referenced_tables = builder._extract_table_references_from_expression(metric_expression)
        
        # Verify we found both tables
        assert 'ai_context' in referenced_tables
        assert 'ai_messages_member_facing' in referenced_tables
        
        # Simulate semantic view with only some tables
        available_tables = {'ai_context', 'single_customer_view'}
        
        # Check which tables are missing
        missing = [t for t in referenced_tables if t not in available_tables]
        
        # The metric should be rejected because ai_messages_member_facing is missing
        assert 'ai_messages_member_facing' in missing
        assert len(missing) == 1
    
    def test_scenario_all_tables_present(self, builder):
        """
        Test valid scenario where all referenced tables are in the view.
        """
        metric_expression = """
            COUNT(DISTINCT AI_CONTEXT.USER_ID)
        """
        
        referenced_tables = builder._extract_table_references_from_expression(metric_expression)
        available_tables = {'ai_context', 'single_customer_view'}
        
        missing = [t for t in referenced_tables if t not in available_tables]
        
        # No missing tables - metric should be included
        assert len(missing) == 0
    
    def test_scenario_composite_metric_multiple_missing_tables(self, builder):
        """
        Test composite metric that references multiple tables not in the view.
        """
        metric_expression = """
            (SUM(AI_COSTS.AMOUNT_VALUE) / 
            NULLIF(COUNT(DISTINCT MEMBERSHIP_STATUS_DAILY.USER_ID), 0)) *
            AVG(AI_TOKEN_USAGE.TOKEN_COUNT)
        """
        
        referenced_tables = builder._extract_table_references_from_expression(metric_expression)
        available_tables = {'single_customer_view'}
        
        missing = [t for t in referenced_tables if t not in available_tables]
        
        # All three tables should be missing
        assert 'ai_costs' in missing
        assert 'membership_status_daily' in missing
        assert 'ai_token_usage' in missing
        assert len(missing) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

