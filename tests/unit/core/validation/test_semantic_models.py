"""
Comprehensive tests for SemanticModelValidator.

Tests all validation rules for metrics, relationships, filters,
custom instructions, verified queries, and semantic views.
"""

import pytest
from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.core.validation.rules.semantic_models import SemanticModelValidator
from snowflake_semantic_tools.core.validation.constants import (
    SNOWFLAKE_MAX_IDENTIFIER_LENGTH,
    IDENTIFIER_WARNING_LENGTH,
)


class TestMetricValidation:
    """Test metric validation rules."""
    
    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()
    
    def test_valid_metric_passes(self, validator):
        """Test that a valid metric passes validation."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'description': 'Total revenue from all orders',
                    'expr': 'SUM(amount)',
                    'tables': ['orders'],
                    'synonyms': ['revenue', 'sales']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count == 0
    
    def test_metric_missing_name_error(self, validator):
        """Test that metric without name produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'expr': 'SUM(amount)',
                    'tables': ['orders']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: name' in e for e in errors)
    
    def test_metric_missing_expr_error(self, validator):
        """Test that metric without expr produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'tables': ['orders']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: expr' in e for e in errors)
    
    def test_metric_missing_tables_error(self, validator):
        """Test that metric without tables produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': 'SUM(amount)'
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: tables' in e for e in errors)
    
    def test_metric_tables_not_list_error(self, validator):
        """Test that metric with non-list tables produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': 'SUM(amount)',
                    'tables': 'orders'  # Should be list
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('tables' in e and 'must be a list' in e for e in errors)
    
    def test_metric_empty_tables_error(self, validator):
        """Test that metric with empty tables list produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': 'SUM(amount)',
                    'tables': []
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('tables' in e and 'cannot be empty' in e for e in errors)
    
    def test_metric_expr_not_string_error(self, validator):
        """Test that metric with non-string expr produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': 123,  # Should be string
                    'tables': ['orders']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('expr' in e and 'must be a string' in e for e in errors)
    
    def test_metric_empty_expr_error(self, validator):
        """Test that metric with empty expr produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': '   ',  # Empty/whitespace
                    'tables': ['orders']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('expr' in e and 'cannot be empty' in e for e in errors)
    
    def test_metric_with_default_aggregation_ignored(self, validator):
        """Test that default_aggregation field is ignored (not part of Snowflake spec).
        
        Note: default_aggregation is NOT validated because it's not used by Snowflake
        semantic views. Aggregation is embedded in the expr field (e.g., SUM(amount)).
        """
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'description': 'Total revenue',
                    'expr': 'SUM(amount)',
                    'tables': ['orders'],
                    'default_aggregation': 'anything_here_is_fine'  # Ignored field
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        # Should not produce any errors or warnings about default_aggregation
        assert result.error_count == 0
        warnings = [i.message for i in result.issues if i.severity.name == 'WARNING']
        assert not any('aggregation' in w.lower() for w in warnings)
    
    def test_metric_missing_description_warning(self, validator):
        """Test that metric without description produces WARNING."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': 'SUM(amount)',
                    'tables': ['orders']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        warnings = [i.message for i in result.issues if i.severity.name == 'WARNING']
        assert any('missing description' in w for w in warnings)
    
    def test_metric_synonyms_not_list_error(self, validator):
        """Test that metric with non-list synonyms produces ERROR."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': 'SUM(amount)',
                    'tables': ['orders'],
                    'synonyms': 'revenue'  # Should be list
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('synonyms' in e and 'must be a list' in e for e in errors)


class TestRelationshipValidation:
    """Test relationship validation rules."""
    
    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()
    
    def test_valid_relationship_passes(self, validator):
        """Test that a valid relationship passes validation."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'orders_to_customers',
                    'description': 'Join orders to customers',
                    'left_table': 'orders',
                    'right_table': 'customers',
                    'relationship_conditions': [
                        "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
                    ]
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count == 0
    
    def test_relationship_case_insensitive_enums(self, validator):
        """Test that enum values are case-insensitive."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'orders_to_customers',
                    'left_table': 'orders',
                    'right_table': 'customers',
                    'join_type': 'LEFT_OUTER',  # Uppercase should be valid
                    'relationship_type': 'MANY_TO_ONE',  # Uppercase should be valid
                    'relationship_columns': [
                        {'left_column': 'customer_id', 'right_column': 'id'}
                    ]
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        # Should not have errors for case differences
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert not any('invalid join_type' in e for e in errors)
        assert not any('invalid relationship_type' in e for e in errors)
    
    def test_relationship_hyphen_to_underscore(self, validator):
        """Test that hyphens in relationship_type are accepted."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'orders_to_customers',
                    'left_table': 'orders',
                    'right_table': 'customers',
                    'join_type': 'left_outer',
                    'relationship_type': 'many-to-one',  # Hyphen should be accepted
                    'relationship_columns': [
                        {'left_column': 'customer_id', 'right_column': 'id'}
                    ]
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        # Should not have errors for hyphen
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert not any('invalid relationship_type' in e for e in errors)
    
    def test_relationship_missing_required_fields_error(self, validator):
        """Test that relationship without required fields produces ERROR."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'orders_to_customers'
                    # Missing all other required fields
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        # Should have 3 errors: left_table, right_table, relationship_conditions
        assert result.error_count >= 3
    
    def test_relationship_conditions_not_list_error(self, validator):
        """Test that relationship with non-list relationship_conditions produces ERROR."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'orders_to_customers',
                    'left_table': 'orders',
                    'right_table': 'customers',
                    'relationship_conditions': 'single_string'  # Should be list
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('relationship_conditions' in e and 'must be a list' in e for e in errors)
    
    def test_relationship_empty_conditions_error(self, validator):
        """Test that relationship with empty relationship_conditions produces ERROR."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'orders_to_customers',
                    'left_table': 'orders',
                    'right_table': 'customers',
                    'relationship_conditions': []
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('relationship_conditions' in e and 'cannot be empty' in e for e in errors)
    
    def test_relationship_condition_invalid_format_error(self, validator):
        """Test that relationship with invalid condition format produces ERROR."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'orders_to_customers',
                    'left_table': 'orders',
                    'right_table': 'customers',
                    'relationship_conditions': [
                        123  # Should be a string, not an integer
                    ]
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count >= 1
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('must be a string' in e for e in errors)


class TestFilterValidation:
    """Test filter validation rules."""
    
    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()
    
    def test_valid_filter_passes(self, validator):
        """Test that a valid filter passes validation."""
        semantic_data = {
            'filters': {
                'items': [{
                    'name': 'active_users',
                    'description': 'Filter for active users only',
                    'expr': "status = 'active'",
                    'synonyms': ['active', 'current users']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count == 0
    
    def test_filter_missing_name_error(self, validator):
        """Test that filter without name produces ERROR."""
        semantic_data = {
            'filters': {
                'items': [{
                    'expr': "status = 'active'"
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: name' in e for e in errors)
    
    def test_filter_missing_expr_error(self, validator):
        """Test that filter without expr produces ERROR."""
        semantic_data = {
            'filters': {
                'items': [{
                    'name': 'active_users'
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: expr' in e for e in errors)


class TestCustomInstructionValidation:
    """Test custom instruction validation rules."""
    
    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()
    
    def test_valid_custom_instruction_passes(self, validator):
        """Test that a valid custom instruction passes validation."""
        semantic_data = {
            'custom_instructions': {
                'items': [{
                    'name': 'revenue_calculation',
                    'description': 'How to calculate revenue',
                    'instruction': 'When calculating revenue, always use gross_revenue and filter for completed transactions.'
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count == 0
    
    def test_custom_instruction_missing_fields_error(self, validator):
        """Test that custom instruction without required fields produces ERROR."""
        semantic_data = {
            'custom_instructions': {
                'items': [{
                    'name': 'revenue_calculation'
                    # Missing instruction
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: instruction' in e for e in errors)
    
    def test_custom_instruction_short_text_warning(self, validator):
        """Test that custom instruction with short text produces WARNING."""
        semantic_data = {
            'custom_instructions': {
                'items': [{
                    'name': 'revenue_calculation',
                    'instruction': 'short'  # < 10 chars
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        warnings = [i.message for i in result.issues if i.severity.name == 'WARNING']
        assert any('very short instruction text' in w for w in warnings)


class TestVerifiedQueryValidation:
    """Test verified query validation rules."""
    
    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()
    
    def test_valid_verified_query_passes(self, validator):
        """Test that a valid verified query passes validation."""
        semantic_data = {
            'verified_queries': {
                'items': [{
                    'name': 'total_revenue_query',
                    'description': 'Query for total revenue',
                    'question': 'What is the total revenue?',
                    'sql': 'SELECT SUM(amount) FROM orders',
                    'verified_at': '2024-01-15'
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count == 0
    
    def test_verified_query_missing_fields_error(self, validator):
        """Test that verified query without required fields produces ERROR."""
        semantic_data = {
            'verified_queries': {
                'items': [{
                    'name': 'total_revenue_query'
                    # Missing question and sql
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count >= 2
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: question' in e for e in errors)
        assert any('missing required field: sql' in e for e in errors)
    
    def test_verified_query_invalid_sql_warning(self, validator):
        """Test that verified query with non-SELECT SQL produces WARNING."""
        semantic_data = {
            'verified_queries': {
                'items': [{
                    'name': 'total_revenue_query',
                    'question': 'What is the total revenue?',
                    'sql': 'DELETE FROM orders'  # Not SELECT/WITH
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        warnings = [i.message for i in result.issues if i.severity.name == 'WARNING']
        assert any('should start with SELECT or WITH' in w for w in warnings)
    
    def test_verified_query_invalid_date_format_warning(self, validator):
        """Test that verified query with invalid date format produces ERROR (not WARNING)."""
        semantic_data = {
            'verified_queries': {
                'items': [{
                    'name': 'total_revenue_query',
                    'question': 'What is the total revenue?',
                    'sql': 'SELECT SUM(amount) FROM orders',
                    'verified_at': '01/15/2024'  # Wrong format
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('YYYY-MM-DD format' in e for e in errors)


class TestSemanticViewValidation:
    """Test semantic view validation rules."""
    
    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()
    
    def test_valid_semantic_view_passes(self, validator):
        """Test that a valid semantic view passes validation."""
        semantic_data = {
            'semantic_views': {
                'items': [{
                    'name': 'orders_view',
                    'description': 'Orders semantic view',
                    'tables': ['orders', 'customers'],
                    'dimensions': ['customer_name', 'status'],
                    'measures': ['total_amount'],
                    'time_dimensions': ['order_date']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count == 0
    
    def test_semantic_view_missing_fields_error(self, validator):
        """Test that semantic view without required fields produces ERROR."""
        semantic_data = {
            'semantic_views': {
                'items': [{
                    'name': 'orders_view'
                    # Missing tables
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('missing required field: tables' in e for e in errors)
    
    def test_semantic_view_empty_tables_error(self, validator):
        """Test that semantic view with empty tables produces ERROR."""
        semantic_data = {
            'semantic_views': {
                'items': [{
                    'name': 'orders_view',
                    'tables': []
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count > 0
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('tables' in e and 'cannot be empty' in e for e in errors)
    
    def test_semantic_view_no_content_warning(self, validator):
        """Test that semantic view with no dimensions/measures does NOT produce warning.
        
        This is BY DESIGN - semantic views are intentionally minimal in YAML
        and get enriched during generation from referenced tables' metadata.
        """
        semantic_data = {
            'semantic_views': {
                'items': [{
                    'name': 'orders_view',
                    'tables': ['orders']
                    # No dimensions, measures, or time_dimensions - this is OK!
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        # Should NOT produce a warning about missing dimensions/measures/time_dimensions
        # because these are auto-populated during generation
        warnings = [i.message for i in result.issues if i.severity.name == 'WARNING']
        assert not any('no dimensions, measures, or time_dimensions' in w for w in warnings)
    
    def test_semantic_view_tables_as_json_string(self, validator):
        """Test that semantic view with tables as JSON string (from parser) is valid."""
        semantic_data = {
            'semantic_views': {
                'items': [{
                    'name': 'orders_view',
                    'description': 'Orders semantic view',
                    'tables': '["orders", "customers"]',  # JSON string format from parser
                    'dimensions': ['customer_name'],
                    'measures': ['total_amount']
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        assert result.error_count == 0


class TestIdentifierLengthValidation:
    """Test identifier length validation (Issue #22)."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_valid_identifier_length_passes(self, validator):
        """Test that an identifier within limits passes validation."""
        result = ValidationResult()
        validator._validate_identifier_length("valid_metric_name", "Metric", result)
        assert result.error_count == 0
        assert len([i for i in result.issues if i.severity.name == 'WARNING']) == 0

    def test_identifier_at_exact_limit_passes(self, validator):
        """Test that an identifier exactly at 255 characters passes."""
        long_name = "a" * SNOWFLAKE_MAX_IDENTIFIER_LENGTH  # Exactly 255 chars
        result = ValidationResult()
        validator._validate_identifier_length(long_name, "Metric", result)
        assert result.error_count == 0

    def test_identifier_exceeds_limit_error(self, validator):
        """Test that an identifier exceeding 255 characters produces ERROR."""
        long_name = "a" * (SNOWFLAKE_MAX_IDENTIFIER_LENGTH + 1)  # 256 chars
        result = ValidationResult()
        validator._validate_identifier_length(long_name, "Metric", result)
        assert result.error_count == 1
        assert "exceeds Snowflake's 255-character limit" in result.issues[0].message

    def test_identifier_above_warning_threshold_warning(self, validator):
        """Test that an identifier >200 characters produces WARNING."""
        long_name = "a" * (IDENTIFIER_WARNING_LENGTH + 10)  # 210 chars
        result = ValidationResult()
        validator._validate_identifier_length(long_name, "Metric", result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 1
        assert "longer than recommended" in warnings[0].message

    def test_identifier_length_with_context(self, validator):
        """Test that context_name is included in error message."""
        long_name = "a" * (SNOWFLAKE_MAX_IDENTIFIER_LENGTH + 1)
        result = ValidationResult()
        validator._validate_identifier_length(long_name, "Column", result, context_name="orders")
        assert result.error_count == 1
        assert "orders." in result.issues[0].message

    def test_metric_long_name_integration(self, validator):
        """Test that a metric with a long name produces error in full validation."""
        long_name = "a" * (SNOWFLAKE_MAX_IDENTIFIER_LENGTH + 1)
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': long_name,
                    'description': 'Test metric',
                    'expr': 'SUM(amount)',
                    'tables': ['orders']
                }]
            }
        }
        result = validator.validate(semantic_data)
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('exceeds Snowflake' in e and '255-character limit' in e for e in errors)


class TestIdentifierCharacterValidation:
    """Test identifier character validation (Issue #25)."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_valid_identifier_passes(self, validator):
        """Test that valid identifiers pass validation."""
        result = ValidationResult()
        validator._validate_identifier_characters("valid_name_123", "Metric", result)
        assert result.error_count == 0

    def test_identifier_with_underscore_prefix_passes(self, validator):
        """Test that identifier starting with underscore passes."""
        result = ValidationResult()
        validator._validate_identifier_characters("_private_metric", "Metric", result)
        assert result.error_count == 0

    def test_identifier_with_dash_error(self, validator):
        """Test that identifier with dash produces ERROR."""
        result = ValidationResult()
        validator._validate_identifier_characters("metric-name", "Metric", result)
        assert result.error_count == 1
        assert "invalid characters" in result.issues[0].message
        assert "metric_name" in result.issues[0].message  # Suggested fix

    def test_identifier_with_space_error(self, validator):
        """Test that identifier with space produces ERROR."""
        result = ValidationResult()
        validator._validate_identifier_characters("metric name", "Metric", result)
        assert result.error_count == 1
        assert "invalid characters" in result.issues[0].message

    def test_identifier_with_dot_error(self, validator):
        """Test that identifier with dot produces ERROR."""
        result = ValidationResult()
        validator._validate_identifier_characters("metric.name", "Metric", result)
        assert result.error_count == 1
        assert "invalid characters" in result.issues[0].message

    def test_identifier_starting_with_digit_error(self, validator):
        """Test that identifier starting with digit produces ERROR."""
        result = ValidationResult()
        validator._validate_identifier_characters("2024_metric", "Metric", result)
        assert result.error_count == 1
        assert "invalid characters" in result.issues[0].message
        assert "_2024_metric" in result.issues[0].message  # Suggested fix

    def test_identifier_with_special_chars_error(self, validator):
        """Test that identifier with special characters produces ERROR."""
        result = ValidationResult()
        validator._validate_identifier_characters("metric@name!", "Metric", result)
        assert result.error_count == 1
        assert "invalid characters" in result.issues[0].message

    def test_filter_invalid_characters_integration(self, validator):
        """Test that a filter with invalid characters produces error in full validation."""
        semantic_data = {
            'filters': {
                'items': [{
                    'name': 'active-users',  # Invalid: contains dash
                    'description': 'Active users filter',
                    'expr': "status = 'active'"
                }]
            }
        }
        result = validator.validate(semantic_data)
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('invalid characters' in e for e in errors)


class TestReservedKeywordValidation:
    """Test reserved keyword validation (Issue #24)."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_normal_identifier_passes(self, validator):
        """Test that normal identifiers pass without warning."""
        result = ValidationResult()
        validator._validate_reserved_keywords("revenue", "Metric", result)
        assert len([i for i in result.issues if i.severity.name == 'WARNING']) == 0

    def test_select_keyword_warning(self, validator):
        """Test that 'select' keyword produces WARNING."""
        result = ValidationResult()
        validator._validate_reserved_keywords("select", "Metric", result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 1
        assert "SQL reserved keyword" in warnings[0].message

    def test_where_keyword_warning(self, validator):
        """Test that 'where' keyword produces WARNING."""
        result = ValidationResult()
        validator._validate_reserved_keywords("WHERE", "Metric", result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 1
        assert "SQL reserved keyword" in warnings[0].message

    def test_order_keyword_warning(self, validator):
        """Test that 'order' keyword produces WARNING (common conflict)."""
        result = ValidationResult()
        validator._validate_reserved_keywords("order", "Metric", result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 1
        assert "SQL reserved keyword" in warnings[0].message
        # Check for suggestions
        assert "order_value" in warnings[0].message or "suggestions" in str(warnings[0].context)

    def test_date_keyword_warning(self, validator):
        """Test that 'date' keyword produces WARNING."""
        result = ValidationResult()
        validator._validate_reserved_keywords("date", "Column", result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 1
        assert "SQL reserved keyword" in warnings[0].message

    def test_case_insensitive_keyword_detection(self, validator):
        """Test that keyword detection is case-insensitive."""
        result = ValidationResult()
        validator._validate_reserved_keywords("Select", "Metric", result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 1

    def test_relationship_reserved_keyword_integration(self, validator):
        """Test that a relationship with reserved keyword name produces warning in full validation."""
        semantic_data = {
            'relationships': {
                'items': [{
                    'name': 'join',  # Reserved keyword
                    'left_table': 'orders',
                    'right_table': 'customers',
                    'relationship_conditions': [
                        "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
                    ]
                }]
            }
        }
        result = validator.validate(semantic_data)
        warnings = [i.message for i in result.issues if i.severity.name == 'WARNING']
        assert any('SQL reserved keyword' in w for w in warnings)


class TestCombinedIdentifierValidation:
    """Test combined identifier validation using _validate_identifier helper."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_validate_identifier_runs_all_checks(self, validator):
        """Test that _validate_identifier runs all three validation checks."""
        # Name that triggers all three validations
        # - Too long (>200 chars) for warning
        # - Contains invalid characters (dash)
        # - Is a reserved keyword (would need to be, but let's test separately)
        
        # Test with dash (invalid char)
        result = ValidationResult()
        validator._validate_identifier("metric-name", "Metric", result)
        errors = [i for i in result.issues if i.severity.name == 'ERROR']
        assert len(errors) >= 1
        assert any("invalid characters" in e.message for e in errors)

    def test_validate_identifier_with_reserved_keyword(self, validator):
        """Test that _validate_identifier catches reserved keywords."""
        result = ValidationResult()
        validator._validate_identifier("select", "Metric", result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) >= 1
        assert any("SQL reserved keyword" in w.message for w in warnings)

    def test_semantic_view_with_invalid_name(self, validator):
        """Test that semantic view with invalid name is caught."""
        semantic_data = {
            'semantic_views': {
                'items': [{
                    'name': '123_invalid-view.name',  # Multiple issues
                    'description': 'Test view',
                    'tables': ['orders']
                }]
            }
        }
        result = validator.validate(semantic_data)
        errors = [i.message for i in result.issues if i.severity.name == 'ERROR']
        assert any('invalid characters' in e for e in errors)


class TestExpressionValidation:
    """Test expression validation (Issue #26 - syntax validation).
    
    Note: Data type and column type validation is handled by DbtModelValidator
    in dbt_models.py (Issues #28, #29). Empty expression validation (Issue #27)
    is done inline in _validate_metrics and _validate_filters.
    """

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    # Expression syntax tests (Issue #26)
    def test_balanced_parentheses_passes(self, validator):
        """Test that balanced parentheses pass."""
        result = ValidationResult()
        validator._validate_expression_syntax("SUM(order_total)", "test_metric", "Metric", result)
        assert result.error_count == 0

    def test_unbalanced_parentheses_extra_close(self, validator):
        """Test that extra closing parenthesis produces error."""
        result = ValidationResult()
        validator._validate_expression_syntax("SUM(x))", "test_metric", "Metric", result)
        assert result.error_count == 1
        assert "unbalanced parentheses" in result.issues[0].message

    def test_unbalanced_parentheses_extra_open(self, validator):
        """Test that extra opening parenthesis produces error."""
        result = ValidationResult()
        validator._validate_expression_syntax("SUM((x)", "test_metric", "Metric", result)
        assert result.error_count == 1
        assert "unbalanced parentheses" in result.issues[0].message

    def test_unbalanced_brackets(self, validator):
        """Test that unbalanced brackets produce error."""
        result = ValidationResult()
        validator._validate_expression_syntax("arr[1", "test_metric", "Metric", result)
        assert result.error_count == 1
        assert "unbalanced brackets" in result.issues[0].message

    def test_unbalanced_braces(self, validator):
        """Test that unbalanced braces produce error."""
        result = ValidationResult()
        validator._validate_expression_syntax("{{ column('t', 'c') }", "test_metric", "Metric", result)
        assert result.error_count == 1
        assert "unbalanced braces" in result.issues[0].message

    def test_complex_valid_expression(self, validator):
        """Test that complex but valid expression passes."""
        result = ValidationResult()
        expr = "CASE WHEN {{ column('orders', 'status') }} = 'complete' THEN SUM({{ column('orders', 'amount') }}) ELSE 0 END"
        validator._validate_expression_syntax(expr, "test_metric", "Metric", result)
        assert result.error_count == 0

    def test_empty_expression_passes_syntax_check(self, validator):
        """Test that empty expression passes syntax check (separate validation)."""
        result = ValidationResult()
        validator._validate_expression_syntax("", "test_metric", "Metric", result)
        assert result.error_count == 0  # Empty is handled by _validate_expression_not_empty


class TestFilterParsingHelpers:
    """Test filter parsing helper functions."""

    def test_extract_table_names_from_jinja_single(self):
        """Test extracting single table name from Jinja2 expression."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import _extract_table_names_from_jinja
        
        result = _extract_table_names_from_jinja("{{ column('orders', 'total') }} > 0")
        assert result == ['orders']

    def test_extract_table_names_from_jinja_multiple_same(self):
        """Test extracting multiple references to same table."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import _extract_table_names_from_jinja
        
        result = _extract_table_names_from_jinja("{{ column('orders', 'a') }} AND {{ column('orders', 'b') }}")
        assert result == ['orders']  # Should deduplicate

    def test_extract_table_names_from_jinja_multiple_different(self):
        """Test extracting multiple different table names."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import _extract_table_names_from_jinja
        
        result = _extract_table_names_from_jinja("{{ column('orders', 'a') }} AND {{ column('users', 'b') }}")
        assert 'orders' in result
        assert 'users' in result
        assert len(result) == 2

    def test_extract_table_names_from_jinja_no_match(self):
        """Test that expression without Jinja2 returns empty list."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import _extract_table_names_from_jinja
        
        result = _extract_table_names_from_jinja("total > 0")
        assert result == []

    def test_extract_table_names_from_jinja_double_quotes(self):
        """Test extracting with double quotes."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import _extract_table_names_from_jinja
        
        result = _extract_table_names_from_jinja('{{ column("orders", "total") }} > 0')
        assert result == ['orders']

    def test_extract_table_name_from_template(self):
        """Test extracting table name from table() template."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import _extract_table_name_from_template
        
        result = _extract_table_name_from_template("{{ table('orders') }}")
        assert result == 'orders'

    def test_extract_table_name_from_template_not_template(self):
        """Test that non-template string is returned as-is."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import _extract_table_name_from_template
        
        result = _extract_table_name_from_template("orders")
        assert result == 'orders'


class TestCircularDependencyDetection:
    """Test circular dependency detection for metrics (Issue #33)."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_no_circular_dependency(self, validator):
        """Test that metrics without circular dependencies pass."""
        metrics = [
            {'name': 'metric_a', 'expr': 'SUM(amount)'},
            {'name': 'metric_b', 'expr': "{{ metric('metric_a') }} * 2"},
            {'name': 'metric_c', 'expr': "{{ metric('metric_b') }} + 10"},
        ]
        result = ValidationResult()
        validator._detect_metric_cycles(metrics, result)
        assert result.error_count == 0

    def test_self_reference_detected(self, validator):
        """Test that self-referencing metric is detected."""
        metrics = [
            {'name': 'self_ref', 'expr': "{{ metric('self_ref') }} + 1"},
        ]
        result = ValidationResult()
        validator._detect_metric_cycles(metrics, result)
        assert result.error_count == 1
        assert 'Circular dependency' in result.issues[0].message
        assert 'self_ref' in result.issues[0].message

    def test_simple_cycle_detected(self, validator):
        """Test that A -> B -> A cycle is detected."""
        metrics = [
            {'name': 'metric_a', 'expr': "{{ metric('metric_b') }}"},
            {'name': 'metric_b', 'expr': "{{ metric('metric_a') }}"},
        ]
        result = ValidationResult()
        validator._detect_metric_cycles(metrics, result)
        assert result.error_count == 1
        assert 'Circular dependency' in result.issues[0].message

    def test_complex_cycle_detected(self, validator):
        """Test that A -> B -> C -> A cycle is detected."""
        metrics = [
            {'name': 'metric_a', 'expr': "{{ metric('metric_b') }}"},
            {'name': 'metric_b', 'expr': "{{ metric('metric_c') }}"},
            {'name': 'metric_c', 'expr': "{{ metric('metric_a') }}"},
        ]
        result = ValidationResult()
        validator._detect_metric_cycles(metrics, result)
        assert result.error_count == 1
        assert 'Circular dependency' in result.issues[0].message

    def test_no_metric_references(self, validator):
        """Test that metrics without references pass."""
        metrics = [
            {'name': 'metric_a', 'expr': 'SUM(amount)'},
            {'name': 'metric_b', 'expr': 'COUNT(*)'},
        ]
        result = ValidationResult()
        validator._detect_metric_cycles(metrics, result)
        assert result.error_count == 0

    def test_reference_to_nonexistent_metric(self, validator):
        """Test that reference to non-existent metric doesn't crash."""
        metrics = [
            {'name': 'metric_a', 'expr': "{{ metric('does_not_exist') }}"},
        ]
        result = ValidationResult()
        validator._detect_metric_cycles(metrics, result)
        # Should not error - reference to non-existent metric is different validation
        assert result.error_count == 0


class TestExtractMetricReferences:
    """Test metric reference extraction helper."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_single_reference(self, validator):
        """Test extracting single metric reference."""
        result = validator._extract_metric_references("{{ metric('base_metric') }} * 2")
        assert result == ['base_metric']

    def test_multiple_references(self, validator):
        """Test extracting multiple metric references."""
        result = validator._extract_metric_references(
            "{{ metric('metric_a') }} + {{ metric('metric_b') }}"
        )
        assert 'metric_a' in result
        assert 'metric_b' in result

    def test_uppercase_metric(self, validator):
        """Test that Metric (capitalized) is also detected."""
        result = validator._extract_metric_references("{{ Metric('base_metric') }}")
        assert result == ['base_metric']

    def test_double_quotes(self, validator):
        """Test that double quotes work."""
        result = validator._extract_metric_references('{{ metric("base_metric") }}')
        assert result == ['base_metric']

    def test_no_references(self, validator):
        """Test expression without metric references."""
        result = validator._extract_metric_references("SUM(amount)")
        assert result == []

    def test_empty_expression(self, validator):
        """Test empty expression."""
        result = validator._extract_metric_references("")
        assert result == []


class TestDuplicateNameDetection:
    """Test duplicate name detection for views/filters/relationships (Issue #34)."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_no_duplicates(self, validator):
        """Test that unique names pass."""
        items = [
            {'name': 'view_a'},
            {'name': 'view_b'},
            {'name': 'view_c'},
        ]
        result = ValidationResult()
        validator._check_duplicate_names(items, "semantic view", result)
        assert result.error_count == 0

    def test_duplicate_detected(self, validator):
        """Test that duplicate names are detected."""
        items = [
            {'name': 'my_view'},
            {'name': 'my_view'},
        ]
        result = ValidationResult()
        validator._check_duplicate_names(items, "semantic view", result)
        assert result.error_count == 1
        assert 'Duplicate semantic view' in result.issues[0].message

    def test_case_insensitive(self, validator):
        """Test that duplicate detection is case-insensitive."""
        items = [
            {'name': 'MyView'},
            {'name': 'MYVIEW'},
        ]
        result = ValidationResult()
        validator._check_duplicate_names(items, "filter", result)
        assert result.error_count == 1

    def test_empty_names_skipped(self, validator):
        """Test that empty names are skipped."""
        items = [
            {'name': ''},
            {'name': ''},
        ]
        result = ValidationResult()
        validator._check_duplicate_names(items, "relationship", result)
        assert result.error_count == 0


class TestRelationshipStructureValidation:
    """Test relationship structure validation (Issues #37, #38, #39)."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_valid_relationship(self, validator):
        """Test that valid relationship passes."""
        relationship = {
            'name': 'orders_to_customers',
            'left_table': 'orders',
            'right_table': 'customers',
            'relationship_conditions': ["col1 = col2"],
        }
        result = ValidationResult()
        validator._validate_relationship_structure(relationship, result)
        assert result.error_count == 0
        assert len([i for i in result.issues if i.severity.name == 'WARNING']) == 0

    def test_unknown_field_warning(self, validator):
        """Test that unknown field produces warning."""
        relationship = {
            'name': 'orders_to_customers',
            'left_table': 'orders',
            'right_table': 'customers',
            'relationship_conditions': ["col1 = col2"],
            'join_typ': 'left',  # Typo: should be join_type
        }
        result = ValidationResult()
        validator._validate_relationship_structure(relationship, result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 1
        assert 'unknown field' in warnings[0].message
        assert 'join_typ' in warnings[0].message

    def test_multiple_unknown_fields(self, validator):
        """Test that multiple unknown fields each produce warning."""
        relationship = {
            'name': 'rel',
            'left_table': 'a',
            'right_table': 'b',
            'relationship_conditions': ["x = y"],
            'unknown1': 'value',
            'unknown2': 'value',
        }
        result = ValidationResult()
        validator._validate_relationship_structure(relationship, result)
        warnings = [i for i in result.issues if i.severity.name == 'WARNING']
        assert len(warnings) == 2


