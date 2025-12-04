"""
Comprehensive tests for SemanticModelValidator.

Tests all validation rules for metrics, relationships, filters,
custom instructions, verified queries, and semantic views.
"""

import pytest
from snowflake_semantic_tools.core.validation.rules.semantic_models import SemanticModelValidator


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
    
    def test_metric_invalid_aggregation_warning(self, validator):
        """Test that metric with invalid aggregation produces WARNING."""
        semantic_data = {
            'metrics': {
                'items': [{
                    'name': 'total_revenue',
                    'expr': 'SUM(amount)',
                    'tables': ['orders'],
                    'default_aggregation': 'invalid_agg'
                }]
            }
        }
        
        result = validator.validate(semantic_data)
        warnings = [i.message for i in result.issues if i.severity.name == 'WARNING']
        assert any('unrecognized default_aggregation' in w for w in warnings)
    
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
