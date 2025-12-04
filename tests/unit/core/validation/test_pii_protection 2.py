"""
Test PII Protection Validation

Critical security tests to ensure PII columns cannot have sample values exposed.
"""

import pytest
from snowflake_semantic_tools.core.validation.rules import DbtModelValidator
from snowflake_semantic_tools.core.models import ValidationResult


class TestPIIProtection:
    """Test PII protection validation rules."""
    
    @pytest.fixture
    def validator(self):
        """Create a DbtModelValidator instance."""
        return DbtModelValidator()
    
    def test_direct_identifier_with_sample_values_produces_error(self, validator):
        """Test that direct_identifier columns with sample_values produce ERROR."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'USERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['USER_ID'],
                    'description': 'User information table'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'USERS',
                        'name': 'EMAIL',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User email address',
                        'privacy_category': 'direct_identifier',
                        'sample_values': ['user@example.com', 'test@test.com']  # VIOLATION!
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should have at least one error
        assert result.has_errors, "Expected ERROR for PII column with sample_values"
        
        # Check that the error message mentions PII exposure
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        pii_errors = [msg for msg in error_messages if 'direct_identifier' in msg and 'sample_values' in msg]
        
        assert len(pii_errors) > 0, "Expected specific error about PII exposure"
        assert 'PII columns must not expose sample data' in pii_errors[0]
    
    def test_direct_identifier_without_sample_values_passes(self, validator):
        """Test that direct_identifier columns WITHOUT sample_values pass validation."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'USERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['USER_ID'],
                    'description': 'User information table'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'USERS',
                        'name': 'EMAIL',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User email address',
                        'privacy_category': 'direct_identifier',
                        'sample_values': []  # Correctly protected
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should not have errors about PII
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        pii_errors = [msg for msg in error_messages if 'direct_identifier' in msg and 'sample_values' in msg]
        
        assert len(pii_errors) == 0, "Should not have PII errors when sample_values is empty"
    
    def test_non_pii_column_with_sample_values_passes(self, validator):
        """Test that non-PII columns can have sample_values."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'USERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['USER_ID'],
                    'description': 'User information table'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'USERS',
                        'name': 'COUNTRY',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User country',
                        # No privacy_category - this is public data
                        'sample_values': ['USA', 'UK', 'Canada']  # OK for non-PII
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should not have errors about PII
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        pii_errors = [msg for msg in error_messages if 'direct_identifier' in msg]
        
        assert len(pii_errors) == 0, "Non-PII columns should be allowed to have sample_values"
    
    def test_quasi_identifier_with_sample_values_allowed(self, validator):
        """Test that quasi_identifier (not direct_identifier) can have sample_values."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'USERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['USER_ID'],
                    'description': 'User information table'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'USERS',
                        'name': 'ZIP_CODE',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User ZIP code',
                        'privacy_category': 'quasi_identifier',  # Less sensitive than direct_identifier
                        'sample_values': ['02134', '90210', '10001']  # OK for quasi-identifiers
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should not have errors about PII - only direct_identifier is blocked
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        pii_errors = [msg for msg in error_messages if 'privacy_category' in msg and 'sample_values' in msg]
        
        assert len(pii_errors) == 0, "quasi_identifier columns should be allowed to have sample_values"
    
    def test_multiple_pii_columns_all_caught(self, validator):
        """Test that multiple PII violations are all caught."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'USERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['USER_ID'],
                    'description': 'User information table'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'USERS',
                        'name': 'EMAIL',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User email',
                        'privacy_category': 'direct_identifier',
                        'sample_values': ['user@example.com']  # VIOLATION 1
                    },
                    {
                        'table_name': 'USERS',
                        'name': 'PHONE',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User phone number',
                        'privacy_category': 'direct_identifier',
                        'sample_values': ['555-1234']  # VIOLATION 2
                    },
                    {
                        'table_name': 'USERS',
                        'name': 'SSN',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'Social security number',
                        'privacy_category': 'direct_identifier',
                        'sample_values': ['123-45-6789']  # VIOLATION 3
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should have 3 errors - one for each PII violation
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        pii_errors = [msg for msg in error_messages if 'direct_identifier' in msg and 'sample_values' in msg]
        
        assert len(pii_errors) == 3, f"Expected 3 PII errors, got {len(pii_errors)}"
        
        # Check that all three columns are mentioned
        all_errors_text = ' '.join(pii_errors)
        assert 'EMAIL' in all_errors_text
        assert 'PHONE' in all_errors_text
        assert 'SSN' in all_errors_text


class TestEnumValidation:
    """Test enum validation rules for fact and time_dimension columns."""
    
    @pytest.fixture
    def validator(self):
        """Create a DbtModelValidator instance."""
        return DbtModelValidator()
    
    def test_fact_column_with_is_enum_true_produces_error(self, validator):
        """Test that fact columns with is_enum=true produce ERROR."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'ORDERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['ORDER_ID'],
                    'description': 'Order transactions'
                }
            ],
            'sm_dimensions': {'items': []},
            'sm_facts': {
                'items': [
                    {
                        'table_name': 'ORDERS',
                        'name': 'AMOUNT',
                        'column_type': 'fact',
                        'data_type': 'number',
                        'description': 'Order amount',
                        'sample_values': ['0', '100', '200'],
                        'is_enum': True  # VIOLATION! Facts should never be enums
                    }
                ]
            },
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should have at least one error
        assert result.has_errors, "Expected ERROR for fact column with is_enum=true"
        
        # Check that the error message mentions enum type mismatch
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        enum_errors = [msg for msg in error_messages if 'is_enum=true' in msg and 'fact' in msg]
        
        assert len(enum_errors) > 0, "Expected specific error about fact enum mismatch"
        assert 'should never be enums' in enum_errors[0]
    
    def test_time_dimension_with_is_enum_true_produces_error(self, validator):
        """Test that time_dimension columns with is_enum=true produce ERROR."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'ORDERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['ORDER_ID'],
                    'description': 'Order transactions'
                }
            ],
            'sm_dimensions': {'items': []},
            'sm_facts': {'items': []},
            'sm_time_dimensions': {
                'items': [
                    {
                        'table_name': 'ORDERS',
                        'name': 'ORDER_DATE',
                        'column_type': 'time_dimension',
                        'data_type': 'date',
                        'description': 'Order date',
                        'sample_values': ['2025-01-01', '2025-01-02'],
                        'is_enum': True  # VIOLATION! Time dimensions should never be enums
                    }
                ]
            }
        }
        
        result = validator.validate(dbt_data)
        
        # Should have at least one error
        assert result.has_errors, "Expected ERROR for time_dimension column with is_enum=true"
        
        # Check that the error message mentions enum type mismatch
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        enum_errors = [msg for msg in error_messages if 'is_enum=true' in msg and 'time_dimension' in msg]
        
        assert len(enum_errors) > 0, "Expected specific error about time_dimension enum mismatch"
        assert 'should never be enums' in enum_errors[0]
    
    def test_dimension_with_is_enum_true_passes(self, validator):
        """Test that dimension columns CAN have is_enum=true."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'ORDERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['ORDER_ID'],
                    'description': 'Order transactions'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'ORDERS',
                        'name': 'STATUS',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'Order status',
                        'sample_values': ['pending', 'shipped', 'delivered'],
                        'is_enum': True  # OK for dimensions!
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should not have enum type mismatch errors
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        enum_errors = [msg for msg in error_messages if 'is_enum=true' in msg and 'should never be enums' in msg]
        
        assert len(enum_errors) == 0, "Dimension columns should be allowed to have is_enum=true"
    
    def test_fact_with_is_enum_false_passes(self, validator):
        """Test that fact columns with is_enum=false pass validation."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'ORDERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['ORDER_ID'],
                    'description': 'Order transactions'
                }
            ],
            'sm_dimensions': {'items': []},
            'sm_facts': {
                'items': [
                    {
                        'table_name': 'ORDERS',
                        'name': 'AMOUNT',
                        'column_type': 'fact',
                        'data_type': 'number',
                        'description': 'Order amount',
                        'sample_values': ['0', '100', '200'],
                        'is_enum': False  # Correct for facts
                    }
                ]
            },
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should not have enum errors
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        enum_errors = [msg for msg in error_messages if 'is_enum' in msg and 'fact' in msg]
        
        assert len(enum_errors) == 0, "Fact columns with is_enum=false should pass"


class TestJinjaSanitization:
    """Test Jinja character sanitization validation rules."""
    
    @pytest.fixture
    def validator(self):
        """Create a DbtModelValidator instance."""
        return DbtModelValidator()
    
    def test_sample_values_with_jinja_characters_produces_error(self, validator):
        """Test that sample values containing Jinja characters produce ERROR."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'AI_MESSAGES',
                    'database': 'ANALYTICS',
                    'schema': 'AI_COACH',
                    'primary_key': ['MESSAGE_ID'],
                    'description': 'AI coach messages'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'AI_MESSAGES',
                        'name': 'QUERY',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'AI query',
                        'sample_values': [
                            '{{@aiql query data}}',  # Jinja breaking
                            '{%- set var = value -%}',  # Jinja breaking
                            'normal value'
                        ]
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should have error about Jinja characters
        assert result.has_errors, "Expected ERROR for sample values with Jinja characters"
        
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        jinja_errors = [msg for msg in error_messages if 'Jinja template characters' in msg]
        
        assert len(jinja_errors) > 0, "Expected specific error about Jinja characters"
        assert 'will break dbt compilation' in jinja_errors[0]
        assert 'sst enrich' in jinja_errors[0]
    
    def test_sample_values_without_jinja_characters_passes(self, validator):
        """Test that normal sample values pass validation."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'USERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['USER_ID'],
                    'description': 'User table'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'USERS',
                        'name': 'STATUS',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User status',
                        'sample_values': ['active', 'inactive', 'pending']
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should not have Jinja errors
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        jinja_errors = [msg for msg in error_messages if 'Jinja template characters' in msg]
        
        assert len(jinja_errors) == 0, "Normal sample values should not trigger Jinja errors"
    
    def test_empty_sample_values_passes(self, validator):
        """Test that empty sample_values list passes validation."""
        dbt_data = {
            'sm_tables': [
                {
                    'table_name': 'USERS',
                    'database': 'ANALYTICS',
                    'schema': 'PUBLIC',
                    'primary_key': ['USER_ID'],
                    'description': 'User table'
                }
            ],
            'sm_dimensions': {
                'items': [
                    {
                        'table_name': 'USERS',
                        'name': 'STATUS',
                        'column_type': 'dimension',
                        'data_type': 'varchar',
                        'description': 'User status',
                        'sample_values': []
                    }
                ]
            },
            'sm_facts': {'items': []},
            'sm_time_dimensions': {'items': []}
        }
        
        result = validator.validate(dbt_data)
        
        # Should not have Jinja errors for empty list
        error_messages = [issue.message for issue in result.issues if issue.severity.name == 'ERROR']
        jinja_errors = [msg for msg in error_messages if 'Jinja template characters' in msg]
        
        assert len(jinja_errors) == 0, "Empty sample_values should not trigger Jinja errors"
