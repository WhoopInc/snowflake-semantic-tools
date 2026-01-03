"""
Unit tests for relationship reference validation.

Tests cover:
- SQL transformations in column references
- Missing column references
- Missing table references
- Case sensitivity handling
- Composite key validation
"""

from pathlib import Path

import pytest

from snowflake_semantic_tools.core.models.validation import ValidationResult
from snowflake_semantic_tools.core.validation.rules.references import ReferenceValidator


class TestRelationshipReferenceValidation:
    """Test relationship reference validation."""

    @pytest.fixture
    def validator(self):
        """Create a reference validator instance."""
        return ReferenceValidator()

    @pytest.fixture
    def sample_dbt_catalog(self):
        """Create a sample dbt catalog for testing."""
        return {
            "orders": {
                "database": "analytics",
                "schema": "sales",
                "primary_key": "id",
                "columns": {
                    "id": {"data_type": "NUMBER"},
                    "user_id": {"data_type": "NUMBER"},
                    "product_id": {"data_type": "NUMBER"},
                    "created_date": {"data_type": "DATE"},
                    "created_at": {"data_type": "TIMESTAMP"},
                    "total_amount": {"data_type": "NUMBER"},
                },
            },
            "users": {
                "database": "analytics",
                "schema": "users",
                "primary_key": "id",
                "columns": {
                    "id": {"data_type": "NUMBER"},
                    "email": {"data_type": "VARCHAR"},
                    "registration_date": {"data_type": "DATE"},
                    "created_at": {"data_type": "TIMESTAMP"},
                },
            },
            "products": {
                "database": "analytics",
                "schema": "products",
                "primary_key": "id",
                "columns": {
                    "id": {"data_type": "NUMBER"},
                    "name": {"data_type": "VARCHAR"},
                    "sku": {"data_type": "VARCHAR"},
                },
            },
        }

    def test_valid_relationship_passes(self, validator, sample_dbt_catalog):
        """Test that valid relationships pass validation."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "ORDERS_TO_USERS",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                        "join_type": "LEFT_OUTER",
                        "relationship_type": "MANY_TO_ONE",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "ORDERS_TO_USERS",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "USERS.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        # Should have no errors for valid relationship
        assert result.error_count == 0

    def test_sql_transformation_type_casting(self, validator, sample_dbt_catalog):
        """Test that :: type casting in column reference is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_CASTING",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_CASTING",
                        "left_column": "ORDERS.CREATED_DATE",
                        "right_column": "USERS.REGISTRATION_DATE::DATE",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("type casting" in str(error).lower() for error in errors)
        assert any("::" in str(error) for error in errors)

    def test_sql_transformation_cast_function(self, validator, sample_dbt_catalog):
        """Test that CAST function in column reference is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_CAST",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_CAST",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "CAST(ID AS VARCHAR)",  # No table prefix
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("cast function" in str(error).lower() for error in errors)

    def test_sql_transformation_to_date(self, validator, sample_dbt_catalog):
        """Test that TO_DATE function in column reference is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_TO_DATE",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_TO_DATE",
                        "left_column": "ORDERS.CREATED_DATE",
                        "right_column": "TO_DATE(REGISTRATION_DATE)",  # No table prefix
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("to_date function" in str(error).lower() for error in errors)

    def test_sql_transformation_coalesce(self, validator, sample_dbt_catalog):
        """Test that COALESCE function in column reference is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_COALESCE",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_COALESCE",
                        "left_column": "COALESCE(USER_ID, 0)",  # No table prefix
                        "right_column": "USERS.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("coalesce function" in str(error).lower() for error in errors)

    def test_sql_transformation_trim(self, validator, sample_dbt_catalog):
        """Test that TRIM function in column reference is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_TRIM",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_TRIM",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "TRIM(EMAIL)",  # No table prefix
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("trim function" in str(error).lower() for error in errors)

    def test_sql_transformation_arithmetic(self, validator, sample_dbt_catalog):
        """Test that arithmetic operations in column reference are caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_ARITHMETIC",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_ARITHMETIC",
                        "left_column": "ORDERS.USER_ID + 1",
                        "right_column": "USERS.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("arithmetic operation" in str(error).lower() for error in errors)

    def test_sql_transformation_case_statement(self, validator, sample_dbt_catalog):
        """Test that CASE statements in column reference are caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_CASE",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_CASE",
                        "left_column": "CASE WHEN USER_ID > 0 THEN USER_ID ELSE NULL END",  # No table prefix
                        "right_column": "USERS.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("case statement" in str(error).lower() for error in errors)

    def test_missing_left_column(self, validator, sample_dbt_catalog):
        """Test that missing left column is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_MISSING_LEFT",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_MISSING_LEFT",
                        "left_column": "ORDERS.NONEXISTENT_COLUMN",
                        "right_column": "USERS.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("unknown column" in str(error).lower() for error in errors)
        assert any("nonexistent_column" in str(error).lower() for error in errors)

    def test_missing_right_column(self, validator, sample_dbt_catalog):
        """Test that missing right column is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_MISSING_RIGHT",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_MISSING_RIGHT",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "USERS.NONEXISTENT_COLUMN",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("unknown column" in str(error).lower() for error in errors)
        assert any("nonexistent_column" in str(error).lower() for error in errors)

    def test_missing_left_table(self, validator, sample_dbt_catalog):
        """Test that missing left table is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_MISSING_LEFT_TABLE",
                        "left_table_name": "NONEXISTENT_TABLE",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_MISSING_LEFT_TABLE",
                        "left_column": "NONEXISTENT_TABLE.ID",
                        "right_column": "USERS.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("unknown" in str(error).lower() and "table" in str(error).lower() for error in errors)
        assert any("nonexistent_table" in str(error).lower() for error in errors)

    def test_missing_right_table(self, validator, sample_dbt_catalog):
        """Test that missing right table is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_MISSING_RIGHT_TABLE",
                        "left_table_name": "ORDERS",
                        "right_table_name": "NONEXISTENT_TABLE",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_MISSING_RIGHT_TABLE",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "NONEXISTENT_TABLE.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("unknown" in str(error).lower() and "table" in str(error).lower() for error in errors)
        assert any("nonexistent_table" in str(error).lower() for error in errors)

    def test_case_sensitivity_table_names(self, validator, sample_dbt_catalog):
        """Test that table name case sensitivity is handled correctly."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_CASE_SENSITIVITY",
                        "left_table_name": "ORDERS",  # Uppercase
                        "right_table_name": "USERS",  # Uppercase
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_CASE_SENSITIVITY",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "USERS.ID",
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        # Should handle case insensitivity and pass
        assert result.error_count == 0

    def test_case_sensitivity_column_names(self, validator, sample_dbt_catalog):
        """Test that column name case sensitivity is handled correctly."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_COLUMN_CASE",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_COLUMN_CASE",
                        "left_column": "ORDERS.USER_ID",  # Lowercase in catalog
                        "right_column": "USERS.ID",  # Lowercase in catalog
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        # Should handle case insensitivity and pass
        assert result.error_count == 0

    def test_composite_key_validation(self, validator, sample_dbt_catalog):
        """Test that composite key relationships are validated correctly."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_COMPOSITE_KEY",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_COMPOSITE_KEY",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "USERS.ID",
                    },
                    {
                        "relationship_name": "TEST_COMPOSITE_KEY",
                        "left_column": "ORDERS.CREATED_DATE",
                        "right_column": "USERS.REGISTRATION_DATE",
                    },
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        # Should pass with valid composite key
        assert result.error_count == 0

    def test_composite_key_with_missing_column(self, validator, sample_dbt_catalog):
        """Test that composite key with missing column is caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_COMPOSITE_MISSING",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_COMPOSITE_MISSING",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "USERS.ID",
                    },
                    {
                        "relationship_name": "TEST_COMPOSITE_MISSING",
                        "left_column": "ORDERS.NONEXISTENT_COLUMN",
                        "right_column": "USERS.REGISTRATION_DATE",
                    },
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        assert result.error_count >= 1
        errors = result.get_errors()
        assert any("unknown column" in str(error).lower() for error in errors)
        assert any("nonexistent_column" in str(error).lower() for error in errors)

    def test_multiple_transformations_in_one_relationship(self, validator, sample_dbt_catalog):
        """Test that multiple transformation types are all caught."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "TEST_MULTIPLE_TRANSFORMATIONS",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "TEST_MULTIPLE_TRANSFORMATIONS",
                        "left_column": "ORDERS.USER_ID::VARCHAR",
                        "right_column": "USERS.ID",
                    },
                    {
                        "relationship_name": "TEST_MULTIPLE_TRANSFORMATIONS",
                        "left_column": "ORDERS.TOTAL_AMOUNT",
                        "right_column": "USERS.ID + 100",
                    },
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        # Should catch both transformations
        assert result.error_count >= 2
        errors = result.get_errors()
        assert any("type casting" in str(error).lower() for error in errors)
        assert any("arithmetic operation" in str(error).lower() for error in errors)

    def test_right_column_not_primary_key_warning(self, validator, sample_dbt_catalog):
        """Test that relationship referencing non-primary-key column produces a warning."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "USERS_TO_ORDERS",
                        "left_table_name": "USERS",
                        "right_table_name": "ORDERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "USERS_TO_ORDERS",
                        "left_column": "USERS.ID",
                        "right_column": "ORDERS.USER_ID",  # NOT the primary key (pk is 'id')
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        # Should have ERROR (upgraded from WARNING) since relationships must reference primary keys
        # Even though UNIQUE constraints are valid, we enforce this strictly to catch bugs early
        assert result.error_count == 1
        errors = result.get_errors()
        error_msg = str(errors[0]).lower()
        assert "primary key" in error_msg
        assert "unique" in error_msg  # Still mentions UNIQUE constraint as valid option
        assert "user_id" in error_msg
        assert "id" in error_msg  # The actual primary key

    def test_right_column_is_primary_key_passes(self, validator, sample_dbt_catalog):
        """Test that relationship correctly referencing primary key passes."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "ORDERS_TO_USERS",
                        "left_table_name": "ORDERS",
                        "right_table_name": "USERS",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "ORDERS_TO_USERS",
                        "left_column": "ORDERS.USER_ID",
                        "right_column": "USERS.ID",  # IS the primary key ✓
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, sample_dbt_catalog)

        # Should have NO errors
        assert result.error_count == 0

    def test_composite_primary_key_partial_reference_warning(self, validator):
        """Test that referencing only part of a composite primary key produces a warning."""
        catalog_with_composite = {
            "membership_status_daily": {
                "database": "analytics",
                "schema": "memberships",
                "primary_key": ["user_id", "calendar_date"],  # Composite key
                "columns": {
                    "user_id": {"data_type": "NUMBER"},
                    "calendar_date": {"data_type": "DATE"},
                    "status": {"data_type": "VARCHAR"},
                },
            },
            "ai_costs": {
                "database": "analytics",
                "schema": "ai_coach",
                "primary_key": "primary_key",
                "columns": {
                    "primary_key": {"data_type": "VARCHAR"},
                    "start_date": {"data_type": "DATE"},
                    "amount": {"data_type": "NUMBER"},
                },
            },
        }

        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "AI_COSTS_TO_MEMBERSHIP",
                        "left_table_name": "AI_COSTS",
                        "right_table_name": "MEMBERSHIP_STATUS_DAILY",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "AI_COSTS_TO_MEMBERSHIP",
                        "left_column": "AI_COSTS.START_DATE",
                        "right_column": "MEMBERSHIP_STATUS_DAILY.CALENDAR_DATE",  # Only part of composite key
                    }
                ],
            }
        }

        result = validator.validate(semantic_data, catalog_with_composite)

        # Should have ERROR (upgraded from WARNING) since relationships must reference primary keys
        # Even though UNIQUE constraints are valid, we enforce this strictly to catch bugs early
        assert result.error_count == 1
        errors = result.get_errors()
        error_msg = str(errors[0]).lower()
        assert "composite" in error_msg
        assert "unique" in error_msg  # Still mentions UNIQUE constraint as valid option
        assert "user_id" in error_msg
        assert "calendar_date" in error_msg
        assert "missing" in error_msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
