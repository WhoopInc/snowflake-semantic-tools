"""
Tests for SchemaValidator (Issue #23).

Tests the Snowflake schema verification functionality that validates
YAML column definitions against actual Snowflake table schemas.
"""

from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.core.validation.rules.schema_validator import SchemaValidator


class TestSchemaValidatorBasic:
    """Test basic SchemaValidator functionality."""

    @pytest.fixture
    def mock_metadata_manager(self):
        """Create a mock metadata manager."""
        return MagicMock()

    @pytest.fixture
    def validator(self, mock_metadata_manager):
        return SchemaValidator(mock_metadata_manager)

    def test_empty_catalog_returns_empty_result(self, validator):
        """Test that empty catalog returns no issues."""
        result = validator.validate({})
        assert result.error_count == 0
        assert result.warning_count == 0

    def test_table_without_location_skipped(self, validator):
        """Test that tables without database/schema are skipped."""
        catalog = {
            "orders": {
                "name": "orders",
                "database": None,
                "schema": None,
                "columns": {"id": {}, "amount": {}},
            }
        }
        result = validator.validate(catalog)
        # Should skip, not error
        assert result.error_count == 0

    def test_matching_columns_pass(self, validator, mock_metadata_manager):
        """Test that matching columns pass validation."""
        # Mock Snowflake returning same columns
        mock_metadata_manager.get_table_schema.return_value = [
            {"name": "ID"},
            {"name": "AMOUNT"},
            {"name": "STATUS"},
        ]

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}, "amount": {}, "status": {}},
            }
        }

        result = validator.validate(catalog)
        assert result.error_count == 0

    def test_missing_column_error(self, validator, mock_metadata_manager):
        """Test that column not in Snowflake produces error."""
        # Mock Snowflake returning fewer columns
        mock_metadata_manager.get_table_schema.return_value = [
            {"name": "ID"},
            {"name": "AMOUNT"},
            # No STATUS column
        ]

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}, "amount": {}, "status": {}},
            }
        }

        result = validator.validate(catalog)
        assert result.error_count == 1
        assert "status" in result.issues[0].message.lower()
        assert "does not exist" in result.issues[0].message.lower()

    def test_missing_column_with_suggestion(self, validator, mock_metadata_manager):
        """Test that missing column error includes suggestions."""
        # Mock Snowflake returning similar column
        mock_metadata_manager.get_table_schema.return_value = [
            {"name": "ID"},
            {"name": "CUSTOMER_ID"},  # Similar to custmer_id typo
            {"name": "AMOUNT"},
        ]

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}, "custmer_id": {}, "amount": {}},  # Typo
            }
        }

        result = validator.validate(catalog)
        assert result.error_count == 1
        error_msg = result.issues[0].message
        # Should suggest the correct column
        assert "Did you mean" in error_msg
        assert "CUSTOMER_ID" in error_msg

    def test_case_insensitive_matching(self, validator, mock_metadata_manager):
        """Test that column matching is case-insensitive."""
        mock_metadata_manager.get_table_schema.return_value = [
            {"name": "ORDER_ID"},
            {"name": "AMOUNT"},
        ]

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"order_id": {}, "amount": {}},  # Lowercase in YAML
            }
        }

        result = validator.validate(catalog)
        assert result.error_count == 0


class TestSchemaValidatorErrorHandling:
    """Test SchemaValidator error handling."""

    @pytest.fixture
    def mock_metadata_manager(self):
        return MagicMock()

    @pytest.fixture
    def validator(self, mock_metadata_manager):
        return SchemaValidator(mock_metadata_manager)

    def test_table_not_found_error(self, validator, mock_metadata_manager):
        """Test that table not found produces error."""
        mock_metadata_manager.get_table_schema.side_effect = Exception("Object 'ORDERS' does not exist")

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}},
            }
        }

        result = validator.validate(catalog)
        assert result.error_count == 1
        assert "does not exist" in result.issues[0].message

    def test_permission_denied_warning(self, validator, mock_metadata_manager):
        """Test that permission denied produces warning, not error."""
        mock_metadata_manager.get_table_schema.side_effect = Exception("User is not authorized to access the table")

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}},
            }
        }

        result = validator.validate(catalog)
        # Permission issues should be warnings, not errors
        assert result.warning_count == 1
        assert "permission denied" in result.issues[0].message.lower()

    def test_generic_connection_error_warning(self, validator, mock_metadata_manager):
        """Test that generic errors produce warning."""
        mock_metadata_manager.get_table_schema.side_effect = Exception("Network timeout")

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}},
            }
        }

        result = validator.validate(catalog)
        assert result.warning_count == 1
        assert "Could not verify" in result.issues[0].message


class TestSchemaValidatorMissingInYaml:
    """Test optional check for Snowflake columns missing from YAML."""

    @pytest.fixture
    def mock_metadata_manager(self):
        return MagicMock()

    @pytest.fixture
    def validator(self, mock_metadata_manager):
        return SchemaValidator(mock_metadata_manager)

    def test_check_missing_in_yaml_disabled_by_default(self, validator, mock_metadata_manager):
        """Test that missing-in-YAML check is disabled by default."""
        mock_metadata_manager.get_table_schema.return_value = [
            {"name": "ID"},
            {"name": "AMOUNT"},
            {"name": "EXTRA_COLUMN"},  # Not in YAML
        ]

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}, "amount": {}},
            }
        }

        result = validator.validate(catalog, check_missing_in_yaml=False)
        # Should not report missing column as info
        info_issues = [i for i in result.issues if i.severity.name == "INFO"]
        assert len(info_issues) == 0

    def test_check_missing_in_yaml_enabled(self, validator, mock_metadata_manager):
        """Test that missing-in-YAML check reports when enabled."""
        mock_metadata_manager.get_table_schema.return_value = [
            {"name": "ID"},
            {"name": "AMOUNT"},
            {"name": "EXTRA_COLUMN"},  # Not in YAML
        ]

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}, "amount": {}},
            }
        }

        result = validator.validate(catalog, check_missing_in_yaml=True)
        # Should report missing column as info
        info_issues = [i for i in result.issues if i.severity.name == "INFO"]
        assert len(info_issues) == 1
        assert "EXTRA_COLUMN" in info_issues[0].message


class TestFindSimilarColumns:
    """Test the _find_similar_columns helper in SchemaValidator."""

    @pytest.fixture
    def validator(self):
        mock_mm = MagicMock()
        return SchemaValidator(mock_mm)

    def test_prefix_match(self, validator):
        """Test that prefix matching works."""
        columns = {"USER_ID", "USER_NAME", "ORDER_ID"}
        suggestions = validator._find_similar_columns("user_email", columns)
        assert any("USER" in s for s in suggestions)

    def test_suffix_match(self, validator):
        """Test that suffix matching works."""
        columns = {"CREATED_AT", "UPDATED_AT", "USER_ID"}
        suggestions = validator._find_similar_columns("deleted_at", columns)
        assert any("_AT" in s for s in suggestions)

    def test_max_three_suggestions(self, validator):
        """Test that at most 3 suggestions are returned."""
        columns = {"COL_A", "COL_B", "COL_C", "COL_D", "COL_E"}
        suggestions = validator._find_similar_columns("col_x", columns)
        assert len(suggestions) <= 3

    def test_empty_columns_empty_result(self, validator):
        """Test that empty columns returns empty list."""
        suggestions = validator._find_similar_columns("test", set())
        assert suggestions == []


class TestMultipleTablesValidation:
    """Test validation across multiple tables."""

    @pytest.fixture
    def mock_metadata_manager(self):
        return MagicMock()

    @pytest.fixture
    def validator(self, mock_metadata_manager):
        return SchemaValidator(mock_metadata_manager)

    def test_multiple_tables_validated(self, validator, mock_metadata_manager):
        """Test that multiple tables are all validated."""

        def get_schema(table_name, schema, database):
            if table_name.lower() == "orders":
                return [{"name": "ID"}, {"name": "AMOUNT"}]
            elif table_name.lower() == "customers":
                return [{"name": "ID"}, {"name": "NAME"}]
            return []

        mock_metadata_manager.get_table_schema.side_effect = get_schema

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}, "amount": {}},
            },
            "customers": {
                "name": "customers",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}, "name": {}},
            },
        }

        result = validator.validate(catalog)
        assert result.error_count == 0
        # Should have called get_table_schema twice
        assert mock_metadata_manager.get_table_schema.call_count == 2

    def test_one_table_error_doesnt_stop_others(self, validator, mock_metadata_manager):
        """Test that error in one table doesn't prevent validating others."""
        call_count = [0]

        def get_schema(table_name, schema, database):
            call_count[0] += 1
            if table_name.lower() == "orders":
                raise Exception("Table does not exist")
            return [{"name": "ID"}]

        mock_metadata_manager.get_table_schema.side_effect = get_schema

        catalog = {
            "orders": {
                "name": "orders",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}},
            },
            "customers": {
                "name": "customers",
                "database": "DB",
                "schema": "SCHEMA",
                "columns": {"id": {}},
            },
        }

        result = validator.validate(catalog)
        # Should have tried both tables
        assert call_count[0] == 2
        # Orders should have an error
        assert result.error_count == 1
