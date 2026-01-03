"""
Test DbtModelValidator

Tests the validation logic for dbt models, including column_type validation.
"""

import pytest

from snowflake_semantic_tools.core.models.validation import ValidationSeverity
from snowflake_semantic_tools.core.validation.rules.dbt_models import DbtModelValidator


class TestDbtModelValidator:
    """Test DbtModelValidator validation logic."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = DbtModelValidator()

    def test_valid_column_types(self):
        """Test that valid column_type values pass validation."""
        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "TEST_TABLE",
                    "database": "ANALYTICS",
                    "schema": "TEST",
                    "primary_key": ["id"],
                    "cortex_searchable": True,
                }
            ],
            "sm_dimensions": {
                "items": [
                    {
                        "table_name": "TEST_TABLE",
                        "name": "dimension_col",
                        "column_type": "dimension",
                        "data_type": "text",
                    }
                ]
            },
            "sm_facts": {
                "items": [
                    {"table_name": "TEST_TABLE", "name": "fact_col", "column_type": "fact", "data_type": "number"}
                ]
            },
            "sm_time_dimensions": {
                "items": [
                    {
                        "table_name": "TEST_TABLE",
                        "name": "time_col",
                        "column_type": "time_dimension",
                        "data_type": "timestamp_ntz",
                    }
                ]
            },
        }

        result = self.validator.validate(dbt_data)

        # Should not have any errors related to column_type
        column_type_errors = [
            issue
            for issue in result.issues
            if issue.severity == ValidationSeverity.ERROR and "invalid column_type" in issue.message
        ]
        assert len(column_type_errors) == 0

    def test_invalid_column_type_dimenson(self):
        """Test that 'dimenson' typo is caught as an error."""
        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "TEST_TABLE",
                    "database": "ANALYTICS",
                    "schema": "TEST",
                    "primary_key": ["id"],
                    "cortex_searchable": True,
                }
            ],
            "sm_dimensions": {
                "items": [
                    {
                        "table_name": "TEST_TABLE",
                        "name": "bad_col",
                        "column_type": "dimenson",  # Invalid typo
                        "data_type": "text",
                    }
                ]
            },
        }

        result = self.validator.validate(dbt_data)

        # Should have an error about invalid column_type
        column_type_errors = [
            issue
            for issue in result.issues
            if issue.severity == ValidationSeverity.ERROR
            and "invalid column_type" in issue.message
            and "dimenson" in issue.message
        ]
        assert len(column_type_errors) == 1
        assert "Must be one of: dimension, fact, time_dimension" in column_type_errors[0].message

    def test_invalid_column_type_various_typos(self):
        """Test various column_type typos are caught."""
        test_cases = [
            ("dimensoin", "dimension typo"),
            ("fac", "fact typo"),
            ("tim_dimension", "time_dimension typo"),
            ("measure", "old measure term"),
            ("metric", "old metric term"),
            ("time", "old time term"),
        ]

        for invalid_type, description in test_cases:
            dbt_data = {
                "sm_tables": [
                    {
                        "table_name": "TEST_TABLE",
                        "database": "ANALYTICS",
                        "schema": "TEST",
                        "primary_key": ["id"],
                        "cortex_searchable": True,
                    }
                ],
                "sm_dimensions": {
                    "items": [
                        {
                            "table_name": "TEST_TABLE",
                            "name": "bad_col",
                            "column_type": invalid_type,
                            "data_type": "text",
                        }
                    ]
                },
            }

            result = self.validator.validate(dbt_data)

            # Should have an error about invalid column_type
            column_type_errors = [
                issue
                for issue in result.issues
                if issue.severity == ValidationSeverity.ERROR
                and "invalid column_type" in issue.message
                and invalid_type in issue.message
            ]
            assert len(column_type_errors) == 1, f"Failed to catch {description}: {invalid_type}"

    def test_empty_column_type_not_validated(self):
        """Test that empty/missing column_type doesn't trigger the invalid type error."""
        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "TEST_TABLE",
                    "database": "ANALYTICS",
                    "schema": "TEST",
                    "primary_key": ["id"],
                    "cortex_searchable": True,
                }
            ],
            "sm_dimensions": {
                "items": [
                    {"table_name": "TEST_TABLE", "name": "col_no_type", "column_type": "", "data_type": "text"}  # Empty
                ]
            },
        }

        result = self.validator.validate(dbt_data)

        # Should not have invalid column_type error (empty is handled elsewhere)
        column_type_errors = [
            issue
            for issue in result.issues
            if issue.severity == ValidationSeverity.ERROR and "invalid column_type" in issue.message
        ]
        assert len(column_type_errors) == 0

    def test_valid_column_types_constant(self):
        """Test that VALID_COLUMN_TYPES constant is correct."""
        expected_types = {"dimension", "fact", "time_dimension"}
        assert self.validator.VALID_COLUMN_TYPES == expected_types

    def test_column_type_error_context(self):
        """Test that column_type errors include proper context."""
        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "MY_TABLE",
                    "database": "ANALYTICS",
                    "schema": "TEST",
                    "primary_key": ["id"],
                    "cortex_searchable": True,
                }
            ],
            "sm_dimensions": {
                "items": [
                    {"table_name": "MY_TABLE", "name": "my_column", "column_type": "invalid_type", "data_type": "text"}
                ]
            },
        }

        result = self.validator.validate(dbt_data)

        column_type_errors = [
            issue
            for issue in result.issues
            if issue.severity == ValidationSeverity.ERROR and "invalid column_type" in issue.message
        ]

        assert len(column_type_errors) == 1
        error = column_type_errors[0]

        # Check error message contains table and column names
        assert "MY_TABLE" in error.message
        assert "my_column" in error.message
        assert "invalid_type" in error.message

        # Check context
        assert error.context["table"] == "MY_TABLE"
        assert error.context["column"] == "my_column"
        assert error.context["column_type"] == "invalid_type"
        assert error.context["level"] == "column"
