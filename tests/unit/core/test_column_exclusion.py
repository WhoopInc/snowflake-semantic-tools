"""
Unit tests for column-level exclusion (meta.sst.exclude: true).
Issue #125: Hide columns from semantic view generation.
"""

from pathlib import Path

import pytest

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.core.parsing.parsers.data_extractors import extract_column_info
from snowflake_semantic_tools.core.parsing.parsers.dbt_parser import parse_single_model
from snowflake_semantic_tools.core.validation.rules.dbt_models import DbtModelValidator


class TestExtractionFiltersExcludedColumns:
    """Excluded columns should not appear in SM_DIMENSIONS/SM_FACTS/SM_TIME_DIMENSIONS."""

    def test_excluded_dimension_not_extracted(self):
        model = {
            "name": "orders",
            "config": {"meta": {"sst": {"database": "DB", "schema": "SCH", "primary_key": "order_id"}}},
            "columns": [
                {
                    "name": "order_id",
                    "config": {"meta": {"sst": {"column_type": "dimension", "data_type": "TEXT"}}},
                },
                {
                    "name": "internal_hash",
                    "config": {"meta": {"sst": {"column_type": "dimension", "data_type": "TEXT", "exclude": True}}},
                },
            ],
        }
        from snowflake_semantic_tools.core.parsing.parsers.error_handler import ErrorTracker

        result = parse_single_model(model, Path("/tmp/test.yml"), ErrorTracker())
        dim_names = [d["name"] for d in result["sm_dimensions"]]
        assert "ORDER_ID" in dim_names
        assert "INTERNAL_HASH" not in dim_names

    def test_excluded_fact_not_extracted(self):
        model = {
            "name": "orders",
            "config": {"meta": {"sst": {"database": "DB", "schema": "SCH", "primary_key": "order_id"}}},
            "columns": [
                {
                    "name": "order_total",
                    "config": {"meta": {"sst": {"column_type": "fact", "data_type": "NUMBER"}}},
                },
                {
                    "name": "debug_score",
                    "config": {"meta": {"sst": {"column_type": "fact", "data_type": "NUMBER", "exclude": True}}},
                },
            ],
        }
        from snowflake_semantic_tools.core.parsing.parsers.error_handler import ErrorTracker

        result = parse_single_model(model, Path("/tmp/test.yml"), ErrorTracker())
        fact_names = [f["name"] for f in result["sm_facts"]]
        assert "ORDER_TOTAL" in fact_names
        assert "DEBUG_SCORE" not in fact_names

    def test_non_excluded_column_still_extracted(self):
        model = {
            "name": "orders",
            "config": {"meta": {"sst": {"database": "DB", "schema": "SCH", "primary_key": "order_id"}}},
            "columns": [
                {
                    "name": "col_a",
                    "config": {"meta": {"sst": {"column_type": "dimension", "data_type": "TEXT", "exclude": False}}},
                },
            ],
        }
        from snowflake_semantic_tools.core.parsing.parsers.error_handler import ErrorTracker

        result = parse_single_model(model, Path("/tmp/test.yml"), ErrorTracker())
        dim_names = [d["name"] for d in result["sm_dimensions"]]
        assert "COL_A" in dim_names


class TestValidationSkipsExcludedColumns:
    """Excluded columns should not trigger missing column_type errors."""

    @pytest.fixture
    def validator(self):
        return DbtModelValidator()

    def test_excluded_column_without_column_type_no_error(self, validator):
        columns = [
            {
                "name": "email",
                "exclude": True,
                "source_file": "/tmp/test.yml",
            }
        ]
        result = ValidationResult()
        for col in columns:
            validator._validate_column(col, "users", result)
        errors = result.get_errors()
        assert len(errors) == 0

    def test_excluded_column_with_column_type_warns(self, validator):
        columns = [
            {
                "name": "email",
                "exclude": True,
                "column_type": "dimension",
                "source_file": "/tmp/test.yml",
            }
        ]
        result = ValidationResult()
        for col in columns:
            validator._validate_column(col, "users", result)
        warnings = [i for i in result.issues if i.severity.name == "WARNING"]
        assert len(warnings) == 1
        assert "V047" in warnings[0].rule_id

    def test_non_excluded_column_still_validated(self, validator):
        columns = [
            {
                "name": "bad_col",
                "source_file": "/tmp/test.yml",
            }
        ]
        result = ValidationResult()
        for col in columns:
            validator._validate_column(col, "users", result)
        errors = result.get_errors()
        assert any("column_type" in str(e.message) for e in errors)


class TestExtractColumnInfoIncludesExcludeField:
    """extract_column_info should pass through the exclude flag."""

    def test_exclude_true_in_record(self):
        column = {
            "name": "secret_col",
            "config": {"meta": {"sst": {"column_type": "dimension", "data_type": "TEXT", "exclude": True}}},
        }
        record = extract_column_info(column, "my_table", Path("/tmp/test.yml"))
        assert record["exclude"] is True

    def test_exclude_false_by_default(self):
        column = {
            "name": "normal_col",
            "config": {"meta": {"sst": {"column_type": "dimension", "data_type": "TEXT"}}},
        }
        record = extract_column_info(column, "my_table", Path("/tmp/test.yml"))
        assert record["exclude"] is False
