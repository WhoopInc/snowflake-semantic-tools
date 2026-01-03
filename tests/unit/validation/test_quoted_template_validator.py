"""
Unit tests for QuotedTemplateValidator.

Tests validation of quoted template expressions in semantic models.
"""

import pytest

from snowflake_semantic_tools.core.models.validation import ValidationResult
from snowflake_semantic_tools.core.validation.rules.quoted_templates import QuotedTemplateValidator


class TestQuotedTemplateValidator:
    """Test suite for quoted template expression validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = QuotedTemplateValidator()
        self.result = ValidationResult()

    def test_detects_quoted_column_in_metric_expr(self):
        """Test detection of quoted column template in metric expression."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "test_metric",
                    "tables": ["{{ table('orders') }}"],
                    "expr": "SUM(CASE WHEN \"{{ column('table', 'col') }}\" = 1 THEN 1 END)",
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        assert len(self.result.get_errors()) == 1
        error = self.result.get_errors()[0]
        assert error.context["type"] == "QUOTED_TEMPLATE_EXPRESSION"
        assert "test_metric" in str(error)
        assert "column" in str(error)

    def test_detects_quoted_table_in_metric_tables(self):
        """Test detection of quoted table template in metric tables field."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "test_metric",
                    "tables": ["\"{{ table('orders') }}\""],
                    "expr": "SUM({{ column('orders', 'total') }})",
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        assert len(self.result.get_errors()) == 1
        assert "tables field" in str(self.result.get_errors()[0])
        assert "test_metric" in str(self.result.get_errors()[0])

    def test_detects_quoted_table_in_semantic_view(self):
        """Test detection of quoted table template in semantic view."""
        semantic_views_data = {"semantic_views": [{"name": "test_view", "tables": ["\"{{ table('orders') }}\""]}]}

        self.validator.validate(
            metrics_data={},
            relationships_data={},
            semantic_views_data=semantic_views_data,
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        assert "test_view" in str(self.result.get_errors()[0])

    def test_detects_quoted_templates_in_relationship(self):
        """Test detection in relationship conditions."""
        relationships_data = {
            "snowflake_relationships": [
                {
                    "name": "test_rel",
                    "left_table": "{{ table('t1') }}",
                    "right_table": "{{ table('t2') }}",
                    "relationship_conditions": ["\"{{ column('t1', 'id') }}\" = \"{{ column('t2', 'id') }}\""],
                }
            ]
        }

        self.validator.validate(
            metrics_data={},
            relationships_data=relationships_data,
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        # Should detect both quoted columns
        assert len(self.result.get_errors()) == 2

    def test_detects_quoted_left_right_table_fields(self):
        """Test detection of quoted templates in left_table and right_table fields."""
        relationships_data = {
            "snowflake_relationships": [
                {
                    "name": "test_rel",
                    "left_table": "\"{{ table('orders') }}\"",
                    "right_table": "\"{{ table('customers') }}\"",
                    "relationship_conditions": [
                        "{{ column('orders', 'customer_id') }} = {{ column('customers', 'id') }}"
                    ],
                }
            ]
        }

        self.validator.validate(
            metrics_data={},
            relationships_data=relationships_data,
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        assert len(self.result.get_errors()) == 2  # left_table and right_table
        errors_text = " ".join(str(e) for e in self.result.get_errors())
        assert "left_table" in errors_text
        assert "right_table" in errors_text

    def test_allows_unquoted_templates(self):
        """Test that properly formatted templates pass validation."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "valid_metric",
                    "tables": ["{{ table('orders') }}"],
                    "expr": "SUM(CASE WHEN {{ column('table', 'col') }} = 1 THEN 1 END)",
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert not self.result.has_errors

    def test_ignores_legitimate_quoted_strings(self):
        """Test that legitimate SQL string literals are not flagged."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "test_metric",
                    "tables": ["{{ table('t') }}"],
                    "expr": "SUM(CASE WHEN {{ column('t', 'status') }} = \"Active\" THEN 1 END)",
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        # Should not flag "Active" as it doesn't contain template syntax
        assert not self.result.has_errors

    def test_multiline_expression_detection(self):
        """Test detection in multi-line expressions."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "multi_line_metric",
                    "tables": ["{{ table('orders') }}"],
                    "expr": """
                        SUM(CASE
                          WHEN "{{ column('orders', 'type') }}" = 'New'
                            AND "{{ column('orders', 'status') }}" = 'Active'
                            THEN "{{ column('orders', 'quantity') }}"
                          ELSE 0
                        END)
                    """,
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        # Should detect all 3 quoted templates
        assert len(self.result.get_errors()) == 3

    def test_provides_helpful_error_context(self):
        """Test that errors include helpful context and fix suggestions."""
        metrics_data = {
            "snowflake_metrics": [
                {"name": "broken_metric", "tables": ["{{ table('t') }}"], "expr": "\"{{ column('t', 'c') }}\""}
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        error = str(self.result.get_errors()[0])
        assert "broken_metric" in error
        assert "Fix:" in error
        assert "{{ column('t', 'c') }}" in error  # Suggested fix without quotes
        assert "Snowflake" in error  # Explanation

    def test_detects_quoted_metric_template(self):
        """Test detection of quoted metric template reference."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "test_metric",
                    "tables": ["{{ table('orders') }}"],
                    "expr": "\"{{ metric('base_metric') }}\" / 100",
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        assert "metric" in str(self.result.get_errors()[0]).lower()

    def test_detects_in_filter_expressions(self):
        """Test detection in filter expressions."""
        filters_data = {
            "snowflake_filters": [{"name": "test_filter", "expr": "\"{{ column('orders', 'date') }}\" > CURRENT_DATE"}]
        }

        self.validator.validate(
            metrics_data={},
            relationships_data={},
            semantic_views_data={},
            filters_data=filters_data,
            result=self.result,
        )

        assert self.result.has_errors
        assert "test_filter" in str(self.result.get_errors()[0])

    def test_handles_empty_data_gracefully(self):
        """Test that validator handles empty data without errors."""
        self.validator.validate(
            metrics_data={}, relationships_data={}, semantic_views_data={}, filters_data={}, result=self.result
        )

        assert not self.result.has_errors

    def test_detects_template_without_spaces(self):
        """Test detection of quoted templates without spaces."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "test_metric",
                    "tables": ["\"{{table('orders')}}\""],
                    "expr": "SUM({{ column('orders', 'total') }})",
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        assert len(self.result.get_errors()) == 1

    def test_provides_line_numbers_for_multiline(self):
        """Test that line numbers are provided for multi-line expressions."""
        metrics_data = {
            "snowflake_metrics": [
                {
                    "name": "test_metric",
                    "tables": ["{{ table('orders') }}"],
                    "expr": "Line 1\n\"{{ column('orders', 'col') }}\"\nLine 3",
                }
            ]
        }

        self.validator.validate(
            metrics_data=metrics_data,
            relationships_data={},
            semantic_views_data={},
            filters_data={},
            result=self.result,
        )

        assert self.result.has_errors
        # Check that context includes line information
        assert self.result.get_errors()[0].context["line"] == 2
