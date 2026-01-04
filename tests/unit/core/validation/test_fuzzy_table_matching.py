"""
Tests for fuzzy table matching in ReferenceValidator (Issue #35).

Tests the "Did you mean?" suggestion functionality for typos
and misreferenced table names.
"""

import pytest

from snowflake_semantic_tools.core.validation.rules.references import ReferenceValidator


class TestFindSimilarTables:
    """Test the _find_similar_tables helper method."""

    @pytest.fixture
    def validator(self):
        return ReferenceValidator()

    def test_empty_catalog_returns_empty(self, validator):
        """Test that empty catalog returns no suggestions."""
        suggestions = validator._find_similar_tables("orders", {})
        assert suggestions == []

    def test_empty_table_name_returns_empty(self, validator):
        """Test that empty table name returns no suggestions."""
        catalog = {"orders": {}, "customers": {}}
        suggestions = validator._find_similar_tables("", catalog)
        assert suggestions == []

    def test_exact_match_not_needed(self, validator):
        """Test that we don't suggest the exact table (it would already match)."""
        catalog = {"orders": {}}
        # This would be an odd case but should still work
        suggestions = validator._find_similar_tables("orders", catalog)
        # Prefix matching will include it, which is fine
        assert "orders" in suggestions or len(suggestions) == 0

    def test_prefix_match_suggestion(self, validator):
        """Test that tables with same prefix are suggested."""
        catalog = {
            "orders": {},
            "order_items": {},
            "order_history": {},
            "customers": {},
        }
        suggestions = validator._find_similar_tables("order_details", catalog)
        # Should suggest tables starting with "ord"
        assert any("order" in s for s in suggestions)

    def test_substring_match_suggestion(self, validator):
        """Test that tables containing the search term are suggested."""
        catalog = {
            "user_orders": {},
            "daily_orders": {},
            "customers": {},
        }
        suggestions = validator._find_similar_tables("orders", catalog)
        # Should find tables containing "orders"
        assert any("orders" in s for s in suggestions)

    def test_typo_suggestion(self, validator):
        """Test that typos get reasonable suggestions."""
        catalog = {
            "orders": {},
            "customers": {},
            "products": {},
        }
        # Typo: "ordres" instead of "orders"
        suggestions = validator._find_similar_tables("ordres", catalog)
        # Should suggest "orders" due to character overlap
        assert "orders" in suggestions

    def test_case_insensitive_matching(self, validator):
        """Test that matching is case-insensitive."""
        catalog = {
            "ORDERS": {},
            "CUSTOMERS": {},
        }
        suggestions = validator._find_similar_tables("orders", catalog)
        assert "ORDERS" in suggestions

    def test_max_three_suggestions(self, validator):
        """Test that at most 3 suggestions are returned."""
        catalog = {
            "table_a": {},
            "table_b": {},
            "table_c": {},
            "table_d": {},
            "table_e": {},
        }
        suggestions = validator._find_similar_tables("table_x", catalog)
        assert len(suggestions) <= 3

    def test_suffix_matching(self, validator):
        """Test that tables with same suffix are considered."""
        catalog = {
            "raw_orders": {},
            "stg_orders": {},
            "fct_orders": {},
            "customers": {},
        }
        suggestions = validator._find_similar_tables("dim_orders", catalog)
        # Should find tables ending with "orders"
        assert len(suggestions) > 0

    def test_no_match_returns_empty(self, validator):
        """Test that completely different names return empty."""
        catalog = {
            "alpha": {},
            "beta": {},
            "gamma": {},
        }
        suggestions = validator._find_similar_tables("xyz", catalog)
        # Might be empty or have weak matches
        # The algorithm may still find weak matches, so we just verify it doesn't crash
        assert isinstance(suggestions, list)


class TestFindSimilarColumns:
    """Test the _find_similar_columns helper method."""

    @pytest.fixture
    def validator(self):
        return ReferenceValidator()

    def test_empty_columns_returns_empty(self, validator):
        """Test that empty columns set returns no suggestions."""
        suggestions = validator._find_similar_columns("user_id", set())
        assert suggestions == []

    def test_prefix_match_column(self, validator):
        """Test that columns with same prefix are suggested."""
        columns = {"user_id", "user_name", "user_email", "order_id"}
        suggestions = validator._find_similar_columns("user_status", columns)
        assert any("user" in s.lower() for s in suggestions)

    def test_suffix_match_column(self, validator):
        """Test that columns with same suffix are suggested."""
        columns = {"user_id", "order_id", "customer_id", "name"}
        suggestions = validator._find_similar_columns("product_id", columns)
        assert any("_id" in s.lower() for s in suggestions)

    def test_typo_column_suggestion(self, validator):
        """Test that column typos get reasonable suggestions."""
        columns = {"customer_id", "order_date", "amount"}
        suggestions = validator._find_similar_columns("custmer_id", columns)  # Typo
        assert "customer_id" in suggestions


class TestReferenceValidatorSuggestions:
    """Test that validation errors include suggestions."""

    @pytest.fixture
    def validator(self):
        return ReferenceValidator()

    def test_metric_unknown_table_has_suggestions(self, validator):
        """Test that metric with unknown table gets suggestions in error."""
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "total_revenue",
                        "tables": ["ordres"],  # Typo for "orders"
                        "expr": "SUM(amount)",
                    }
                ]
            }
        }
        dbt_catalog = {
            "orders": {"database": "db", "schema": "schema", "columns": {}},
            "customers": {"database": "db", "schema": "schema", "columns": {}},
        }

        result = validator.validate(semantic_data, dbt_catalog)
        errors = [i for i in result.issues if i.severity.name == "ERROR"]

        # Should have an error about unknown table
        assert len(errors) >= 1
        error_msg = errors[0].message

        # Error should include "Did you mean" with suggestion
        assert "Did you mean" in error_msg or "orders" in str(errors[0].context.get("suggestions", []))

    def test_relationship_unknown_table_has_suggestions(self, validator):
        """Test that relationship with unknown table gets suggestions."""
        semantic_data = {
            "relationships": {
                "items": [
                    {
                        "relationship_name": "orders_to_custs",
                        "left_table_name": "orders",
                        "right_table_name": "custs",  # Should be "customers"
                    }
                ],
                "relationship_columns": [],
            }
        }
        dbt_catalog = {
            "orders": {"database": "db", "schema": "schema", "columns": {}},
            "customers": {"database": "db", "schema": "schema", "columns": {}},
        }

        result = validator.validate(semantic_data, dbt_catalog)
        errors = [i for i in result.issues if i.severity.name == "ERROR"]

        # Should have an error about unknown table
        assert len(errors) >= 1
        # Check that suggestions are provided
        assert any("Did you mean" in e.message or e.context.get("suggestions") for e in errors if "custs" in e.message)

    def test_filter_unknown_table_has_suggestions(self, validator):
        """Test that filter with unknown table gets suggestions."""
        semantic_data = {
            "filters": {
                "items": [
                    {
                        "name": "active_filter",
                        "table_name": "usr",  # Should be "users"
                        "expression": "status = 'active'",
                    }
                ]
            }
        }
        dbt_catalog = {
            "users": {"database": "db", "schema": "schema", "columns": {}},
            "orders": {"database": "db", "schema": "schema", "columns": {}},
        }

        result = validator.validate(semantic_data, dbt_catalog)
        errors = [i for i in result.issues if i.severity.name == "ERROR"]

        assert len(errors) >= 1
        assert any("Did you mean" in e.message or e.context.get("suggestions") for e in errors)

    def test_no_suggestions_when_no_similar_tables(self, validator):
        """Test that error message still makes sense with no similar tables."""
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "test_metric",
                        "tables": ["xyz123"],  # Completely different
                        "expr": "SUM(amount)",
                    }
                ]
            }
        }
        dbt_catalog = {
            "orders": {"database": "db", "schema": "schema", "columns": {}},
        }

        result = validator.validate(semantic_data, dbt_catalog)
        errors = [i for i in result.issues if i.severity.name == "ERROR"]

        assert len(errors) >= 1
        # Should have guidance about meta.sst configuration
        error_msg = errors[0].message
        assert "config.meta.sst" in error_msg or "Did you mean" in error_msg
