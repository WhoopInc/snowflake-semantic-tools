"""Tests for suggestion helpers."""

import pytest

from snowflake_semantic_tools.core.diagnostics.suggestions import (
    format_available_list,
    suggest_column,
    suggest_table,
    suggest_value,
)


class TestSuggestTable:
    def test_exact_match_not_needed(self):
        result = suggest_table("orders", ["orders", "customers"])
        assert result == "orders"

    def test_typo_correction(self):
        result = suggest_table("ordres", ["orders", "customers", "products"])
        assert result == "orders"

    def test_case_insensitive(self):
        result = suggest_table("ORDERS", ["orders", "customers"])
        assert result == "orders"

    def test_no_match(self):
        result = suggest_table("xyz_completely_different", ["orders", "customers"])
        assert result is None

    def test_empty_available(self):
        result = suggest_table("orders", [])
        assert result is None

    def test_close_match_with_prefix(self):
        result = suggest_table("customer", ["customers", "orders"])
        assert result == "customers"


class TestSuggestColumn:
    def test_typo_correction(self):
        result = suggest_column("amout", ["amount", "status", "id"])
        assert result == "amount"

    def test_no_match(self):
        result = suggest_column("xyz", ["amount", "status"])
        assert result is None


class TestSuggestValue:
    def test_column_type_typo(self):
        result = suggest_value("dimensin", ["dimension", "fact", "time_dimension"])
        assert result == "dimension"

    def test_visibility_typo(self):
        result = suggest_value("privte", ["private", "public"])
        assert result == "private"


class TestFormatAvailableList:
    def test_short_list(self):
        result = format_available_list(["a", "b", "c"])
        assert result == "a, b, c"

    def test_truncation(self):
        result = format_available_list(["a", "b", "c", "d", "e", "f", "g"], max_show=3)
        assert "a, b, c" in result
        assert "4 more" in result

    def test_empty(self):
        result = format_available_list([])
        assert result == "(none)"

    def test_exact_max(self):
        result = format_available_list(["a", "b", "c"], max_show=3)
        assert result == "a, b, c"
        assert "more" not in result
