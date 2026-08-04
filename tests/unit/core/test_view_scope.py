"""Tests for view-level include/exclude scope controls (GitHub issue #248)."""
import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
    _extract_view_scope_names,
    parse_semantic_views,
)
from snowflake_semantic_tools.core.generation.semantic_view_builder import SemanticViewBuilder
from snowflake_semantic_tools.core.models.semantic_model import SemanticView


# ===== Parser tests =====


class TestExtractViewScopeNames:
    """Tests for _extract_view_scope_names helper."""

    def test_returns_none_when_items_is_none(self):
        assert _extract_view_scope_names(None, "metrics") is None

    def test_returns_none_when_items_is_not_list(self):
        assert _extract_view_scope_names("not a list", "metrics") is None

    def test_bare_names_normalized_to_uppercase(self):
        result = _extract_view_scope_names(["total_revenue", "active_users"], "metrics")
        assert result == ["TOTAL_REVENUE", "ACTIVE_USERS"]

    def test_jinja_metric_syntax(self):
        result = _extract_view_scope_names(
            ["{{ metric('total_revenue') }}", "{{ metric('avg_order') }}"],
            "metrics",
        )
        assert result == ["TOTAL_REVENUE", "AVG_ORDER"]

    def test_jinja_column_syntax(self):
        result = _extract_view_scope_names(
            ["{{ column('orders', 'order_date') }}", "{{ column('users', 'segment') }}"],
            "columns",
        )
        assert result == ["ORDERS.ORDER_DATE", "USERS.SEGMENT"]

    def test_jinja_relationship_syntax(self):
        result = _extract_view_scope_names(
            ["{{ relationship('orders_to_customers') }}"],
            "relationships",
        )
        assert result == ["ORDERS_TO_CUSTOMERS"]

    def test_jinja_filter_syntax(self):
        result = _extract_view_scope_names(
            ["{{ filter('completed_orders') }}"],
            "filters",
        )
        assert result == ["COMPLETED_ORDERS"]

    def test_bare_column_with_dot_notation(self):
        result = _extract_view_scope_names(
            ["orders.created_at", "users.segment"],
            "columns",
        )
        assert result == ["ORDERS.CREATED_AT", "USERS.SEGMENT"]

    def test_exclude_metrics_uses_metric_pattern(self):
        result = _extract_view_scope_names(
            ["{{ metric('debug_metric') }}"],
            "exclude_metrics",
        )
        assert result == ["DEBUG_METRIC"]

    def test_empty_list_returns_none(self):
        result = _extract_view_scope_names([], "metrics")
        assert result is None

    def test_mixed_jinja_and_bare(self):
        result = _extract_view_scope_names(
            ["{{ metric('total_revenue') }}", "active_users"],
            "metrics",
        )
        assert result == ["TOTAL_REVENUE", "ACTIVE_USERS"]


class TestParseSemanticViewsScope:
    """Tests for scope field extraction in parse_semantic_views."""

    def test_include_metrics_parsed(self):
        views = [
            {
                "name": "test_view",
                "tables": ["orders"],
                "metrics": ["{{ metric('total_revenue') }}", "{{ metric('avg_order') }}"],
            }
        ]
        result = parse_semantic_views(views, Path("test.yml"))
        assert len(result) == 1
        metrics = json.loads(result[0]["metrics"])
        assert metrics == ["TOTAL_REVENUE", "AVG_ORDER"]

    def test_exclude_columns_parsed(self):
        views = [
            {
                "name": "test_view",
                "tables": ["customers"],
                "exclude_columns": ["{{ column('customers', 'email') }}"],
            }
        ]
        result = parse_semantic_views(views, Path("test.yml"))
        assert len(result) == 1
        exclude_cols = json.loads(result[0]["exclude_columns"])
        assert exclude_cols == ["CUSTOMERS.EMAIL"]

    def test_dimensions_key_maps_to_columns(self):
        """Legacy 'dimensions' key should be treated as 'columns'."""
        views = [
            {
                "name": "test_view",
                "tables": ["orders"],
                "dimensions": ["orders.created_at", "orders.status"],
            }
        ]
        result = parse_semantic_views(views, Path("test.yml"))
        assert len(result) == 1
        columns = json.loads(result[0]["columns"])
        assert columns == ["ORDERS.CREATED_AT", "ORDERS.STATUS"]

    def test_no_scope_fields_not_in_record(self):
        views = [{"name": "simple_view", "tables": ["orders"]}]
        result = parse_semantic_views(views, Path("test.yml"))
        assert "metrics" not in result[0]
        assert "columns" not in result[0]
        assert "relationships" not in result[0]
        assert "filters" not in result[0]


# ===== Builder scope filter tests =====


class TestApplyViewScope:
    """Tests for SemanticViewBuilder._apply_view_scope static method."""

    def test_include_mode_filters_to_allowed(self):
        items = [
            {"NAME": "total_revenue"},
            {"NAME": "avg_order"},
            {"NAME": "user_count"},
        ]
        result = SemanticViewBuilder._apply_view_scope(
            items, include_list=["TOTAL_REVENUE", "AVG_ORDER"], exclude_list=None,
            key_fn=lambda x: x["NAME"]
        )
        assert len(result) == 2
        assert result[0]["NAME"] == "total_revenue"
        assert result[1]["NAME"] == "avg_order"

    def test_exclude_mode_removes_blocked(self):
        items = [
            {"NAME": "total_revenue"},
            {"NAME": "debug_metric"},
            {"NAME": "user_count"},
        ]
        result = SemanticViewBuilder._apply_view_scope(
            items, include_list=None, exclude_list=["DEBUG_METRIC"],
            key_fn=lambda x: x["NAME"]
        )
        assert len(result) == 2
        assert all(i["NAME"] != "debug_metric" for i in result)

    def test_none_lists_returns_all(self):
        items = [{"NAME": "a"}, {"NAME": "b"}]
        result = SemanticViewBuilder._apply_view_scope(
            items, include_list=None, exclude_list=None,
            key_fn=lambda x: x["NAME"]
        )
        assert result == items

    def test_include_takes_priority_over_exclude(self):
        """When include_list is set, exclude_list is ignored."""
        items = [{"NAME": "a"}, {"NAME": "b"}, {"NAME": "c"}]
        result = SemanticViewBuilder._apply_view_scope(
            items, include_list=["A"], exclude_list=["B"],
            key_fn=lambda x: x["NAME"]
        )
        assert len(result) == 1
        assert result[0]["NAME"] == "a"

    def test_case_insensitive_matching(self):
        items = [{"NAME": "Total_Revenue"}]
        result = SemanticViewBuilder._apply_view_scope(
            items, include_list=["TOTAL_REVENUE"], exclude_list=None,
            key_fn=lambda x: x["NAME"]
        )
        assert len(result) == 1


# ===== Dataclass tests =====


class TestSemanticViewDataclass:
    """Tests for SemanticView dataclass with scope fields."""

    def test_default_scope_fields_are_none(self):
        view = SemanticView(name="test", tables=["orders"])
        assert view.columns is None
        assert view.metrics is None
        assert view.relationships is None
        assert view.filters is None
        assert view.exclude_columns is None
        assert view.exclude_metrics is None
        assert view.exclude_relationships is None
        assert view.exclude_filters is None

    def test_to_dict_includes_scope_when_set(self):
        view = SemanticView(
            name="test",
            tables=["orders"],
            metrics=["TOTAL_REVENUE"],
            exclude_columns=["ORDERS.EMAIL"],
        )
        d = view.to_dict()
        assert d["metrics"] == ["TOTAL_REVENUE"]
        assert d["exclude_columns"] == ["ORDERS.EMAIL"]

    def test_to_dict_excludes_none_scope(self):
        view = SemanticView(name="test", tables=["orders"])
        d = view.to_dict()
        assert "metrics" not in d
        assert "columns" not in d
        assert "exclude_metrics" not in d
