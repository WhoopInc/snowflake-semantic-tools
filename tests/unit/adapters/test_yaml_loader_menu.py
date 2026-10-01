"""Focused checks for the temporal and semi-additive Jaffle Menu surface."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.model.semantic_view import SemanticView, SortKey
from tests.helpers.projects import load_views

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


@pytest.fixture(scope="module")
def menu() -> SemanticView:
    views = load_views(FIXTURE, manifest_path=MANIFEST)
    return next(view for view in views if view.fqn.endswith(".JAFFLE_MENU"))


def test_uses_manifest_alias_and_view_range_constraint(menu: SemanticView) -> None:
    pricing = next(table for table in menu.tables if table.logical_name == "PRICING_PERIODS")
    assert pricing.fqn == "SST_REF_DEV.JAFFLE.PRICING_CALENDAR"
    assert pricing.distinct_range == ("EFFECTIVE_START_AT", "EFFECTIVE_END_AT")


def test_loads_asof_and_range_relationships(menu: SemanticView) -> None:
    relationships = {relationship.name: relationship for relationship in menu.relationships}
    asof = relationships["ORDER_ITEMS_TO_ORDERS"]
    assert asof.from_columns == ("ORDER_ID", "OCCURRED_AT")
    assert asof.to_columns == ("ORDER_ID", "ORDERED_AT")
    assert asof.asof_index == 1
    range_relationship = relationships["ORDERS_TO_PRICING_PERIODS"]
    assert range_relationship.from_columns == ("ORDERED_AT",)
    assert range_relationship.range_bounds == ("EFFECTIVE_START_AT", "EFFECTIVE_END_AT")


def test_loads_path_pinning_and_non_additive_dimension(menu: SemanticView) -> None:
    metrics = {metric.name: metric for metric in menu.metrics}
    assert metrics["LINE_ITEM_COUNT"].using_relationships == ("ORDER_ITEMS_TO_ORDERS",)
    assert metrics["TOTAL_SUPPLY_COST"].non_additive_by == (SortKey("SNAPSHOT_MONTH"),)
    assert metrics["OPENING_SUPPLY_COST"].non_additive_by == (
        SortKey("SUPPLIES.SNAPSHOT_MONTH", descending=True, nulls_first=True),
    )


def test_loads_sql_sidecar_without_its_comment_header(menu: SemanticView) -> None:
    query = next(query for query in menu.verified_queries if query.name == "PRODUCT_MIX_BY_TYPE")
    assert query.sql.startswith("SELECT\n")
    assert "semantic_models/verified_queries/sql" not in query.sql
    assert "INNER JOIN products" in query.sql


def test_derived_metric_depth_is_resolved_to_rendered_metric_names(menu: SemanticView) -> None:
    metrics = {metric.name: metric for metric in menu.metrics}
    assert metrics["GROSS_MARGIN"].expr == ("ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE - SUPPLIES.TOTAL_SUPPLY_COST")
    assert metrics["GROSS_MARGIN_RATE"].expr == "DIV0(GROSS_MARGIN, ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE)"
