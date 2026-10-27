"""A semantic view read back with SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW, beside what SST renders.

The document shapes are the ones a live account returned for a probe view that
used every feature DESCRIBE does not report: a variable, the staleness, a window metric, `USING`,
an ASOF and a range relationship, and a distinct-range constraint. Snowflake spells a variable's
type with its precision, `NUMBER(38,0)`, and its default as text.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pytest

from snowflake_semantic_tools.domain.model.semantic_view import (
    Metric,
    Relationship,
    SemanticView,
    SortKey,
    Table,
    Variable,
    Window,
)
from snowflake_semantic_tools.domain.plan.definitions.semantic_view import desired_view, live_view_from_yaml
from snowflake_semantic_tools.domain.plan.member_diff import CHANGED, diff_definitions
from tests.helpers.sql_values import authored, statement


def _document(**changes: object) -> dict[str, Any]:
    document: dict[str, Any] = {
        "name": "PROBE",
        "description": "Probe. [sst:" + "a" * 64 + ":" + "b" * 64 + "]",
        "max_staleness": "300 seconds",
        "variables": [
            {"name": "LARGE", "description": "Threshold.", "data_type": "NUMBER(38,0)", "default_value": "10"}
        ],
        "tables": [
            {
                "name": "CUSTOMERS",
                "base_table": {"database": "DB", "schema": "MARTS", "table": "CUSTOMERS"},
                "primary_key": {"columns": ["CUSTOMER_ID"]},
                "dimensions": [{"name": "FIRST_AT", "expr": "CUSTOMERS.FIRST_AT", "access_modifier": "public_access"}],
                "metrics": [
                    {
                        "name": "RUNNING",
                        "expr": "SUM(CUSTOMERS.N) OVER (ORDER BY CUSTOMERS.FIRST_AT ASC NULLS LAST)",
                        "access_modifier": "public_access",
                    },
                ],
            },
            {
                "name": "ORDERS",
                "base_table": {"database": "DB", "schema": "MARTS", "table": "ORDERS"},
                "primary_key": {"columns": ["ORDER_ID"]},
                "metrics": [
                    {
                        "name": "REVENUE",
                        "expr": "SUM(ORDERS.SUBTOTAL)",
                        "access_modifier": "public_access",
                        "using_relationships": ["ORDERS_TO_CUSTOMERS"],
                    },
                    {
                        "name": "STOCK",
                        "expr": "SUM(ORDERS.UNITS)",
                        "access_modifier": "public_access",
                        "non_additive_dimensions": [
                            {"table": "ORDERS", "dimension": "AT", "sort_direction": "ascending", "null_order": "first"}
                        ],
                    },
                ],
            },
            {
                "name": "PERIODS",
                "base_table": {"database": "DB", "schema": "MARTS", "table": "PERIODS"},
                "primary_key": {"columns": ["PERIOD_ID"]},
                "constraints": [
                    {"name": "PERIODS_DISTINCT_RANGE", "distinct_range": {"start_column": "S", "end_column": "E"}}
                ],
            },
        ],
        "relationships": [
            {
                "name": "ORDERS_TO_CUSTOMERS",
                "left_table": "ORDERS",
                "right_table": "CUSTOMERS",
                "relationship_columns": [{"left_column": "CUSTOMER_ID", "right_column": "CUSTOMER_ID"}],
            },
            {
                "name": "ORDERS_ASOF",
                "left_table": "ORDERS",
                "right_table": "CUSTOMERS",
                "relationship_columns": [
                    {"left_column": "CUSTOMER_ID", "right_column": "CUSTOMER_ID"},
                    {"left_column": "AT", "right_column": "FIRST_AT", "type": "asof"},
                ],
            },
            {
                "name": "ORDERS_TO_PERIODS",
                "left_table": "ORDERS",
                "right_table": "PERIODS",
                "relationship_columns": [
                    {"left_column": "AT", "type": "range", "right_range": {"start_column": "S", "end_column": "E"}}
                ],
            },
        ],
    }
    document.update(changes)
    return document


def _view(**changes: object) -> SemanticView:
    view = SemanticView(
        fqn="DB.SV.PROBE",
        tables=(
            Table("CUSTOMERS", "DB.MARTS.CUSTOMERS", primary_key=("CUSTOMER_ID",)),
            Table("ORDERS", "DB.MARTS.ORDERS", primary_key=("ORDER_ID",)),
            Table("PERIODS", "DB.MARTS.PERIODS", primary_key=("PERIOD_ID",), distinct_range=("S", "E")),
        ),
        relationships=(
            Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",)),
            Relationship(
                "ORDERS_ASOF", "ORDERS", ("CUSTOMER_ID", "AT"), "CUSTOMERS", ("CUSTOMER_ID", "FIRST_AT"), asof_index=1
            ),
            Relationship("ORDERS_TO_PERIODS", "ORDERS", ("AT",), "PERIODS", ("S", "E"), range_bounds=("S", "E")),
        ),
        variables=(Variable("LARGE", statement("NUMBER"), statement("10"), "Threshold."),),
        metrics=(
            Metric(
                "RUNNING",
                authored("SUM(CUSTOMERS.N)"),
                "CUSTOMERS",
                window=Window(order_by=(SortKey(authored("CUSTOMERS.FIRST_AT"), descending=False, nulls_first=False),)),
            ),
            Metric("REVENUE", authored("SUM(ORDERS.SUBTOTAL)"), "ORDERS", using_relationships=("ORDERS_TO_CUSTOMERS",)),
            Metric(
                "STOCK",
                authored("SUM(ORDERS.UNITS)"),
                "ORDERS",
                non_additive_by=(SortKey(authored("ORDERS.AT"), nulls_first=True),),
            ),
        ),
        comment="Probe.",
        max_staleness="300 seconds",
    )
    return replace(view, **changes)  # type: ignore[arg-type]


def _without_dimension(document: Mapping[str, Any]) -> dict[str, Any]:
    tables = [dict(table) for table in document["tables"]]
    tables[0].pop("dimensions")
    return {**document, "tables": tables}


def test_the_view_and_its_yaml_agree_on_every_member_describe_misses() -> None:
    live = live_view_from_yaml(_without_dimension(_document()))
    assert diff_definitions(live, desired_view(_view())) == ()


@pytest.mark.parametrize(
    ("changes", "member", "prop"),
    [
        ({"max_staleness": "1 hour"}, "max_staleness", "text"),
        ({"variables": [{"name": "LARGE", "data_type": "NUMBER(38,0)", "default_value": "20"}]}, "large", "default"),
    ],
)
def test_a_change_only_the_yaml_reports_is_named(changes: dict[str, object], member: str, prop: str) -> None:
    live = live_view_from_yaml(_without_dimension(_document(**changes)))
    [diff] = [item for item in diff_definitions(live, desired_view(_view())) if item.name == member]
    assert diff.op == CHANGED and prop in [item.name for item in diff.properties]


def test_a_dropped_using_and_a_changed_window_are_named() -> None:
    view = _view(
        metrics=(
            replace(_view().metrics[0], window=Window(order_by=(SortKey(authored("CUSTOMERS.FIRST_AT"), True),))),
            replace(_view().metrics[1], using_relationships=()),
            _view().metrics[2],
        )
    )
    changed = {
        (item.name, prop.name)
        for item in diff_definitions(live_view_from_yaml(_without_dimension(_document())), desired_view(view))
        for prop in item.properties
    }
    assert changed == {("customers.running", "window_expression"), ("orders.revenue", "using")}


def test_asof_and_range_relationships_compare_their_referenced_columns() -> None:
    relationships = {
        member.name: dict(member.properties)
        for member in live_view_from_yaml(_document()).members
        if member.kind == "relationship"
    }
    assert relationships["orders_asof"]["asof_column"] == "FIRST_AT"
    assert relationships["orders_to_periods"]["range"] == "S AND E"
    assert "ref_columns" not in relationships["orders_asof"]
    assert relationships["orders_to_customers"]["ref_columns"] == ("CUSTOMER_ID",)


def test_a_document_without_tables_is_refused() -> None:
    with pytest.raises(ValueError, match="tables"):
        live_view_from_yaml({"name": "X"})
