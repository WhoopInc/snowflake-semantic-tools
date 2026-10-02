"""SST-VAL125: a metric attaches to more than one view, so its reach is reported."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView, Table
from snowflake_semantic_tools.domain.validate.semantic_view import fan_out_diagnostics
from tests.helpers.sql_values import authored


def _view(name: str, *metrics: str) -> tuple[SemanticView, str]:
    view = SemanticView(
        f"DB.S.{name}",
        (Table("T", "DB.S.T"),),
        metrics=tuple(Metric(metric, authored("COUNT(1)"), "T") for metric in metrics),
    )
    return view, f"semantic_view:{name.casefold()}"


def test_sst_val125_fires() -> None:
    [found] = [
        item
        for item in fan_out_diagnostics((_view("A", "ORDER_COUNT"), _view("B", "ORDER_COUNT")))
        if item.code == "SST-VAL125"
    ]
    assert found.severity is Severity.INFO
    assert found.message == "metric 'order_count' attaches to 2 views"
    assert found.subject == "metric:order_count"


def test_sst_val125_silent() -> None:
    diagnostics = fan_out_diagnostics((_view("A", "ORDER_COUNT"), _view("B", "REVENUE")))
    assert [item for item in diagnostics if item.code == "SST-VAL125"] == []
