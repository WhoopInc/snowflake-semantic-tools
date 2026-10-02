"""SST-VAL006: an authored expression, query or table entry hardcodes a fully-qualified name."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import METRICS, edited, found

EXPR = "    expr: \"SUM({{ ref('orders', 'order_total') }})\"\n"


def test_sst_val006_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, METRICS, EXPR, '    expr: "SUM(SST_REF_DEV.JAFFLE.ORDERS.ORDER_TOTAL)"\n')
    [diagnostic] = found(project, "SST-VAL006")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric 'total_revenue': 'expr' hardcodes 'SST_REF_DEV.JAFFLE.ORDERS.ORDER_TOTAL'"
    assert diagnostic.subject == "metric:total_revenue"


def test_sst_val006_silent(tmp_path: Path) -> None:
    # A dotted string literal is data, not a name.
    project = edited(
        tmp_path,
        METRICS,
        EXPR,
        "    expr: \"SUM(CASE WHEN 'a.b.c' = 'x' THEN 0 END) + SUM({{ ref('orders', 'order_total') }})\"\n",
    )
    assert found(project, "SST-VAL006") == []
