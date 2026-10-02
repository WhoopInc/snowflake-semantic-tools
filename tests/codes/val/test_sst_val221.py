"""SST-VAL221: an attached expression holds a bare word that is neither a column nor a variable."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import METRICS, edited, found

EXPR = "    expr: \"SUM({{ ref('orders', 'order_total') }})\"\n"


def test_sst_val221_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, METRICS, EXPR, "    expr: \"SUM({{ ref('orders', 'order_total') }}) * fudge_factor\"\n")
    diagnostic = next(item for item in found(project, "SST-VAL221") if item.subject == "semantic_view:jaffle_sales")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:jaffle_sales: 'fudge_factor' in the expr of 'total_revenue' "
        "is neither a column nor a declared variable"
    )


def test_sst_val221_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, METRICS, EXPR, EXPR), "SST-VAL221") == []
