"""SST-VAL112: a metric's expression reaches a table outside its tables."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val112_fires() -> None:
    reaching = metric("total", "SUM({{ ref('orders', 'total') }}) + SUM({{ ref('balances', 'balance') }})")
    [found] = metric_findings(reaching, code="SST-VAL112")
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'total' in metric:total reaches balances"
    assert found.subject == "metric:total"


def test_sst_val112_silent() -> None:
    both = metric(
        "total",
        "SUM({{ ref('orders', 'total') }}) + SUM({{ ref('balances', 'balance') }})",
        tables=("orders", "balances"),
    )
    assert metric_findings(both, code="SST-VAL112") == []
