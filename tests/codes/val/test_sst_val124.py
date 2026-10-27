"""SST-VAL124: two metrics compute the same thing under two names."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val124_fires() -> None:
    first, twin = (
        metric("total", "SUM({{ ref('orders', 'total') }})"),
        metric("revenue", "sum({{ ref('orders', 'total') }})"),
    )
    [found] = metric_findings(first, twin, code="SST-VAL124")
    assert found.severity is Severity.WARNING
    assert found.message == "metric 'revenue' has the same expression as 'total'"
    assert found.subject == "metric:revenue"


def test_sst_val124_silent() -> None:
    assert (
        metric_findings(
            metric("total", "SUM({{ ref('orders', 'total') }})"),
            metric("cost", "SUM({{ ref('orders', 'cost') }})"),
            code="SST-VAL124",
        )
        == []
    )
