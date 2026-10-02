"""SST-VAL110: a metric expression names a column of its table without a ref()."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val110_fires() -> None:
    [found] = metric_findings(metric("total", "SUM(total)"), code="SST-VAL110")
    assert found.severity is Severity.WARNING
    assert found.message == "metric 'total' expression contains bare identifier 'total'"
    assert found.subject == "metric:total"


def test_sst_val110_silent() -> None:
    assert metric_findings(metric("total", "SUM({{ ref('orders', 'total') }})"), code="SST-VAL110") == []
