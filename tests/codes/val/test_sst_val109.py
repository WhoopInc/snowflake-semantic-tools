"""SST-VAL109: a table-scoped metric omits its table list."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val109_fires() -> None:
    [found] = metric_findings(metric("total", "SUM({{ ref('orders', 'total') }})", tables=()), code="SST-VAL109")
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'total' is table-scoped and declares no tables:"
    assert found.subject == "metric:total"


def test_sst_val109_silent() -> None:
    assert metric_findings(metric("total", "SUM({{ ref('orders', 'total') }})"), code="SST-VAL109") == []
