"""SST-VAL101: a table-scoped metric's expression has no aggregate at its root."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val101_fires() -> None:
    [found] = metric_findings(metric("raw_total", "{{ ref('orders', 'total') }} + 1"), code="SST-VAL101")
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'raw_total' is table-scoped and its expr is not an aggregate"
    assert found.subject == "metric:raw_total"


def test_sst_val101_silent() -> None:
    assert metric_findings(metric("total", "SUM({{ ref('orders', 'total') }})"), code="SST-VAL101") == []
