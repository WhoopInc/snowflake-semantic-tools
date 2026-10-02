"""SST-VAL318: an expression names a column marked exclude."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val318_fires() -> None:
    [found] = metric_findings(metric("secrets", "SUM({{ ref('orders', 'secret') }})"), code="SST-VAL318")
    assert found.severity is Severity.ERROR
    assert found.message == "metric:secrets: 'secrets' references excluded column 'secret'"
    assert found.subject == "metric:secrets"


def test_sst_val318_silent() -> None:
    assert metric_findings(metric("total", "SUM({{ ref('orders', 'total') }})"), code="SST-VAL318") == []
