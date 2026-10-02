"""SST-VAL115: a metric's using_relationships lists more than one hop."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val115_fires() -> None:
    chained = metric("total", "SUM({{ ref('orders', 'total') }})", using_relationships=("A_TO_B", "B_TO_C"))
    [found] = metric_findings(chained, code="SST-VAL115")
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'total' declares a chain of 2 relationships"
    assert found.subject == "metric:total"


def test_sst_val115_silent() -> None:
    assert (
        metric_findings(
            metric("total", "SUM({{ ref('orders', 'total') }})", using_relationships=("A_TO_B",)), code="SST-VAL115"
        )
        == []
    )
