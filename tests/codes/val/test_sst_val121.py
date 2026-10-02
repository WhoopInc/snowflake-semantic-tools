"""SST-VAL121: a metric's access modifier is outside the closed set."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val121_fires() -> None:
    [found] = metric_findings(
        metric("total", "SUM({{ ref('orders', 'total') }})", access_modifier="secret"), code="SST-VAL121"
    )
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'total': access_modifier is 'secret'"
    assert found.subject == "metric:total"


def test_sst_val121_silent() -> None:
    assert (
        metric_findings(
            metric("total", "SUM({{ ref('orders', 'total') }})", access_modifier="private_access"), code="SST-VAL121"
        )
        == []
    )
