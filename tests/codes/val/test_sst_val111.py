"""SST-VAL111: a metric divides by a denominator neither NULLIF nor DIV0 guards."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val111_fires() -> None:
    [found] = metric_findings(
        metric("margin_rate", "SUM({{ ref('orders', 'total') }}) / SUM({{ ref('orders', 'cost') }})"), code="SST-VAL111"
    )
    assert found.severity is Severity.WARNING
    assert found.message == "metric 'margin_rate' divides without DIV0 or NULLIF on the denominator"
    assert found.subject == "metric:margin_rate"


def test_sst_val111_silent() -> None:
    guarded = metric("margin_rate", "SUM({{ ref('orders', 'total') }}) / NULLIF(SUM({{ ref('orders', 'cost') }}), 0)")
    in_cents = metric("total_dollars", "SUM({{ ref('orders', 'total') }}) / 100")
    assert metric_findings(guarded, in_cents, code="SST-VAL111") == []
