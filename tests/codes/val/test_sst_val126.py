"""SST-VAL126: a window metric's function applies to a row-level column."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.defs import WindowDef
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val126_fires() -> None:
    [found] = metric_findings(
        metric("running", "SUM({{ ref('orders', 'total') }})", window=WindowDef()), code="SST-VAL126"
    )
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "metric 'running': SUM must apply to a metric or an aggregate to be a window metric"
    assert found.subject == "metric:running"


def test_sst_val126_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    running = metric("running", "SUM({{ metric('total') }})", window=WindowDef())
    assert metric_findings(base, running, code="SST-VAL126") == []
