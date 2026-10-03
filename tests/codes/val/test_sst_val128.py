"""SST-VAL128: a metric references a window function metric."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.model.authored import WindowDef
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val128_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    running = metric("running", "SUM({{ metric('total') }})", window=WindowDef())
    [found] = metric_findings(
        base, running, metric("uses_running", "{{ metric('running') }} + 1", derived=True), code="SST-VAL128"
    )
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "metric 'uses_running' references 'running', a window function metric"
    assert found.subject == "metric:uses_running"


def test_sst_val128_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    assert (
        metric_findings(base, metric("uses_total", "{{ metric('total') }} + 1", derived=True), code="SST-VAL128") == []
    )
