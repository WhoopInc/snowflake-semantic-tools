"""SST-VAL127: a window declares a frame and no order_by."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.model.authored import WindowDef, WindowOrderDef
from tests.helpers.semantic_members import metric, metric_findings

FRAME = "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"


def test_sst_val127_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    running = metric("running", "SUM({{ metric('total') }})", window=WindowDef(frame=FRAME, has_frame=True))
    [found] = metric_findings(base, running, code="SST-VAL127")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == f"metric 'running': window frame '{FRAME}' needs an order_by"
    assert found.subject == "metric:running"


def test_sst_val127_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    window = WindowDef(order_by=(WindowOrderDef("{{ ref('orders', 'ordered_at') }}"),), frame=FRAME, has_frame=True)
    assert (
        metric_findings(base, metric("running", "SUM({{ metric('total') }})", window=window), code="SST-VAL127") == []
    )
