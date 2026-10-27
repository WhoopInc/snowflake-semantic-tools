"""SST-VAL117: a sum over a snapshot grain declares no non-additive dimension."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.authored import NonAdditiveDef
from tests.helpers.semantic_members import metric, metric_findings

BALANCE = "{{ ref('balances', 'balance') }}"


def test_sst_val117_fires() -> None:
    [found] = metric_findings(metric("balance_total", f"SUM({BALANCE})", tables=("balances",)), code="SST-VAL117")
    assert found.severity is Severity.WARNING
    assert found.message == "metric 'balance_total' is over a snapshot grain and declares no non_additive_dimensions"
    assert found.subject == "metric:balance_total"


def test_sst_val117_silent() -> None:
    latest = metric("balance_latest", f"SUM({BALANCE})", tables=("balances",), non_additive=(NonAdditiveDef("as_of"),))
    # orders is one row per order: its key holds no date, so a sum over it is additive.
    assert metric_findings(latest, metric("total", "SUM({{ ref('orders', 'total') }})"), code="SST-VAL117") == []
