"""SST-VAL104: a derived metric's expression names a physical column.

A `prop` code: generated metrics show the rule holds across the inputs it is about.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.semantic_members import metric, metric_findings
from tests.helpers.semantic_strategies import base_metrics, combination


def test_sst_val104_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    derived = metric("plus_column", "{{ metric('total') }} + {{ ref('orders', 'total') }}", derived=True)
    [found] = metric_findings(base, derived, code="SST-VAL104")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "derived metric 'plus_column' references column 'orders.total'"
    assert found.subject == "metric:plus_column"


def test_sst_val104_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    assert metric_findings(base, metric("plus_one", "{{ metric('total') }} + 1", derived=True), code="SST-VAL104") == []


@given(column=st.sampled_from(("order_id", "customer_id", "total", "cost", "state")), rest=combination())
def test_sst_val104_fires_for_every_column_a_derived_metric_names(column: str, rest: str) -> None:
    derived = metric("mixed", f"{rest} + {{{{ ref('orders', '{column}') }}}}", derived=True)
    [found] = metric_findings(*base_metrics(), derived, code="SST-VAL104")
    assert found.context["column"] == f"orders.{column}"
