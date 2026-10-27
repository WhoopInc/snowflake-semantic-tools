"""SST-VAL103: a derived metric wraps a metric reference in an aggregate.

A `prop` code: generated metrics show the rule holds across the inputs it is about.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.semantic_members import metric, metric_findings
from tests.helpers.semantic_strategies import AGGREGATES, BASES, base_metrics, combination


def test_sst_val103_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    [found] = metric_findings(base, metric("summed", "SUM({{ metric('total') }})", derived=True), code="SST-VAL103")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "derived metric 'summed' aggregates 'total'"
    assert found.subject == "metric:summed"


def test_sst_val103_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    assert metric_findings(base, metric("doubled", "{{ metric('total') }} * 2", derived=True), code="SST-VAL103") == []


@given(aggregate=st.sampled_from(AGGREGATES), name=st.sampled_from(BASES), rest=combination())
def test_sst_val103_fires_for_every_aggregate_of_a_metric(aggregate: str, name: str, rest: str) -> None:
    derived = metric("summed", f"{aggregate}({{{{ metric('{name}') }}}}) + {rest}", derived=True)
    found = metric_findings(*base_metrics(), derived, code="SST-VAL103")
    assert {item.context["other"] for item in found} >= {name}
