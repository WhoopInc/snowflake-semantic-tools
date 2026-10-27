"""SST-VAL105: a derived metric reaches an un-aggregated fact or dimension.

A `prop` code: generated metrics show the rule holds across the inputs it is about.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.semantic_members import metric, metric_findings
from tests.helpers.semantic_strategies import base_metrics, combination


def test_sst_val105_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    derived = metric("plus_fact", "{{ metric('total') }} + {{ fact('total') }}", derived=True)
    [found] = metric_findings(base, derived, code="SST-VAL105")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "derived metric 'plus_fact' references un-aggregated member 'total'"
    assert found.subject == "metric:plus_fact"


def test_sst_val105_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    assert metric_findings(base, metric("halved", "{{ metric('total') }} / 2", derived=True), code="SST-VAL105") == []


@given(kind=st.sampled_from(("fact", "dimension")), member=st.sampled_from(("total", "state")), rest=combination())
def test_sst_val105_fires_for_every_member_a_derived_metric_reads(kind: str, member: str, rest: str) -> None:
    derived = metric("mixed", f"{rest} + {{{{ {kind}('{member}') }}}}", derived=True)
    [found] = metric_findings(*base_metrics(), derived, code="SST-VAL105")
    assert found.context["other"] == member
