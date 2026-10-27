"""SST-VAL107: a table-scoped metric references a metric with non-additive dimensions.

A `prop` code: generated metrics show the rule holds across the inputs it is about.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.model.authored import NonAdditiveDef
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val107_fires() -> None:
    snapshot = metric("latest", "SUM({{ ref('orders', 'total') }})", non_additive=(NonAdditiveDef("ordered_at"),))
    regular = metric("uses_latest", "SUM({{ ref('orders', 'cost') }}) - {{ metric('latest') }}")
    [found] = metric_findings(snapshot, regular, code="SST-VAL107")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "metric 'uses_latest' references 'latest', which declares non_additive_dimensions"
    assert found.subject == "metric:uses_latest"


def test_sst_val107_silent() -> None:
    snapshot = metric("latest", "SUM({{ ref('orders', 'total') }})", non_additive=(NonAdditiveDef("ordered_at"),))
    derived = metric("latest_doubled", "{{ metric('latest') }} * 2", derived=True)
    assert metric_findings(snapshot, derived, code="SST-VAL107") == []


@given(
    dimension=st.sampled_from(("ordered_at", "state", "customer_id")), descending=st.sampled_from((None, True, False))
)
def test_sst_val107_fires_for_every_non_additive_declaration(dimension: str, descending: bool | None) -> None:
    snapshot = metric(
        "latest", "SUM({{ ref('orders', 'total') }})", non_additive=(NonAdditiveDef(dimension, descending=descending),)
    )
    regular = metric("uses_latest", "SUM({{ ref('orders', 'cost') }}) - {{ metric('latest') }}")
    [found] = metric_findings(snapshot, regular, code="SST-VAL107")
    assert found.subject == "metric:uses_latest"
