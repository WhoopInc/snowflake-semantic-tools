"""SST-VAL118: a non-additive dimension names a missing table or dimension."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.authored import NonAdditiveDef
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val118_fires() -> None:
    bad = metric("latest", "SUM({{ ref('orders', 'total') }})", non_additive=(NonAdditiveDef("nope"),))
    [found] = metric_findings(bad, code="SST-VAL118")
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'latest': non_additive_dimensions names nope, which does not resolve"
    assert found.subject == "metric:latest"


def test_sst_val118_silent() -> None:
    good = metric("latest", "SUM({{ ref('orders', 'total') }})", non_additive=(NonAdditiveDef("ordered_at"),))
    assert metric_findings(good, code="SST-VAL118") == []
