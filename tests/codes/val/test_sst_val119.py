"""SST-VAL119: a metric's two or more non-additive dimensions are surfaced in their order."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.authored import NonAdditiveDef
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val119_fires() -> None:
    entries = (NonAdditiveDef("ordered_at", descending=True, nulls_first=True), NonAdditiveDef("state"))
    [found] = metric_findings(
        metric("latest", "SUM({{ ref('orders', 'total') }})", non_additive=entries), code="SST-VAL119"
    )
    assert found.severity is Severity.INFO
    assert (
        found.message
        == "metric 'latest': effective non_additive_dimensions order is ORDERED_AT DESC NULLS FIRST, STATE"
    )
    assert found.subject == "metric:latest"


def test_sst_val119_silent() -> None:
    one = metric("latest", "SUM({{ ref('orders', 'total') }})", non_additive=(NonAdditiveDef("ordered_at"),))
    assert metric_findings(one, code="SST-VAL119") == []
