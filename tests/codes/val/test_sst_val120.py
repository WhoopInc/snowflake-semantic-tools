"""SST-VAL120: a non-additive or window order entry declares a sort direction and no null order."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.defs import NonAdditiveDef
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val120_fires() -> None:
    half = metric(
        "latest", "SUM({{ ref('orders', 'total') }})", non_additive=(NonAdditiveDef("ordered_at", descending=True),)
    )
    [found] = metric_findings(half, code="SST-VAL120")
    assert found.severity is Severity.WARNING
    assert found.message == "metric 'latest' declares sort_direction with no null_order"
    assert found.subject == "metric:latest"


def test_sst_val120_silent() -> None:
    whole = metric(
        "latest",
        "SUM({{ ref('orders', 'total') }})",
        non_additive=(NonAdditiveDef("ordered_at", descending=True, nulls_first=False),),
    )
    unsorted = metric("earliest", "SUM({{ ref('orders', 'cost') }})", non_additive=(NonAdditiveDef("ordered_at"),))
    assert metric_findings(whole, unsorted, code="SST-VAL120") == []
