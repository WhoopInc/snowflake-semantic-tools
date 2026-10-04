"""SST-MEM012: a member declares no `tables:`, so its tables are inferred from its expression."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem012_fires() -> None:
    [diagnostic, *_] = coded(
        membership(member("metric", "m", None, "COUNT({{ ref('orders', 'id') }})")).diagnostics, "SST-MEM012"
    )
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "metric:m: tables inferred as orders"
    assert diagnostic.subject == "metric:m"


def test_sst_mem012_silent() -> None:
    assert (
        coded(
            membership(member("metric", "m", ("orders",), "COUNT({{ ref('orders', 'id') }})")).diagnostics, "SST-MEM012"
        )
        == []
    )
