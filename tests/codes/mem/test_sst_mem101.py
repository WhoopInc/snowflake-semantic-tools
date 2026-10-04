"""SST-MEM101: a member's expression reaches a table its declared `tables:` leaves out."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem101_fires() -> None:
    [diagnostic, *_] = coded(
        membership(member("metric", "m", ("orders",), "COUNT({{ ref('customers', 'id') }})")).diagnostics, "SST-MEM101"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:m: expression reaches customers, beyond its declared tables:"
    assert diagnostic.subject == "metric:m"


def test_sst_mem101_silent() -> None:
    assert (
        coded(
            membership(member("metric", "m", ("orders",), "COUNT({{ ref('orders', 'id') }})")).diagnostics, "SST-MEM101"
        )
        == []
    )
