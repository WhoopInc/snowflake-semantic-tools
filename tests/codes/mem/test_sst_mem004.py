"""SST-MEM004: a member's `tables:` lists one table twice."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem004_fires() -> None:
    [diagnostic, *_] = coded(membership(member("metric", "m", ("orders", "orders"))).diagnostics, "SST-MEM004")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:m lists table 'orders' more than once"
    assert diagnostic.subject == "metric:m"


def test_sst_mem004_silent() -> None:
    assert coded(membership(member("metric", "m", ("orders", "customers"))).diagnostics, "SST-MEM004") == []
