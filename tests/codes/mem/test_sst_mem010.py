"""SST-MEM010: a member attaches nowhere because the closest view reaches one of its tables only by a join."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem010_fires() -> None:
    [diagnostic, *_] = coded(
        membership(member("metric", "m", ("products", "customers")), joins=(("orders", "customers"),)).diagnostics,
        "SST-MEM010",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:m declares 'customers', reachable from semantic_view:menu only through a join"
    assert diagnostic.subject == "metric:m"


def test_sst_mem010_silent() -> None:
    assert coded(membership(member("metric", "m", ("products", "customers"))).diagnostics, "SST-MEM010") == []
