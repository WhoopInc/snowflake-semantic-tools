"""SST-MEM103: what each view that will be built holds once members attach, by member type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem103_fires() -> None:
    result = membership(member("metric", "m", ("orders",)), member("filter", "f", ("products",)))
    menu, sales = coded(result.diagnostics, "SST-MEM103")
    assert menu.severity is Severity.INFO
    assert (menu.message, menu.subject) == ("semantic_view:menu: 1 metric, 1 filter", "semantic_view:menu")
    assert sales.message == "semantic_view:sales: 1 metric"


def test_sst_mem103_silent() -> None:
    # A view that will not be built is not reported on.
    result = membership(member("metric", "m", ("orders",)), reported_views=frozenset())
    assert coded(result.diagnostics, "SST-MEM103") == []
