"""SST-PLN022: two changes carry one artifact key, so no order can be computed."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.plan.order import order_changes
from tests.helpers.artifact_builders import change, rendered


def test_sst_pln022_fires() -> None:
    first = change(rendered("SALES"))
    again = replace(first, key="semantic_view:SALES")
    ordered, diagnostic = order_changes((first, again))
    assert ordered == () and diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN022", Severity.ERROR)
    assert diagnostic.message == (
        "change order could not be computed: 'semantic_view:sales' and 'semantic_view:SALES' are one artifact key"
    )


def test_sst_pln022_silent() -> None:
    ordered, diagnostic = order_changes((change(rendered("SALES")), change(rendered("ORDERS"))))
    assert diagnostic is None and len(ordered) == 2
