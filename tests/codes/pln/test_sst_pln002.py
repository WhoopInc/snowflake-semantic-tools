"""SST-PLN002: an object of a different type holds the artifact's target name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import live, plan, view


def test_sst_pln002_fires() -> None:
    sales = view("sales")
    planned = plan((sales,), observed=(live(sales, object_type="AGENT"),))
    [diagnostic] = coded(planned.diagnostics, "SST-PLN002")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:sales: DB.SCH.SALES exists as a AGENT"
    assert planned.changes[0].action is Action.BLOCKED


def test_sst_pln002_silent() -> None:
    sales = view("sales")
    assert coded(plan((sales,), observed=()).diagnostics, "SST-PLN002") == []
