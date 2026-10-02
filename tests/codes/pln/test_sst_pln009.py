"""SST-PLN009: a planned create's target name is already taken by an object observation did not list."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.plan_codes import only, plan, view


def test_sst_pln009_fires() -> None:
    sales = view("sales")
    planned = plan((sales,), preflight=Preflight("dev", "DEPLOYER", occupied=frozenset((sales.key,))))
    [diagnostic] = only(planned, "SST-PLN009")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:sales: DB.SCH.SALES already exists"
    assert planned.changes[0].action is Action.CREATE


def test_sst_pln009_silent() -> None:
    assert only(plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER")), "SST-PLN009") == []
