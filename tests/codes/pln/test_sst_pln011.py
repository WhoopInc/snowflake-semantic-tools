"""SST-PLN011: a planned write's database does not exist in the target."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.plan_codes import only, plan, view


def test_sst_pln011_fires() -> None:
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_databases=frozenset(("DB",))))
    [diagnostic] = only(planned, "SST-PLN011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "database DB does not exist or is not authorised"
    assert only(planned, "SST-PLN010") == []
    assert planned.changes[0].action is Action.BLOCKED


def test_sst_pln011_silent() -> None:
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_databases=frozenset(("OTHER",))))
    assert only(planned, "SST-PLN011") == []
