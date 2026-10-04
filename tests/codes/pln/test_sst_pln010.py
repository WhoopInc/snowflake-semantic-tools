"""SST-PLN010: a planned write's schema does not exist in the target."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import plan, view


def test_sst_pln010_fires() -> None:
    missing = frozenset((("DB", "SCH"),))
    planned = plan((view("sales"), view("orders")), preflight=Preflight("dev", "DEPLOYER", missing_schemas=missing))
    [diagnostic] = coded(planned.diagnostics, "SST-PLN010")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "schema DB.SCH does not exist or is not authorised"
    assert {change.action for change in planned.changes} == {Action.BLOCKED}


def test_sst_pln010_silent() -> None:
    missing = frozenset((("DB", "OTHER"),))
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_schemas=missing))
    assert coded(planned.diagnostics, "SST-PLN010") == []
