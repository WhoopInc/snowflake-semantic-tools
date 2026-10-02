"""SST-PLN008: the deploying role lacks the privilege a planned create needs on its schema."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.plan_codes import only, plan, view


def test_sst_pln008_fires() -> None:
    lacking = MappingProxyType({("DB", "SCH"): ("CREATE SEMANTIC VIEW",)})
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_privileges=lacking))
    [diagnostic] = only(planned, "SST-PLN008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "DEPLOYER lacks CREATE SEMANTIC VIEW on DB.SCH"
    assert planned.changes[0].action is Action.BLOCKED


def test_sst_pln008_silent() -> None:
    # A privilege lacking on another schema does not concern a create in this one.
    lacking = MappingProxyType({("DB", "OTHER"): ("CREATE SEMANTIC VIEW",)})
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_privileges=lacking))
    assert only(planned, "SST-PLN008") == []
