"""SST-PLN008: the deploying role lacks the privilege a planned create needs on its schema."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import plan, view


def test_sst_pln008_fires() -> None:
    lacking = MappingProxyType({("DB", "SCH"): ("CREATE SEMANTIC VIEW",)})
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_privileges=lacking))
    [diagnostic] = coded(planned.diagnostics, "SST-PLN008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "DEPLOYER lacks CREATE SEMANTIC VIEW on DB.SCH"
    assert planned.changes[0].action is Action.BLOCKED


def test_sst_pln008_silent() -> None:
    # A privilege lacking on another schema does not concern a create in this one.
    lacking = MappingProxyType({("DB", "OTHER"): ("CREATE SEMANTIC VIEW",)})
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_privileges=lacking))
    assert coded(planned.diagnostics, "SST-PLN008") == []


def test_sst_pln008_fires_once_for_an_absent_state_table_the_role_cannot_create() -> None:
    lacking = MappingProxyType({("DB", "STATE"): ("CREATE TABLE",)})
    absent = (QualifiedName.parse("DB.STATE.SST_STATE"), QualifiedName.parse("DB.STATE.SST_STATE_LOCK"))
    preflight = Preflight("dev", "DEPLOYER", missing_privileges=lacking, absent_state_tables=absent)
    planned = plan((view("sales"),), preflight=preflight)
    [diagnostic] = coded(planned.diagnostics, "SST-PLN008")
    assert diagnostic.message == "DEPLOYER lacks CREATE TABLE on DB.STATE"
    assert diagnostic.subject is None and planned.changes[0].action is Action.CREATE


def test_sst_pln008_silent_for_an_absent_state_table_when_the_plan_writes_nothing() -> None:
    lacking = MappingProxyType({("DB", "STATE"): ("CREATE TABLE",)})
    absent = (QualifiedName.parse("DB.STATE.SST_STATE"),)
    preflight = Preflight("dev", "DEPLOYER", missing_privileges=lacking, absent_state_tables=absent)
    assert coded(plan((), preflight=preflight).diagnostics, "SST-PLN008") == []


def test_sst_pln008_silent_when_the_state_tables_exist() -> None:
    lacking = MappingProxyType({("DB", "STATE"): ("CREATE TABLE",)})
    planned = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_privileges=lacking))
    assert coded(planned.diagnostics, "SST-PLN008") == []
