"""SST-PLN007: the preflight read shows a relation a write references is missing from the target."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.plan_codes import only, plan, view


def test_sst_pln007_fires() -> None:
    sales = view("sales", relations=("DB.SCH.ORDERS",))
    missing = MappingProxyType({sales.key: (QualifiedName.parse("DB.SCH.ORDERS"),)})
    planned = plan((sales,), preflight=Preflight("dev", "DEPLOYER", missing_relations=missing))
    [diagnostic] = only(planned, "SST-PLN007")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:sales references DB.SCH.ORDERS, absent from target 'dev'"
    assert diagnostic.subject == sales.key
    assert planned.changes[0].action is Action.BLOCKED


def test_sst_pln007_silent() -> None:
    sales = view("sales", relations=("DB.SCH.ORDERS",))
    planned = plan((sales,), preflight=Preflight("dev", "DEPLOYER"))
    assert only(planned, "SST-PLN007") == [] and planned.changes[0].action is Action.CREATE
