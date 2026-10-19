"""`apply.create_schemas`: an absent schema in a database that exists is planned, not blocked."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.state.saved_plan import SavedPlan
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import plan, view

_STATE = QualifiedName.parse("SST.STATE.SST_STATE")


def _preflight(**facts: object) -> Preflight:
    return replace(Preflight("dev", "DEPLOYER", create_schemas=True), **facts)  # type: ignore[arg-type]


def test_an_absent_schema_is_a_schema_to_create_ahead_of_the_writes_into_it() -> None:
    preflight = _preflight(
        missing_schemas=frozenset({("DB", "SCH"), ("DB", "OTHER")}),
        absent_state_tables=(_STATE,),
        missing_state_schemas=frozenset({("SST", "STATE")}),
    )
    planned = plan((view("sales"), view("orders", schema="other"), view("menu", schema="kept")), preflight=preflight)
    assert coded(planned.diagnostics, "SST-PLN010") == []
    assert {change.action for change in planned.changes} == {Action.CREATE}
    # The state schema first, as apply takes its lock before anything else, then in change order.
    assert [scope.sql for scope in planned.schemas] == ["SST.STATE", "DB.OTHER", "DB.SCH"]
    assert planned.plan_id != plan((view("sales"), view("orders", schema="other"))).plan_id


def test_a_missing_database_still_blocks_and_nothing_is_created_with_the_setting_off() -> None:
    missing = frozenset({("DB", "SCH")})
    no_database = plan(
        (view("sales"),), preflight=_preflight(missing_databases=frozenset({"DB"}), missing_schemas=missing)
    )
    assert [item.code for item in no_database.diagnostics] == ["SST-PLN011"] and no_database.schemas == ()
    off = plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", missing_schemas=missing))
    assert off.schemas == () and coded(off.diagnostics, "SST-PLN010")
    # A plan that writes nothing creates no state schema either.
    idle = plan(
        (), preflight=_preflight(absent_state_tables=(_STATE,), missing_state_schemas=frozenset({("SST", "STATE")}))
    )
    assert idle.schemas == ()


def test_a_saved_plan_records_its_schemas_and_refuses_a_fresh_plan_that_creates_others() -> None:
    planned = plan((view("sales"),), preflight=_preflight(missing_schemas=frozenset({("DB", "SCH")})))
    saved = SavedPlan.from_changeset(planned)
    assert saved.schemas == ("DB.SCH",) and saved.as_dict()["schemas"] == ["DB.SCH"]
    assert SavedPlan.from_dict(saved.as_dict()) == saved
    fresh = SavedPlan.from_changeset(replace(planned, schemas=()))
    assert "schemas" not in fresh.as_dict()
    mismatch = saved.check_applicable(fresh, source="plan.json")
    assert mismatch is not None and "the schemas it creates" in mismatch.diagnostic.message
    assert saved.check_applicable(saved, source="plan.json") is None
    assert planned.schemas == (SchemaScope.parse("DB.SCH"),)


def test_a_state_table_in_a_schema_that_exists_creates_no_schema() -> None:
    preflight = _preflight(missing_schemas=frozenset({("DB", "SCH")}), absent_state_tables=(_STATE,))
    assert [scope.sql for scope in plan((view("sales"),), preflight=preflight).schemas] == ["DB.SCH"]
