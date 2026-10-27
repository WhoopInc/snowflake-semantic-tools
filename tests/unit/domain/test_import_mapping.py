"""Mapping live objects to project files: where they go, and what an agent, profile and view become."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from snowflake_semantic_tools.domain.importing.agent import (
    AgentPlacement,
    LiveAgent,
    import_agent,
    import_cowork_profile,
)
from snowflake_semantic_tools.domain.importing.placement import Destination, destination, inherited
from snowflake_semantic_tools.domain.importing.references import Placed
from snowflake_semantic_tools.domain.importing.semantic_view import ViewPlacement, import_semantic_view
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import TARGET, dbt_column, dbt_model, index, view_document

_SALES = Placed("semantic_view", "sales", QualifiedName.parse("DB.SV.SALES"), ())
_ROUTES = {"+schema": "AGENTS", "sales": {"+database": "SALES_DB"}, "cx": {"+database": "CX_DB"}}


def test_an_object_goes_in_the_folder_whose_routes_place_it_where_it_is() -> None:
    live = QualifiedName.parse("CX_DB.AGENTS.BOT")
    assert destination(_ROUTES, TARGET, live, ()) == Destination(("cx",))
    # A folder already holding artifacts there comes first.
    assert destination({**_ROUTES, "help": {"+database": "CX_DB"}}, TARGET, live, (("help",),)) == Destination(
        ("help",)
    )
    # Nothing routes to it: the file names its own location.
    assert destination(_ROUTES, TARGET, QualifiedName.parse("ELSEWHERE.X.BOT"), ()).overridden


def test_a_folder_inherits_its_routes_settings_with_templates_rendered() -> None:
    block = {"+warehouse": "{{ target.warehouse }}", "sales": {"+query_timeout": 60}}
    assert inherited(block, ("sales",), TARGET) == {"warehouse": "WH", "query_timeout": 60}


def _agent(spec: Mapping[str, Any]) -> LiveAgent:
    return LiveAgent(QualifiedName.parse("DB.S.BOT"), "Bot.", {"display_name": "Bot"}, False, spec)


def _placed(**inherited_settings: object) -> AgentPlacement:
    return AgentPlacement("agents", "tools", Destination(("sales",)), inherited_settings)


def test_an_agent_writes_only_the_settings_its_folder_does_not_give_it() -> None:
    spec = {
        "models": {"orchestration": "claude-sonnet-4-6"},
        "orchestration": {"budget": {"seconds": 120, "tokens": 9000}},
        "instructions": {"orchestration": "Use SALES.\nNever guess.", "response": "Be brief."},
        "tools": [],
    }
    imported = import_agent(_agent(spec), index(), _placed(orchestration_model="claude-sonnet-4-6", budget_seconds=120))
    document = imported.files[0].document or {}
    assert document["config"] == {"budget_tokens": 9000}
    instructions = document["spec"]["instructions"]  # type: ignore[index]
    assert instructions == {"orchestration": "{{ file('instructions/orchestration.md') }}", "response": "Be brief."}
    assert [file.path for file in imported.files] == [
        "agents/sales/bot/agent.yml",
        "agents/sales/bot/instructions/orchestration.md",
    ]
    assert imported.files[1].text == "Use SALES.\nNever guess.\n"


def test_a_search_tool_over_an_unknown_service_gets_a_referencing_member() -> None:
    spec = {
        "tools": [{"tool_spec": {"type": "cortex_search", "name": "menu_search", "description": "Menu."}}],
        "tool_resources": {
            "menu_search": {
                "search_service": "DB.TOOLS.MENU_SEARCH",
                "max_results": 5,
                "id_column": "PRODUCT_ID",
                "columns_and_descriptions": {"NAME": {"type": "string", "searchable": True, "filterable": False}},
            }
        },
    }
    imported = import_agent(_agent(spec), index(), _placed())
    tool = (imported.files[0].document or {})["spec"]["tools"][0]  # type: ignore[index]
    assert tool == {
        "type": "cortex_search",
        "name": "menu_search",
        "search_service": "{{ tool('menu_search') }}",
        "description": "Menu.",
        "max_results": 5,
    }
    member = imported.files[1]
    assert member.path == "tools/sales/menu_search/menu_search.yml"
    assert member.document == {
        "tool": {
            "name": "menu_search",
            "type": "cortex_search_service",
            "description": "Menu.",
            "id_column": "product_id",
            "columns": [{"column": "name", "type": "string", "searchable": True, "filterable": False}],
            "relations": {"dev": "DB.TOOLS.MENU_SEARCH"},
        }
    }


def test_a_skill_the_project_publishes_is_loaded_by_skill_and_a_stage_one_by_its_folder() -> None:
    spec = {
        "tools": [],
        "skills": [
            {"name": "jaffle", "source": {"type": "CORTEX_EXTENSION", "path": "DB.SK.JAFFLE", "version": "V$1"}},
            {"name": "check", "source": {"type": "STAGE", "path": "@DB.SK.SRC/skills/ops/check/"}},
        ],
    }
    project = index(
        Placed("skill", "jaffle", QualifiedName.parse("DB.SK.JAFFLE"), ()), skill_names=frozenset({"jaffle"})
    )
    imported = import_agent(_agent(spec), project, _placed())
    skills = (imported.files[0].document or {})["spec"]["skills"]  # type: ignore[index]
    assert skills == [
        {"source": {"type": "CORTEX_EXTENSION", "path": "{{ skill('jaffle') }}"}},
        {"source": {"type": "CORTEX_EXTENSION", "path": "{{ skill('check') }}"}},
    ]
    assert [(item.artifact_type, item.name) for item in imported.dependencies] == [
        ("skill", "@DB.SK.SRC/skills/ops/check/")
    ]


def test_a_profile_routes_to_project_agents_and_references_the_rest() -> None:
    spec = {
        "tools": [{"tool_spec": {"type": "agent_router", "name": "router"}}],
        "tool_resources": {
            "router": {
                "subagents": [
                    {"display_name": "Sales", "source": {"fqn": "DB.S.SALES_AGENT", "sample_questions": ["Revenue?"]}},
                    {"description": "Delivery.", "source": {"fqn": "PARTNER.D.DELIVERY_AGENT"}},
                ]
            }
        },
    }
    project = index(Placed("agent", "sales_agent", QualifiedName.parse("DB.S.SALES_AGENT"), ()))
    imported = import_cowork_profile(_agent(spec), project, _placed())
    subagents = (imported.files[0].document or {})["spec"]["subagents"]  # type: ignore[index]
    assert subagents == [
        {"agent": "{{ agent('sales_agent') }}", "display_name": "Sales", "sample_questions": ["Revenue?"]},
        {"agent": "{{ tool('delivery_agent') }}", "description": "Delivery."},
    ]
    assert imported.files[1].path == "tools/sales/delivery_agent/delivery_agent.yml"


def test_an_analyst_tool_writes_its_view_by_name_when_the_project_publishes_it() -> None:
    spec = {
        "tools": [{"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "SALES", "description": "Orders."}}],
        "tool_resources": {"SALES": {"semantic_view": "DB.SV.SALES", "execution_environment": {"query_timeout": 30}}},
    }
    imported = import_agent(_agent(spec), index(_SALES), _placed(query_timeout=60))
    tool = (imported.files[0].document or {})["spec"]["tools"][0]  # type: ignore[index]
    assert tool == {
        "type": "cortex_analyst_text_to_sql",
        "semantic_view": "{{ semantic_view('sales') }}",
        "description": "Orders.",
        "query_timeout": 30,
    }
    assert imported.dependencies == ()


_VIEW = QualifiedName.parse("DB.SV.ORDERS_VIEW")
_PLACE = ViewPlacement("semantic_models", Destination(("sales",)))


def _view_project(*columns: object, **fields: object) -> object:
    model = dbt_model("orders", "DB.MARTS.ORDERS", *columns)
    return index(models={("DB", "MARTS", "ORDERS"): "orders"}, dbt_models={"orders": model}, **fields)


def test_a_view_becomes_its_file_its_members_and_its_models_metadata() -> None:
    document = view_document(
        {"name": "STATE", "expr": "ORDERS.STATE", "synonyms": ["status"], "sample_values": ["open"], "is_enum": True},
        {"name": "IS_OPEN", "expr": "ORDERS.STATE = 'open'", "labels": ["filter"], "description": "Open orders."},
    )
    document["tables"][0]["metrics"] = [{"name": "ORDER_COUNT", "expr": "COUNT(ORDERS.ID)", "description": "Orders."}]
    document["metrics"] = [{"name": "PER_ORDER", "expr": "DIV0(ORDERS.ORDER_COUNT, 2)"}]
    imported = import_semantic_view(_VIEW, document, _view_project(), _PLACE)  # type: ignore[arg-type]
    by_path = {file.path: file.document for file in imported.files}
    assert by_path["semantic_models/sales/orders_view.yml"] == {
        "semantic_view": {"name": "orders_view", "tables": ["{{ ref('orders') }}"]}
    }
    assert by_path["semantic_models/sales/filters/is_open.yml"] == {
        "filter": {
            "name": "is_open",
            "description": "Open orders.",
            "expr": "{{ ref('orders', 'state') }} = 'open'",
            "labels": ["filter"],
        }
    }
    assert by_path["semantic_models/sales/metrics/per_order.yml"] == {
        "metric": {"name": "per_order", "expr": "DIV0({{ metric('order_count') }}, 2)"}
    }
    [edit] = imported.model_edits
    assert (edit.model, edit.patch_file, edit.description) == ("orders", "models/orders.yml", "Orders.")
    [column] = edit.columns
    assert column.meta == (
        ("column_type", "dimension"),
        ("synonyms", ("status",)),
        ("sample_values", ("open",)),
        ("is_enum", True),
    )


def test_a_member_the_project_declares_is_not_written_again() -> None:
    document = view_document()
    document["tables"][0]["metrics"] = [{"name": "ORDER_COUNT", "expr": "COUNT(ORDERS.ID)"}]
    project = _view_project(members={"metric": frozenset({"order_count"})})
    imported = import_semantic_view(_VIEW, document, project, _PLACE)  # type: ignore[arg-type]
    assert [file.path for file in imported.files] == ["semantic_models/sales/orders_view.yml"]


def test_a_view_lists_the_instruction_blocks_another_view_renders_its_text_from() -> None:
    document = view_document()
    document["module_custom_instructions"] = {"sql_generation": "Round to cents."}
    project = _view_project(instruction_sets=(("Round to  cents.", "", ("conventions",)),))
    imported = import_semantic_view(_VIEW, document, project, _PLACE)  # type: ignore[arg-type]
    body = (imported.files[0].document or {})["semantic_view"]
    assert body["custom_instructions"] == ["{{ custom_instructions('conventions') }}"]  # type: ignore[index]


def test_a_typed_column_the_live_view_leaves_out_is_excluded() -> None:
    project = _view_project(dbt_column("state", column_type="dimension"), dbt_column("tax", column_type="fact"))
    document = view_document({"name": "STATE", "expr": "ORDERS.STATE"})
    imported = import_semantic_view(_VIEW, document, project, _PLACE)  # type: ignore[arg-type]
    body = (imported.files[0].document or {})["semantic_view"]
    assert body["exclude_columns"] == ["{{ ref('orders', 'tax') }}"]  # type: ignore[index]


def test_a_computed_dimension_has_no_form_and_is_reported() -> None:
    document = view_document({"name": "TOTAL", "expr": "ORDERS.A + ORDERS.B"})
    imported = import_semantic_view(_VIEW, document, _view_project(), _PLACE)  # type: ignore[arg-type]
    assert [item.code for item in imported.diagnostics] == ["SST-IMP013"]
    assert imported.model_edits == () or not imported.model_edits[0].columns


def test_a_column_shown_by_its_bare_name_is_that_column() -> None:
    # Measured on a live account: some views show a plain fact as `NAME`, not `TABLE.NAME`.
    document = view_document({"name": "TAX", "expr": "TAX", "synonyms": ["sales tax"]})
    imported = import_semantic_view(_VIEW, document, _view_project(), _PLACE)  # type: ignore[arg-type]
    assert imported.diagnostics == ()
    [edit] = imported.model_edits
    assert [column.column for column in edit.columns] == ["tax"]
