"""The agent and agent-tool rules, called directly: keys, search tools, signatures, overrides, profile."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.validate.agent import agent_rules, token_budget
from snowflake_semantic_tools.domain.validate.agent_live import names_object, signature_disagreement
from snowflake_semantic_tools.domain.validate.agent_tool import (
    misplaced_keys,
    overrides,
    search_tool_columns,
    search_tool_problems,
    signature_mismatch,
)
from tests.helpers.agent_builders import (
    agent,
    builtin_tool,
    generic_tool,
    parameter,
    procedure_member,
    search_member,
    search_tool,
)

MODEL = agent()


def test_a_key_another_tool_type_takes_is_misplaced_and_a_builtin_may_carry_passthrough() -> None:
    misplaced = generic_tool(declared_keys=("type", "name", "semantic_view", "bogus"))
    assert [item.context["key"] for item in misplaced_keys(MODEL, misplaced, "lookup")] == ["semantic_view"]
    chart = builtin_tool(declared_keys=("type", "description", "passthrough", "warehouse"))
    assert [item.context["key"] for item in misplaced_keys(MODEL, chart, "chart")] == ["warehouse"]


def test_a_search_tool_needs_both_preview_keys_sound_descriptors_and_filterable_filter_columns() -> None:
    columns = {
        "title": {"type": "string", "searchable": True, "filterable": True},
        "body": "text",
        "when": {"type": "number", "searchable": True, "filterable": False},
        "kind": {"type": "datetime", "searchable": "yes", "filterable": False},
    }
    tool = search_tool(stage_path="@DB.S.DOCS", filter={"@and": [{"@eq": {"title": "x"}}, {"@eq": {"kind": "y"}}]})
    found = search_tool_problems(MODEL, tool, "docs", columns)
    assert [(item.code, item.context.get("column") or item.context.get("field")) for item in found] == [
        ("SST-VAL522", "stage_path"),
        ("SST-VAL524", "body"),
        ("SST-VAL524", "when"),
        ("SST-VAL524", "kind"),
        ("SST-VAL523", "kind"),
    ]
    reverse = search_tool_problems(MODEL, search_tool(relative_path_column="PATH"), "docs", {})
    assert [item.context["field"] for item in reverse] == ["relative_path_column"]


def test_a_member_signature_must_match_the_calling_tools_input_schema() -> None:
    member = procedure_member(signature=(parameter("ORDER_ID", "NUMBER(38,0)"), parameter("NOTE", "VARCHAR")))
    matching = {"properties": {"order_id": {"type": "integer"}, "Note": {"type": "string"}}}
    assert signature_mismatch(MODEL, "lookup", member, matching) is None
    wrong = {"properties": {"order_id": {"type": "string"}, "note": {"type": "string"}}}
    found = signature_mismatch(MODEL, "lookup", member, wrong)
    assert found is not None and found.code == "SST-VAL607"
    assert found.context["expected"] == "(order_id string, note string)"
    assert signature_mismatch(MODEL, "lookup", procedure_member(), wrong) is None
    assert signature_mismatch(MODEL, "lookup", member, {"properties": []}) is None


def test_a_tool_value_over_its_members_is_reported_field_by_field() -> None:
    member = replace(
        search_member(config=MappingProxyType({"warehouse": "WH"}), id_column="ID", title_column="TITLE"), columns=()
    )
    tool = search_tool(warehouse="OTHER_WH", id_column="ID", title_column="HEADLINE")
    assert [item.context["value"] for item in overrides(MODEL, tool, "docs", member)] == ["warehouse", "title_column"]


def test_a_live_signature_disagrees_with_the_schema_in_arity_or_type() -> None:
    schema = {"properties": {"order_id": {"type": "integer"}}}
    assert signature_disagreement(("NUMBER",), schema) is None
    assert signature_disagreement(("NUMBER", "VARCHAR"), schema) == ("(NUMBER, VARCHAR)", "(order_id integer)")
    assert signature_disagreement(("NUMBER",), {"properties": None}) is None


def test_the_profile_colour_must_be_a_name_or_a_token_and_a_deprecated_agent_keeps_no_alias() -> None:
    model = agent(profile=AgentProfile("Sales", None, "#ff0000"), deprecated=True, alias="promoted")
    found = agent_rules(model, frozenset(("claude",)), ())
    assert [(item.code, item.context.get("field")) for item in found] == [("SST-VAL548", "color"), ("SST-VAL550", None)]
    token = agent(profile=AgentProfile("Sales", "Icon", "var(--brand-1)"))
    assert agent_rules(token, frozenset(("claude",)), (), frozenset(("Icon",))) == []


def test_a_profile_set_profile_would_refuse_is_reported_by_its_rendered_size() -> None:
    # The JSON SST sends escapes non-ASCII text, six bytes a character, and that is what is measured.
    model = agent(profile=AgentProfile("\u00e9" * 500))
    [found] = agent_rules(model, frozenset(("claude",)), ())
    assert (found.code, found.context["size"]) == ("SST-VAL558", 3019)
    assert agent_rules(agent(profile=AgentProfile("e" * 981)), frozenset(("claude",)), ()) == []


def test_a_token_budget_the_agent_sets_itself_is_reported_unless_documented() -> None:
    own = MappingProxyType({"budget_tokens": Setting("budget_tokens", 1000, "config")})
    routed = MappingProxyType({"budget_tokens": Setting("budget_tokens", 1000, "root")})
    assert [item.code for item in token_budget(agent(budget_tokens=1000, settings=own))] == ["SST-VAL547"]
    assert token_budget(agent(budget_tokens=1000, settings=own, budget_tokens_documented=True)) == ()
    assert token_budget(agent(budget_tokens=1000, settings=routed)) == ()
    assert token_budget(agent()) == ()


def test_a_search_tools_own_columns_and_its_members_both_count() -> None:
    member = search_member()
    tool = search_tool(columns=({"column": "title"},))
    assert [item.context["value"] for item in overrides(MODEL, tool, "docs", member)][-1:] == ["columns"]


def test_a_search_tools_own_columns_are_read_as_its_member_reads_its_own() -> None:
    column = "{{ ref('product_docs', 'doc_name') }}"
    entries = (
        "plain",
        {"description": "No column."},
        {"column": column, "description": "Title.", "type": "string"},
        {"column": "doc_id"},
        {"column": "{{ ref('notes', 'doc_id') }}"},
    )
    tool = search_tool(columns=entries, id_column="{{ ref('product_docs', 'doc_id') }}", title_column="doc_name")
    resolved, columns, found = search_tool_columns(MODEL, tool, "docs", search_member())
    assert columns == {"DOC_NAME": {"description": "Title.", "type": "string"}}
    assert (resolved.id_column, resolved.title_column, resolved.relative_path_column) == ("doc_id", None, None)
    assert [(item.code, item.context.get("field") or item.context.get("detail")) for item in found] == [
        ("SST-VAL524", "the entry is not a mapping"),
        ("SST-VAL524", "it names no column"),
        ("SST-REF050", "tools.docs.columns[3].column"),
        ("SST-VAL622", "tools.docs.columns[4].column"),
        ("SST-REF050", "tools.docs.title_column"),
    ]
    referenced = replace(procedure_member(), type="cortex_search_service")
    resolved, columns, found = search_tool_columns(MODEL, search_tool(columns=({"column": "body"},)), "d", referenced)
    assert (columns, found) == ({"BODY": {}}, [])


def test_a_live_specification_names_an_object_by_its_qualified_name_anywhere_in_it() -> None:
    view = QualifiedName.parse("DB.S.ORDERS")
    assert names_object({"tool_resources": {"analyst": {"semantic_view": "db.s.orders"}}}, view)
    assert names_object({"tools": [{"name": "x"}, ["DB.S.ORDERS"]]}, view)
    assert not names_object({"tool_resources": {"analyst": {"semantic_view": "DB.S.CUSTOMERS"}}}, view)
    assert not names_object({"limit": 3, "flag": None, "text": "no dots here"}, view)
    assert not names_object({"unbalanced": '"DB.S.ORDERS', "four": "A.B.C.D"}, view)
    assert not names_object('"DB"."S"."orders"', view)
