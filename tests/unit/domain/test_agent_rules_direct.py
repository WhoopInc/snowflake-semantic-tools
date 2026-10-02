"""The agent and agent-tool rules, called directly: keys, search tools, signatures, overrides, profile."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.validate.agent import agent_rules, token_budget
from snowflake_semantic_tools.domain.validate.agent_live import signature_disagreement
from snowflake_semantic_tools.domain.validate.agent_tool import (
    misplaced_keys,
    overrides,
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
    member = replace(search_member(warehouse="WH", id_column="ID", title_column="TITLE"), columns=())
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


def test_a_token_budget_the_agent_sets_itself_is_reported_unless_documented() -> None:
    assert [item.code for item in token_budget(agent(budget_tokens=1000))] == ["SST-VAL547"]
    assert token_budget(agent(budget_tokens=1000, budget_tokens_documented=True)) == ()
    assert token_budget(agent()) == ()


def test_a_search_tools_own_columns_and_its_members_both_count() -> None:
    member = search_member()
    tool = search_tool(columns_and_descriptions=MappingProxyType({"title": {}}))
    assert [item.context["value"] for item in overrides(MODEL, tool, "docs", member)][-1:] == [
        "columns_and_descriptions"
    ]
