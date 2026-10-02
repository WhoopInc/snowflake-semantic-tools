"""The agent and tool rule helpers' edge cases: shapes the per-code tests do not reach."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentProfile
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.tool import render_tool
from snowflake_semantic_tools.domain.validate.agent import agent_rules
from snowflake_semantic_tools.domain.validate.agent_live import (
    first_difference,
    live_signature,
    live_spec,
    signature_disagreement,
)
from snowflake_semantic_tools.domain.validate.agent_spec import resource_keys
from snowflake_semantic_tools.domain.validate.agent_tool import (
    filter_columns,
    input_type_matches,
    schema_type,
    search_tool_problems,
)
from tests.helpers.agent_builders import agent, search_member, search_tool

MODEL = AgentModel("sales_agent", Origin("agent.yml"), ("agent.yml",))


def test_an_avatar_outside_the_allowlist_warns() -> None:
    model = agent("sales_agent", profile=AgentProfile("Sales", "Rocket", "blue"))
    [diagnostic] = agent_rules(model, frozenset(("claude",)), (), frozenset(("Icon",)))
    assert diagnostic.message == "agent 'sales_agent': avatar 'Rocket' is not in snowflake.profile.avatar_allowlist"


def test_a_tool_resource_key_naming_a_non_builtin_tool_is_kept() -> None:
    document = {
        "tools": [{"tool_spec": {"type": "cortex_search", "name": "docs"}}, "not a tool", {"tool_spec": {}}],
        "tool_resources": {"docs": {}},
    }
    assert resource_keys(MODEL, document) == []
    assert resource_keys(MODEL, {"tool_resources": "not a mapping"}) == []


def test_a_passthrough_resource_entry_for_a_builtin_is_refused() -> None:
    document = {
        "tools": [{"tool_spec": {"type": "data_to_chart", "name": "data_to_chart"}}],
        "tool_resources": {"data_to_chart": {"region": "us"}},
    }
    [diagnostic] = resource_keys(MODEL, document)
    assert diagnostic.message == "agent 'sales_agent': built-in tool 'data_to_chart' would emit tool_resources"


@pytest.mark.parametrize(
    ("descriptor", "detail"),
    [
        ("BODY", "the descriptor is not a mapping"),
        ({"type": "string", "searchable": "yes", "filterable": False}, "searchable is 'yes', not a boolean"),
        ({"type": "datetime", "searchable": True}, "filterable is None, not a boolean"),
    ],
)
def test_a_column_descriptor_reports_its_first_problem(descriptor: object, detail: str) -> None:
    [diagnostic] = search_tool_problems(MODEL, search_tool(), "docs", {"BODY": descriptor})
    assert diagnostic.context["detail"] == detail


def test_filter_columns_read_compound_operators_and_skip_values() -> None:
    node = {"@and": [{"@not": {"@eq": {"REGION": "x"}}}, {"@gte": {"PRICE": 1}}, "stray"], "TYPE": "x"}
    assert list(filter_columns(node)) == ["REGION", "PRICE", "TYPE"]
    assert list(filter_columns(3)) == []


def test_schema_type_names_a_property_or_shows_a_value() -> None:
    assert schema_type({"type": "string"}) == "string"
    assert schema_type("string") == "'string'"


def test_a_property_that_is_not_a_mapping_carries_no_type() -> None:
    assert not input_type_matches("VARCHAR", "string")
    assert input_type_matches("VARIANT", {"type": "boolean"})


def test_first_difference_names_the_first_path_in_key_order() -> None:
    assert first_difference({"a": 1, "b": [1, 2]}, {"a": 1, "b": [1, 2]}) is None
    assert first_difference({"a": 1}, {"a": 1, "b": 2}) == "b"
    assert first_difference({"b": [1, 2]}, {"b": [1]}) == "b[1]"
    assert first_difference({"b": [1, {"c": 2}]}, {"b": [1, {"c": 3}]}) == "b[1].c"
    assert first_difference(1, 2) == "<root>"


def test_live_spec_reads_only_a_json_object() -> None:
    assert live_spec('{"models": {}}') == {"models": {}}
    assert live_spec("not json") is None
    assert live_spec("[1, 2]") is None


def test_live_signature_and_schema_comparison_skip_what_they_cannot_read() -> None:
    assert live_signature("LOOKUP(VARCHAR, NUMBER) RETURN VARCHAR") == ("VARCHAR", "NUMBER")
    assert live_signature("LOOKUP RETURN VARCHAR") is None
    assert signature_disagreement(("VARCHAR",), {"type": "object"}) is None


def test_an_unknown_refresh_mode_is_refused_at_render() -> None:
    target = QualifiedName.parse("DB.S.DOCS_SEARCH")
    with pytest.raises(ValueError, match="unknown refresh mode"):
        render_tool(search_member(refresh_mode="sometimes"), target, QualifiedName.parse("DB.MARTS.PRODUCT_DOCS"))
    rendered = render_tool(search_member(refresh_mode="full"), target, QualifiedName.parse("DB.MARTS.PRODUCT_DOCS"))
    assert "REFRESH_MODE = FULL" in str(rendered.statements[0])
