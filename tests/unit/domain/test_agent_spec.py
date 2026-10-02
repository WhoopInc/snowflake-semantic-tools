"""An agent's rendered specification as a whole, and its instructions against its tools."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill, ResolvedAgentTool
from snowflake_semantic_tools.domain.validate.agent_spec import (
    instruction_problems,
    near_duplicate_descriptions,
    resource_keys,
    spec_completeness,
)

ORIGIN = Origin("agents/router/agent.yml")


def _agent(**fields: object) -> AgentModel:
    return AgentModel("router", ORIGIN, ("agents/router/agent.yml",), **fields)  # type: ignore[arg-type]


def _tool(
    name: str, description: str = "Answers questions.", kind: str = "generic", **fields: object
) -> ResolvedAgentTool:
    return ResolvedAgentTool(kind, name, description, **fields)  # type: ignore[arg-type]


def test_a_spec_lacks_each_section_the_model_carries_that_the_document_drops() -> None:
    assert spec_completeness(_agent(), (), {"models": {}}) is None
    agent = _agent(
        budget_seconds=30,
        response_instructions="Be brief.",
        skills=(AgentSkill("s", "CORTEX_EXTENSION", "s", "V1"),),
    )
    tools = (_tool("search", resources=MappingProxyType({"index": "X"})), _tool("chart", kind="data_to_chart"))
    found = spec_completeness(agent, tools, {"models": {}, "orchestration": None, "tools": []})
    assert found is not None and found.code == "SST-VAL510"
    assert found.context["value"] == "orchestration, instructions, tool_resources, skills"
    sampled = spec_completeness(_agent(sample_questions=("How many?",)), (), {"models": {}})
    assert sampled is not None and sampled.context["value"] == "instructions"


def test_resource_keys_must_name_a_rendered_tool_that_is_not_built_in() -> None:
    document = {
        "tools": [
            {"tool_spec": {"name": "search", "type": "cortex_search"}},
            {"tool_spec": {"name": "chart", "type": "data_to_chart"}},
            {"tool_spec": {"type": "generic"}},
            "not a tool",
        ],
        "tool_resources": {"search": {}, "chart": {}, "ghost": {}},
    }
    assert [
        (item.code, item.context.get("name") or item.context.get("key")) for item in resource_keys(_agent(), document)
    ] == [
        ("SST-VAL529", "chart"),
        ("SST-VAL530", "ghost"),
    ]
    assert resource_keys(_agent(), {"tools": "x", "tool_resources": []}) == []


def test_near_identical_descriptions_are_reported_once_per_pair() -> None:
    tools = (
        _tool("a", "Search the  product docs."),
        _tool("b", "search the product docs"),
        _tool("c", "Run the quarterly revenue report."),
        _tool("d", "  "),
    )
    assert [(item.context["a"], item.context["b"]) for item in near_duplicate_descriptions(_agent(), tools)] == [
        ("a", "b")
    ]


def test_instructions_name_only_tools_the_agent_has_and_route_nothing_a_tool_excludes() -> None:
    tools = (_tool("docs_search", "Searches the docs. Do not use for pricing; not for refunds."),)
    agent = _agent(
        orchestration_instructions=(
            "Use the docs_search tool for pricing. Use the `ghost` tool for legal; use the cortex_search tool."
        ),
        response_instructions="Never send docs_search refunds. Call tool 'Ghost' when unsure.",
    )
    found = instruction_problems(agent, tools)
    assert [(item.code, item.context.get("name"), item.context.get("value")) for item in found] == [
        ("SST-VAL535", "ghost", None),
        ("SST-VAL535", "Ghost", None),
        ("SST-VAL536", "docs_search", "pricing"),
    ]
    assert instruction_problems(_agent(), tools) == []
