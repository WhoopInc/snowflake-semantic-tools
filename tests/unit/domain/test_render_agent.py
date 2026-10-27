from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill, ResolvedAgentTool
from snowflake_semantic_tools.domain.render.agent import desired_agent_definition, render_agent_json, render_agent_spec


def test_agent_renderer_covers_all_optional_sections_and_builtin_resource_omission() -> None:
    model = AgentModel(
        "agent",
        Origin("agent.yml"),
        ("agent.yml",),
        comment="comment",
        budget_seconds=10,
        budget_tokens=100,
        tool_not_accessible="reject",
        analytical_search=False,
        orchestration_instructions="route",
        response_instructions="answer",
        sample_questions=("question",),
        skills=(AgentSkill("skill", "CORTEX_EXTENSION", "DB.S.EXT", "GIT_1"),),
        alias="promoted",
        tags=(("DOMAIN", "sales"),),
        passthrough=MappingProxyType({"experimental": True}),
    )
    tools = (
        ResolvedAgentTool(
            "generic",
            "lookup",
            "Use for lookups.",
            MappingProxyType({"identifier": "DB.S.LOOKUP"}),
            MappingProxyType({"type": "object", "properties": {}}),
            tool_spec_passthrough=MappingProxyType({"extra": True}),
        ),
        ResolvedAgentTool(
            "data_to_chart",
            "data_to_chart",
            "Use for charts.",
            MappingProxyType({"must_not_render": True}),
        ),
    )
    spec = render_agent_spec(model, tools)
    assert spec["orchestration"] == {
        "budget": {"seconds": 10, "tokens": 100},
        "tool_not_accessible": "reject",
        "capabilities": {"analytical_search": False},
    }
    assert spec["instructions"] == {
        "orchestration": "route",
        "response": "answer",
        "sample_questions": [{"question": "question"}],
    }
    assert "data_to_chart" not in spec["tool_resources"]  # type: ignore[operator]
    assert spec["tools"][0]["tool_spec"]["extra"] is True  # type: ignore[index]
    assert spec["experimental"] is True
    assert render_agent_json(model, tools).endswith("\n")
    assert desired_agent_definition(model, spec) == desired_agent_definition(model, spec)


def test_agent_renderer_minimal_spec_has_only_models() -> None:
    model = AgentModel("agent", Origin("agent.yml"), ("agent.yml",))
    assert render_agent_spec(model, ()) == {"models": {"orchestration": "auto"}}


def test_agent_renderer_omits_empty_resources_for_non_builtin_tool() -> None:
    model = AgentModel("agent", Origin("agent.yml"), ("agent.yml",))
    tool = ResolvedAgentTool("mcp", "server", "Use this server.")
    spec = render_agent_spec(model, (tool,))
    assert "tools" in spec
    assert "tool_resources" not in spec
