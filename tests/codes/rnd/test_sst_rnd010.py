"""SST-RND010: an agent's spec renders with no tools."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgentTool
from snowflake_semantic_tools.domain.render.agent import agent_render_checks, render_agent_json

AGENT = AgentModel("router", Origin("agents/router/agent.yml"), ("agents/router/agent.yml",))
CHART = ResolvedAgentTool("data_to_chart", "data_to_chart", "Charts.")


def test_sst_rnd010_fires() -> None:
    [diagnostic] = agent_render_checks(AGENT, (), render_agent_json(AGENT, ()))
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-RND010",
        Severity.WARNING,
        "agent:router",
    )
    assert diagnostic.message == "agent 'router' renders with no tools"


def test_sst_rnd010_silent() -> None:
    assert agent_render_checks(AGENT, (CHART,), render_agent_json(AGENT, (CHART,))) == ()
