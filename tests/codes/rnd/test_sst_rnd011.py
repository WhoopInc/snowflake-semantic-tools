"""SST-RND011: an agent's spec holds `$$`, which ends the literal an inline spec is published in."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgentTool
from snowflake_semantic_tools.domain.render.agent import agent_render_checks, render_agent_json

AGENT = AgentModel("router", Origin("agents/router/agent.yml"), ("agents/router/agent.yml",))
CHART = ResolvedAgentTool("data_to_chart", "data_to_chart", "Charts.")


def test_sst_rnd011_fires() -> None:
    model = replace(AGENT, response_instructions="Costs are $$ in USD.")
    payload = render_agent_json(model, (CHART,))
    [diagnostic] = agent_render_checks(model, (CHART,), payload)
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND011", Severity.ERROR)
    assert diagnostic.message == f"agent 'router': spec contains '$$' at {payload.index('$$')}"


def test_sst_rnd011_silent() -> None:
    model = replace(AGENT, response_instructions="Costs are $ in USD.")
    assert agent_render_checks(model, (CHART,), render_agent_json(model, (CHART,))) == ()
