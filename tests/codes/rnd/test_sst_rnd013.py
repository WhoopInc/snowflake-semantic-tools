"""SST-RND013: a generic tool's resources render, and CREATE AGENT does not validate them."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgentTool
from snowflake_semantic_tools.domain.render.agent import agent_render_checks, render_agent_json

AGENT = AgentModel("router", Origin("agents/router/agent.yml"), ("agents/router/agent.yml",))
CHART = ResolvedAgentTool("data_to_chart", "data_to_chart", "Charts.")


def test_sst_rnd013_fires() -> None:
    generic = ResolvedAgentTool("generic", "lookup", "Looks up.", MappingProxyType({"identifier": "DB.S.LOOKUP"}))
    [diagnostic] = agent_render_checks(AGENT, (generic,), render_agent_json(AGENT, (generic,)))
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND013", Severity.WARNING)
    assert diagnostic.message == "agent 'router': tool_resources for 'lookup' is not validated by Snowflake"


def test_sst_rnd013_silent() -> None:
    search = ResolvedAgentTool("cortex_search", "docs", "Docs.", MappingProxyType({"name": "DB.S.DOCS"}))
    assert agent_render_checks(AGENT, (search,), render_agent_json(AGENT, (search,))) == ()
