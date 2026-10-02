"""SST-RND012: an agent tool's type is one the renderer does not know."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from tests.helpers.agent_context import agent_context

ORIGIN = Origin("agents/router/agent.yml", 4, 3)
AGENT = AgentModel("router", ORIGIN, ("agents/router/agent.yml",))


def test_sst_rnd012_fires() -> None:
    tool, [diagnostic] = resolve_tool(AGENT, AgentTool("telepathy", ORIGIN, name="x"), agent_context())
    assert tool is None
    assert (diagnostic.code, diagnostic.severity, diagnostic.origin) == ("SST-RND012", Severity.ERROR, ORIGIN)
    assert diagnostic.message == "agent 'router': tool type 'telepathy' is unknown to the renderer"
    assert diagnostic.subject == "agent:router"


def test_sst_rnd012_silent() -> None:
    tool, found = resolve_tool(AGENT, AgentTool("data_to_chart", ORIGIN, description="Charts."), agent_context())
    assert tool is not None and found == ()
