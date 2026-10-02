"""SST-PRS009: an agent tool's resolved name holds a character a tool identifier may not."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from tests.helpers.seam_projects import agent_context

AGENT = AgentModel("sales", Origin("agent.yml"), ("agent.yml",))


def _codes(name: str) -> list[tuple[str, Severity, str, str | None]]:
    tool = AgentTool("data_to_chart", Origin("agent.yml"), name=name, description="Charts.")
    _, diagnostics = resolve_tool(AGENT, tool, agent_context())
    return [(item.code, item.severity, item.message, item.subject) for item in diagnostics]


def test_sst_prs009_fires() -> None:
    assert _codes("chart it") == [
        ("SST-PRS009", Severity.ERROR, "'chart it' does not resolve to a 1-64 char tool identifier", "agent:sales")
    ]


def test_sst_prs009_silent() -> None:
    assert _codes("chart-it_2") == []
