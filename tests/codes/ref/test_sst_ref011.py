"""SST-REF011: an Analyst tool names a semantic view that did not compile."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from tests.helpers.agent_context import agent_context

ORIGIN = Origin("agents/router/agent.yml", 4, 5)
ROUTER = AgentModel("router", ORIGIN, ("agents/router/agent.yml",))


def _codes(diagnostics: tuple[Diagnostic, ...], code: str) -> list[Diagnostic]:
    return [item for item in diagnostics if item.code == code]


def test_sst_ref011_fires() -> None:
    _, diagnostics = resolve_tool(
        ROUTER,
        AgentTool("cortex_analyst_text_to_sql", ORIGIN, description="Sales.", semantic_view="nope"),
        agent_context(),
    )
    [diagnostic] = _codes(diagnostics, "SST-REF011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ semantic_view('nope') } does not resolve"
    assert diagnostic.subject == "agent:router"


def test_sst_ref011_silent() -> None:
    resolved, diagnostics = resolve_tool(
        ROUTER,
        AgentTool("cortex_analyst_text_to_sql", ORIGIN, description="Sales.", semantic_view="SALES"),
        agent_context(),
    )
    assert resolved is not None
    assert _codes(diagnostics, "SST-REF011") == []
