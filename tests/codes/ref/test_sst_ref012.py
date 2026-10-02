"""SST-REF012: an agent tool's `agent()` names no enabled agent of the project."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_context import agent_context

ORIGIN = Origin("agents/router/agent.yml", 4, 5)
ROUTER = AgentModel("router", ORIGIN, ("agents/router/agent.yml",))


def _codes(diagnostics: tuple[Diagnostic, ...], code: str) -> list[Diagnostic]:
    return [item for item in diagnostics if item.code == code]


def test_sst_ref012_fires() -> None:
    _, diagnostics = resolve_tool(
        ROUTER, AgentTool("agent", ORIGIN, name="helper", agent_ref="missing", description="Delegate."), agent_context()
    )
    [diagnostic] = _codes(diagnostics, "SST-REF012")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ agent('missing') } does not resolve"
    assert diagnostic.subject == "agent:router"


def test_sst_ref012_silent() -> None:
    resolved, diagnostics = resolve_tool(
        ROUTER,
        AgentTool("agent", ORIGIN, name="helper", agent_ref="helper", description="Delegate."),
        agent_context(agents={"helper": QualifiedName.parse("DB.S.HELPER")}),
    )
    assert resolved is not None
    assert _codes(diagnostics, "SST-REF012") == []
