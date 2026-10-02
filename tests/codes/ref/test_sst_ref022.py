"""SST-REF022: agents delegate to one another, through agent tools, in a cycle."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents import CompileAgents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_context import agent_context

AGENTS = {"first": QualifiedName.parse("DB.S.FIRST"), "second": QualifiedName.parse("DB.S.SECOND")}


def _delegating(name: str, *to: str) -> AgentModel:
    origin = Origin(f"agents/{name}/agent.yml")
    tools = tuple(AgentTool("agent", origin, name=target, agent_ref=target, description="Delegate.") for target in to)
    return AgentModel(name, origin, (origin.file,), tools=tools)


def _cycles(*agents: AgentModel) -> list[Diagnostic]:
    result = CompileAgents(agents, DiagnosticBag(), agent_context(agents=AGENTS)).run_result()
    return [item for item in result.diagnostics if item.code == "SST-REF022"]


def test_sst_ref022_fires() -> None:
    [diagnostic] = _cycles(_delegating("first", "second"), _delegating("second", "first"))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent delegation cycle: first -> second -> first"
    assert diagnostic.subject is None


def test_sst_ref022_silent() -> None:
    assert _cycles(_delegating("first", "second"), _delegating("second")) == []
