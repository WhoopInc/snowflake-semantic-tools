"""SST-REF013: `extension()` names an extension `skills.extensions:` does not declare."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents import CompileAgents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill
from tests.helpers.agent_context import agent_context


def _compiled(skill: AgentSkill, code: str) -> list[Diagnostic]:
    """The diagnostics of `code` from compiling agent `router` with one skill entry."""
    agent = AgentModel("router", Origin("agents/router/agent.yml"), ("agents/router/agent.yml",), skills=(skill,))
    diagnostics = CompileAgents((agent,), DiagnosticBag(), agent_context()).run_result().diagnostics
    return [item for item in diagnostics if item.code == code]


def test_sst_ref013_fires() -> None:
    [diagnostic] = _compiled(AgentSkill("unknown", "CORTEX_EXTENSION", "nowhere", "V1", ref="extension"), "SST-REF013")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ extension('nowhere') } does not resolve"
    assert diagnostic.subject == "agent:router"


def test_sst_ref013_silent() -> None:
    assert _compiled(AgentSkill("vendor", "CORTEX_EXTENSION", "vendor-pack", "V1", ref="extension"), "SST-REF013") == []
