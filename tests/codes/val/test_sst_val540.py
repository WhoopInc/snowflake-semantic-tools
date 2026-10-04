"""SST-VAL540: a skill() source omits its name."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents import ExtensionPin
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded

PINS = {"semantics": ExtensionPin("skill:semantics", QualifiedName.parse("DB.S.SEMANTICS"), "SST_ABC", ("semantics",))}


def _named(name: str) -> list[Diagnostic]:
    skill = AgentSkill(name, "CORTEX_EXTENSION", "semantics", "", ref="skill")
    return coded(compile_agents(agent("sales_agent", skills=(skill,)), skills=PINS).diagnostics, "SST-VAL540")


def test_sst_val540_fires() -> None:
    [diagnostic] = _named("")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': the skill source for skill('semantics') omits name"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val540_silent() -> None:
    assert _named("semantics") == []
