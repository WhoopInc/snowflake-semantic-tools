"""SST-VAL541: two skill entries contribute the same member name."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents import ExtensionPin
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded

SKILLS = {"semantics": ExtensionPin("skill:semantics", QualifiedName.parse("DB.S.SEMANTICS"), "SST_A", ("semantics",))}


def _plugged(*members: str) -> list[Diagnostic]:
    plugins = {"toolkit": ExtensionPin("plugin:toolkit", QualifiedName.parse("DB.S.TOOLKIT"), "SST_B", members)}
    skills = (
        AgentSkill("semantics", "CORTEX_EXTENSION", "semantics", "", ref="skill"),
        AgentSkill("", "CORTEX_EXTENSION", "toolkit", "", ref="plugin"),
    )
    result = compile_agents(agent("sales_agent", skills=skills), skills=SKILLS, plugins=plugins)
    return coded(result.diagnostics, "SST-VAL541")


def test_sst_val541_fires() -> None:
    [diagnostic] = _plugged("semantics", "operations")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales_agent': 'semantics' and 'plugin('toolkit')' both contribute 'semantics'; the later wins"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val541_silent() -> None:
    assert _plugged("operations") == []
