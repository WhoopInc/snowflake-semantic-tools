"""SST-VAL540: a skill() entry writes a name, which is the skill's own."""

from __future__ import annotations

import json

from snowflake_semantic_tools.app.compile.agents import ExtensionPin
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_builders import agent, compile_agents, compiled_agents
from tests.helpers.diagnostic_filters import coded

PINS = {"semantics": ExtensionPin("skill:semantics", QualifiedName.parse("DB.S.SEMANTICS"), "SST_ABC", ("semantics",))}


def _named(name: str) -> list[Diagnostic]:
    skill = AgentSkill(name, "CORTEX_EXTENSION", "semantics", "", ref="skill")
    return coded(compile_agents(agent("sales_agent", skills=(skill,)), skills=PINS).diagnostics, "SST-VAL540")


def test_sst_val540_fires() -> None:
    [diagnostic] = _named("semantics")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'sales_agent': skill('semantics') takes no name; it is named after the skill, not 'semantics'"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val540_silent() -> None:
    assert _named("") == []


def test_an_unnamed_skill_entry_is_named_after_its_skill() -> None:
    skill = AgentSkill("", "CORTEX_EXTENSION", "semantics", "", ref="skill")
    [compiled] = compiled_agents(compile_agents(agent("sales_agent", skills=(skill,)), skills=PINS))
    [entry] = json.loads(compiled.payload)["skills"]
    assert entry["name"] == "semantics"
    assert entry["source"] == {"type": "CORTEX_EXTENSION", "path": "DB.S.SEMANTICS", "version": "SST_ABC"}
