"""Resolve a `plugin()` skill entry: one that names no member loads every member, each named.

Snowflake refuses a plugin skill entry without a `name`, so an entry naming no member resolves
to one named entry per member, in the plugin's order, each pinned to the plugin's version.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents import ExtensionPin
from snowflake_semantic_tools.app.compile.agents.resolve_skills import resolve_skills
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_builders import agent, context

TOOLKIT = ExtensionPin(
    "plugin:jaffle-toolkit", QualifiedName.parse("DB.SKILLS.JAFFLE_TOOLKIT"), "SST_07A81C1FFB57", ("analytics", "ops")
)


def entry(name: str = "") -> AgentSkill:
    return AgentSkill(name, "CORTEX_EXTENSION", "jaffle-toolkit", "", ref="plugin")


def resolved(*entries: AgentSkill, members: tuple[str, ...] = TOOLKIT.members) -> tuple[AgentSkill, ...]:
    pin = ExtensionPin(TOOLKIT.key, TOOLKIT.target, TOOLKIT.alias, members)
    skills, dependencies, diagnostics = resolve_skills(
        agent("router", skills=entries), context(plugins={"jaffle-toolkit": pin})
    )
    assert diagnostics == () and dependencies == ("plugin:jaffle-toolkit",)
    return skills


def test_a_plugin_entry_naming_no_member_becomes_one_named_entry_per_member() -> None:
    assert [(skill.name, skill.path, skill.version) for skill in resolved(entry())] == [
        ("analytics", "DB.SKILLS.JAFFLE_TOOLKIT", "SST_07A81C1FFB57"),
        ("ops", "DB.SKILLS.JAFFLE_TOOLKIT", "SST_07A81C1FFB57"),
    ]


def test_a_plugin_entry_naming_a_member_stays_one_entry() -> None:
    assert [skill.name for skill in resolved(entry("ops"))] == ["ops"]


def test_a_plugin_without_members_keeps_its_one_entry_unnamed() -> None:
    assert [skill.name for skill in resolved(entry(), members=())] == [""]
