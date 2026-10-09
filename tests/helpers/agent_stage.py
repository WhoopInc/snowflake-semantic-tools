"""Agents staged on the agent spec stage, and the plan and apply runs that publish them.

Each agent is rendered as plan renders it for publication: its specification uploaded to
`STAGE` (or another stage), then created from that folder. The runs go through the one
Snowflake fake, which holds no stage until a test stages one or a CREATE STAGE runs.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgent
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ApplyResult, ChangeSet, RenderedArtifact
from snowflake_semantic_tools.domain.state import State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import empty_state, manifest, target
from tests.helpers.clocks import FixedClock
from tests.helpers.snowflake_fake import FakeSnowflake

STAGE = QualifiedName.parse("DB.S.AGENT_SPECS")
STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
# A statement that would change a stage that already exists: SST never sends one.
_CHANGES_A_STAGE = re.compile(r"\b(CREATE\s+OR\s+REPLACE|ALTER|DROP)\s+(TEMPORARY\s+)?STAGE\b")


def staged_agent(name: str, *, stage: QualifiedName = STAGE) -> RenderedArtifact:
    """The agent `DB.S.<name>`, its specification staged on `stage`, as plan renders it."""
    model = AgentModel(name, Origin(f"{name}.yml"), (f"{name}.yml",))
    compiled = CompiledAgent(ResolvedAgent(model, ()), QualifiedName.from_parts("db", "s", name), "{}\n", "a" * 64)
    return for_publication(compiled, stage=stage, git_sha="sha").rendered_artifact


def plan_agents(
    port: FakeSnowflake, *agents: RenderedArtifact, state: State | None = None, preflight: bool = True
) -> ChangeSet:
    """Plan the agents against the fake, with its preflight reads unless `preflight` is False."""
    rendered = {agent.key: agent for agent in agents}
    return PlanArtifacts(port, preflight=port if preflight else None).run(
        rendered, manifest(rendered), state or empty_state(), target(), fetched_at="now"
    )


def apply_plan(
    port: FakeSnowflake, changeset: ChangeSet, state: State | None = None
) -> tuple[ApplyResult, InMemoryStateStore]:
    """Apply a plan on the fake from `state`, or from an empty state, and return what it recorded."""
    store = InMemoryStateStore()
    result = ApplyArtifacts(port, store, FixedClock(), state_table=STATE_TABLE).run(changeset, state or empty_state())
    return result, store


def stage_creations(port: FakeSnowflake) -> list[str]:
    """Every CREATE STAGE the fake was sent, in order."""
    return [text for script in port.scripts for text in script if text.upper().startswith("CREATE STAGE")]


def stage_changes(port: FakeSnowflake) -> list[str]:
    """Every statement the fake was sent that replaces, alters, or drops a stage."""
    return [text for script in port.scripts for text in script if _CHANGES_A_STAGE.search(text.upper())]
