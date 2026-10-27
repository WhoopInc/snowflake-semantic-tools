"""CoWork profiles (a Snowflake private preview), the agents they route to, and the projects that compile them.

Shared by the profile compile tests and the per-code VAL tests. A project built here publishes its
profiles to SNOWFLAKE_INTELLIGENCE.PROFILES unless a test says otherwise, so a profile is quiet
beside the agents, which publish to the target's own DB.SCH.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.cowork_profiles import CompiledCoworkProfile
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentProfile, AgentTool
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel, CoworkSubagent
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership
from tests.helpers.project_inputs import InMemoryProjectInputs, dev_target

PROFILE_FILE = "cowork_profiles/everyone/cowork_profile.yml"
PROFILE_ORIGIN = Origin(PROFILE_FILE, 1, 1)
AGENT_ORIGIN = Origin("agents/sales/agent.yml", 1, 1)
# Where CoWork lists profiles, as a `cowork_profiles:` block places them.
LISTED: Mapping[str, object] = MappingProxyType(
    {"cowork_profiles": {"+database": "SNOWFLAKE_INTELLIGENCE", "+schema": "PROFILES"}}
)


def subagent(name: str = "sales", reference: str = "agent", line: int = 4, **fields: Any) -> CoworkSubagent:
    """A `spec.subagents` entry calling `reference('<name>')`, written at `line`."""
    return CoworkSubagent(reference, name, Origin(PROFILE_FILE, line, 7), **fields)


def cowork_profile(name: str = "everyone", *subagents: CoworkSubagent, **fields: Any) -> CoworkProfileModel:
    """A profile routing to `subagents`, by default one `agent('sales')` subagent."""
    return CoworkProfileModel(name, PROFILE_ORIGIN, (PROFILE_FILE,), subagents=subagents or (subagent(),), **fields)


def described_agent(name: str = "sales", **fields: Any) -> AgentModel:
    """An agent that compiles on its own, with a display name, a comment, and one sample question."""
    values: dict[str, Any] = {
        "comment": "Pipeline and quota.",
        "profile": AgentProfile(display_name="Sales"),
        "sample_questions": ("What closed last week?",),
        "tools": (AgentTool("code_execution", AGENT_ORIGIN, description="Charts.", declared_keys=("type",)),),
        **fields,
    }
    return AgentModel(name, AGENT_ORIGIN, ("agents/sales/agent.yml",), **values)


def agent_member(name: str = "partner", kind: str = "agent", relation: str = "OTHER.SCH.PARTNER") -> ToolMember:
    """A referenced tool member of `kind`, whose relation for `dev` is `relation`."""
    return ToolMember(
        name,
        kind,
        ToolOwnership.REFERENCE,
        Origin("tools/platform.yml", 1, 1),
        "tools/platform.yml",
        description="A partner's agent.",
        relations=MappingProxyType({"dev": relation}),
    )


def compile_profiles(
    *profiles: CoworkProfileModel,
    agents: tuple[AgentModel, ...] = (),
    members: tuple[ToolMember, ...] = (),
    tree: Mapping[str, object] = LISTED,
    identity: TargetIdentity | None = None,
) -> CompileResult:
    """Compile a dbt project of `profiles`, `agents`, and tool `members` for `identity`, else `dev`."""
    target = identity or dev_target()
    inputs = InMemoryProjectInputs(
        tree=dict(tree),
        identity=target,
        tools=ToolCatalog(members, target.name, frozenset((target.name,))),
        agent_models=agents,
        cowork_profile_models=profiles,
    )
    return CompileProject(inputs).run()


def compiled_profiles(result: CompileResult) -> tuple[CompiledCoworkProfile, ...]:
    """Return the compiled CoWork profiles of a result, in order."""
    return tuple(item for item in result.compiled if isinstance(item, CompiledCoworkProfile))


def router_entries(profile: CompiledCoworkProfile) -> list[dict[str, object]]:
    """Return the subagent entries the profile's router lists, as its payload renders them."""
    document = json.loads(profile.payload)
    entries: list[dict[str, object]] = document["tool_resources"]["router"]["subagents"]
    return entries
