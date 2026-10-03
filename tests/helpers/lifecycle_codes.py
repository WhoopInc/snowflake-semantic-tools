"""Composite artifacts the per-code PLN tests plan and publish: one skill, and one CoCo Desktop profile.

`publish_skills` and `publish_profiles` plan the compiled artifacts against a recorded
Snowflake through their lifecycle handlers, then apply the plan, as the lifecycle tests do.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile, CompileProfiles, DesktopChannel
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.profiles import ProfileLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ChangeSet
from snowflake_semantic_tools.domain.model.profile import (
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    SharedProfile,
)
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, State
from tests.helpers.app_ports import FixedClock, InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.recorded_snowflake import RecordedSnowflake

BUNDLE_STAGE = QualifiedName.parse("DB.S.SKILL_BUNDLES")
PROFILE_STAGE = QualifiedName.parse("DB.S.PROFILES")
REGISTRY = QualifiedName.parse("DB.S.PROFILE_REGISTRY")
STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")


def lifecycle_state(applied: dict[str, AppliedEntry] | None = None) -> State:
    return State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, MappingProxyType(dict(applied or {})))


def month_close() -> dict[str, CompiledExtension]:
    """The `month-close` skill, compiled to publish through the catalog channel."""
    files = (
        SkillFile("SKILL.md", b"---\nname: month-close\ndescription: Close.\n---\nRead reference/steps.md.\n"),
        SkillFile("reference/steps.md", b"steps\n"),
    )
    found = Skill(
        "month-close",
        "skills/month-close",
        "month-close",
        "Close the month.",
        "Read reference/steps.md.\n",
        files,
        Origin("skills/month-close/SKILL.md", 1),
    )
    channel = CatalogChannel("DB", "S", BUNDLE_STAGE)
    result = CompileSkills(SkillCatalog((found,)), channel).run_result()
    assert not result.diagnostics.has_errors, result.diagnostics
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def plan_skills(
    port: RecordedSnowflake, compiled: dict[str, CompiledExtension], previous: State, *, prune: bool = False
) -> ChangeSet:
    """Plan the skills against `port` through their lifecycle handler."""
    releases = {key: item.release for key, item in compiled.items()}
    handler = ExtensionLifecycleHandler(port, releases, "skill")
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    return PlanArtifacts(port, lifecycle_handlers={"skill": handler}).run(
        rendered, manifest, previous, target(), fetched_at="now", include_prune=prune
    )


def publish_skills(port: RecordedSnowflake, compiled: dict[str, CompiledExtension], previous: State) -> State:
    """Plan and apply the skills, returning the state apply leaves."""
    releases = {key: item.release for key, item in compiled.items()}
    handlers = {"skill": ExtensionLifecycleHandler(port, releases, "skill")}
    changeset = plan_skills(port, compiled, previous)
    store = InMemoryStateStore(previous)
    ApplyArtifacts(port, store, FixedClock(), state_table=STATE_TABLE, lifecycle_handlers=handlers).run(
        changeset, previous, ApplyOptions()
    )
    return store.state or previous


def analyst_profile() -> dict[str, CompiledProfile]:
    """The `analyst` CoCo Desktop profile, compiled to publish through the registry."""

    def skill(name: str) -> Skill:
        files = (SkillFile("SKILL.md", f"---\nname: {name}\n---\nBody.\n".encode()),)
        return Skill(name, f"skills/{name}", name, "d", "Body.\n", files, Origin(f"skills/{name}/SKILL.md"))

    profile = DesktopProfile(
        name="analyst",
        directory="profiles/analyst",
        description="Analyst.",
        owner_team="Data",
        skills=("semantics",),
        mcp_servers=("dbt",),
        hooks=("guard",),
        prompt="Be careful.\n",
        origin=Origin("profiles/analyst/profile.yml", 1),
    )
    catalog = ProfileCatalog(
        (profile,),
        SharedProfile("Shared.\n", (("a.md", "Rule."),), ("common",), Origin("profiles/shared/profile.yml")),
        (
            HookDefinition(
                "guard", "hooks/guard", "PreToolUse", "bash", SkillFile("guard.sh", b"#!/bin/sh\n"), Origin("h")
            ),
        ),
        (McpConfig("dbt", "mcp-servers/dbt/mcp.json", {"dbt": {"command": "dbt-mcp"}}, Origin("m")),),
    )
    skills = SkillCatalog((skill("common"), skill("semantics")))
    result = CompileProfiles(
        catalog, skills, DesktopChannel(PROFILE_STAGE, REGISTRY), catalog_channel=True
    ).run_result()
    assert not result.diagnostics.has_errors, result.diagnostics
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledProfile)}


def plan_profiles(port: RecordedSnowflake, compiled: dict[str, CompiledProfile], previous: State) -> ChangeSet:
    """Plan the profiles against `port` through their lifecycle handler."""
    handler = ProfileLifecycleHandler(port, compiled)
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    return PlanArtifacts(port, lifecycle_handlers={"profile": handler}).run(
        rendered, manifest, previous, target(), fetched_at="now"
    )
