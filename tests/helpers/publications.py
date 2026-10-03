"""Skills and Desktop profiles compiled and published through their lifecycle handlers, for code tests.

`publish_skill` plans and applies one skill against a recorded Snowflake; `publish_profile`
does the same for one Desktop profile. Each returns the changeset, the apply result, and the
state the run left.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile, CompileProfiles, DesktopChannel
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.profiles import ProfileLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ApplyResult, ChangeSet
from snowflake_semantic_tools.domain.model.profile import DesktopProfile, ProfileCatalog, SharedProfile
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, State
from tests.helpers.app_ports import FixedClock, InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.recorded_snowflake import RecordedSnowflake

SKILL_STAGE = QualifiedName.parse("DB.S.SKILL_BUNDLES")
PROFILE_STAGE = QualifiedName.parse("DB.S.PROFILES")
PROFILE_REGISTRY = QualifiedName.parse("DB.S.PROFILE_REGISTRY")


def skill(name: str = "month-close", steps: bytes = b"steps\n") -> Skill:
    files = (
        SkillFile("SKILL.md", f"---\nname: {name}\ndescription: Close.\n---\nRead reference/steps.md.\n".encode()),
        SkillFile("reference/steps.md", steps),
    )
    return Skill(name, f"skills/{name}", name, "Close the month.", "Read reference/steps.md.\n", files, Origin("s"))


def compiled_skill(*, certified: bool = False, steps: bytes = b"steps\n") -> CompiledExtension:
    channel = CatalogChannel("DB", "S", SKILL_STAGE, certified=certified)
    result = CompileSkills(SkillCatalog((skill(steps=steps),)), channel).run_result()
    [compiled] = [item for item in result.compiled if isinstance(item, CompiledExtension)]
    return compiled


def empty_state(applied: dict[str, AppliedEntry] | None = None) -> State:
    return State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, MappingProxyType(dict(applied or {})))


def _run(
    port: RecordedSnowflake,
    handlers: dict[str, CompositeLifecycleHandler],
    rendered: dict[str, object],
    previous: State,
) -> tuple[ChangeSet, ApplyResult, State]:
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    changeset = PlanArtifacts(port, lifecycle_handlers=handlers).run(
        rendered,  # type: ignore[arg-type]
        manifest,
        previous,
        target(),
        fetched_at="now",
    )
    store = InMemoryStateStore(previous)
    result = ApplyArtifacts(
        port, store, FixedClock(), state_table=QualifiedName.parse("DB.S.SST_STATE"), lifecycle_handlers=handlers
    ).run(changeset, previous, ApplyOptions())
    return changeset, result, store.state or previous


def publish_skill(
    port: RecordedSnowflake, compiled: CompiledExtension, previous: State | None = None
) -> tuple[ChangeSet, ApplyResult, State]:
    handler = ExtensionLifecycleHandler(port, {compiled.artifact_key: compiled.release}, "skill")
    return _run(
        port,
        {"skill": handler},
        {compiled.artifact_key: compiled.rendered_artifact},
        previous or empty_state(dict(port.state)),
    )


def compiled_profile(*shipped: Skill) -> CompiledProfile:
    """Compile the `analyst` profile, shipping each skill given."""
    profiles = ProfileCatalog(
        (
            DesktopProfile(
                name="analyst",
                directory="profiles/analyst",
                description="Analyst.",
                owner_team="Data",
                skills=tuple(item.name for item in shipped),
                mcp_servers=(),
                hooks=(),
                prompt="Be careful.\n",
                origin=Origin("profiles/analyst/profile.yml", 1),
            ),
        ),
        SharedProfile("Shared.\n", (), (), Origin("profiles/shared/profile.yml")),
        (),
        (),
    )
    result = CompileProfiles(
        profiles, SkillCatalog(shipped), DesktopChannel(PROFILE_STAGE, PROFILE_REGISTRY), catalog_channel=True
    ).run_result()
    assert not result.diagnostics.has_errors, result.diagnostics
    [compiled] = [item for item in result.compiled if isinstance(item, CompiledProfile)]
    return compiled


def publish_profile(port: RecordedSnowflake, compiled: CompiledProfile) -> tuple[ChangeSet, ApplyResult, State]:
    handler = ProfileLifecycleHandler(port, {compiled.artifact_key: compiled})
    return _run(
        port, {"profile": handler}, {compiled.artifact_key: compiled.rendered_artifact}, empty_state(dict(port.state))
    )
