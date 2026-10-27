"""Skills compiled and published through their lifecycle handler, for code tests.

`publish_skill` plans and applies one skill against a recorded Snowflake. It returns the
changeset, the apply result, and the state the run left.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ApplyResult, ChangeSet
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.state import State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import empty_state, target
from tests.helpers.clocks import FixedClock
from tests.helpers.snowflake_fake import FakeSnowflake

SKILL_STAGE = QualifiedName.parse("DB.S.SKILL_BUNDLES")


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


def _run(
    port: FakeSnowflake,
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
    port: FakeSnowflake, compiled: CompiledExtension, previous: State | None = None
) -> tuple[ChangeSet, ApplyResult, State]:
    handler = ExtensionLifecycleHandler(port, {compiled.artifact_key: compiled.release}, "skill")
    return _run(
        port,
        {"skill": handler},
        {compiled.artifact_key: compiled.rendered_artifact},
        previous or empty_state(dict(port.remote_state or {})),
    )
