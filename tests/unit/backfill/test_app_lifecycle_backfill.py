"""Extension, eval and CoCo Desktop profile publication, at the edges the broader suites skip."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from hashlib import md5

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.coco_desktop_profiles import (
    CompileCocoDesktopProfiles,
    CompiledCocoDesktopProfile,
    DesktopChannel,
)
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.coco_desktop_profiles import CocoDesktopProfileLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.coco_desktop_profile import (
    CocoDesktopProfile,
    CocoDesktopProfileCatalog,
    SharedCocoDesktopProfile,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyResult,
    ChangeReason,
    ChangeSet,
    GrantRow,
    OutcomeStatus,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import empty_state, target
from tests.helpers.clocks import FixedClock
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval, resolved_eval
from tests.helpers.snowflake_fake import FakeSnowflake

STAGE = QualifiedName.parse("DB.S.SKILL_BUNDLES")
STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
CERTIFIED = CatalogChannel("DB", "S", STAGE, certified=True)


def _skill(name: str = "month-close") -> Skill:
    files = (
        SkillFile("SKILL.md", f"---\nname: {name}\ndescription: Close.\n---\nRead reference/steps.md.\n".encode()),
        SkillFile("reference/steps.md", b"steps\n"),
    )
    return Skill(name, f"skills/{name}", name, "Close the month.", "Read reference/steps.md.\n", files, Origin("s", 1))


def _extensions(channel: CatalogChannel = CERTIFIED) -> dict[str, CompiledExtension]:
    result = CompileSkills(SkillCatalog((_skill(),)), channel).run_result()
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def _publish_extensions(
    port: FakeSnowflake, compiled: dict[str, CompiledExtension], previous: State
) -> tuple[ChangeSet, ApplyResult]:
    releases = {key: item.release for key, item in compiled.items()}
    handlers = {kind: ExtensionLifecycleHandler(port, releases, kind) for kind in ("skill", "plugin")}
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    changeset = PlanArtifacts(port, lifecycle_handlers=handlers).run(
        rendered, manifest, previous, target(), fetched_at="now"
    )
    result = ApplyArtifacts(
        port, InMemoryStateStore(previous), FixedClock(), state_table=STATE_TABLE, lifecycle_handlers=handlers
    ).run(changeset, previous, ApplyOptions())
    return changeset, result


class _UnreadableGrants(FakeSnowflake):
    """A catalog whose SHOW GRANTS fails."""

    def show_grants(
        self, object_type: str, qualified_name: QualifiedName, routine_signature: tuple[str, ...] = ()
    ) -> tuple[GrantRow, ...]:
        raise SnowflakePortError("down")


def test_a_failed_certification_whose_grants_cannot_be_read_is_a_plain_certification_failure() -> None:
    port = _UnreadableGrants(existing=())
    port.refused = ("SET TAG",)
    _, result = _publish_extensions(port, _extensions(), empty_state())
    assert result.outcomes[0].error is not None and result.outcomes[0].error.code == "SST-APL007"


def test_a_failed_certification_of_an_extension_with_explicit_grants_says_readers_reach_it() -> None:
    port = FakeSnowflake(existing=())
    port.refused = ("SET TAG",)
    port.grants["DB.S.MONTH_CLOSE"] = (GrantRow("USAGE", "ROLE", "ANALYST", granted_by="DEPLOYER"),)
    _, result = _publish_extensions(port, _extensions(), empty_state())
    assert result.outcomes[0].error is not None and result.outcomes[0].error.code == "SST-APL021"


def test_a_certified_release_whose_state_name_does_not_parse_is_not_taken_for_a_moved_schema() -> None:
    compiled = _extensions()
    entry = AppliedEntry("a" * 64, "not a name", "now", "run", "applied", "a" * 64, "m")
    handler = ExtensionLifecycleHandler(
        FakeSnowflake(existing=()), {key: item.release for key, item in compiled.items()}, "skill"
    )
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    plan = handler.plan(compiled["skill:month-close"].rendered_artifact, entry, manifest)
    assert plan.action is Action.CREATE
    assert "SST-VAL807" not in [item.code for item in plan.diagnostics]


def _never_minted() -> tuple[RenderedArtifact, object]:
    resolved = resolved_eval()
    assert resolved.config.dataset is not None
    never = replace(resolved, config=replace(resolved.config, dataset=replace(resolved.config.dataset, mint="never")))
    result = compile_eval(never)
    manifest = build_manifest(result)
    return compiled_as(result, CompiledEval).rendered_for_publish(manifest.manifest_id), manifest


def test_an_eval_that_does_not_mint_holds_only_a_dataset_resource() -> None:
    artifact, _ = _never_minted()
    with_table = replace(artifact, physical_resources=(("TABLE", artifact.target), *artifact.physical_resources))
    with pytest.raises(ValueError, match="does not mint requires one DATASET"):
        EvalLifecycleHandler(FakeSnowflake())._resources_by_type(with_table)


def test_an_unminted_eval_whose_config_changed_since_state_updates() -> None:
    artifact, manifest = _never_minted()
    port = FakeSnowflake()
    port.existing = {artifact.physical_resources[0][1].sql}
    recorded = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        "m",
        component_fingerprints=(
            ("config", "0" * 64),
            ("config_stage_md5", md5(b"", usedforsecurity=False).hexdigest()),
        ),
    )
    plan = EvalLifecycleHandler(port).plan(artifact, recorded, manifest)  # type: ignore[arg-type]
    assert (plan.action, plan.reason) == (Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS)


def test_publishing_an_unminted_eval_whose_dataset_vanished_after_the_plan_fails() -> None:
    artifact, manifest = _never_minted()
    port = FakeSnowflake()
    dataset = artifact.physical_resources[0][1].sql
    port.existing = {dataset}
    handler = EvalLifecycleHandler(port)
    previous = State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, empty_state().applied)
    change = (
        PlanArtifacts(port, lifecycle_handlers={"eval": handler})
        .run(
            {artifact.key: artifact},
            manifest,  # type: ignore[arg-type]
            previous,
            target(),
            fetched_at="now",
        )
        .changes[0]
    )
    assert change.action is Action.CREATE
    # The dataset is dropped between the drift check, which still sees it, and the publish.
    reads: list[str] = []
    seen = port.object_exists

    def dropped_after_check(object_type: str, qualified_name: QualifiedName) -> bool:
        if object_type.upper() == "DATASET":
            reads.append(qualified_name.sql)
            return len(reads) == 1
        return seen(object_type, qualified_name)

    port.object_exists = dropped_after_check  # type: ignore[method-assign]
    outcome = handler.apply(change, ApplyOptions())
    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None and "does not exist, and the eval does not mint it" in outcome.error.message


def _desktop_profiles() -> dict[str, CompiledCocoDesktopProfile]:
    skills = SkillCatalog((_skill("common"),))
    profiles = CocoDesktopProfileCatalog(
        (
            CocoDesktopProfile(
                name="analyst",
                directory="coco_desktop_profiles/analyst",
                description="Analyst.",
                owner_team="Data",
                skills=(),
                mcp_servers=(),
                hooks=(),
                prompt="Be careful.\n",
                origin=Origin("coco_desktop_profiles/analyst/coco_desktop_profile.yml", 1),
            ),
        ),
        SharedCocoDesktopProfile("Shared.\n", (), (), Origin("coco_desktop_profiles/shared/coco_desktop_profile.yml")),
    )
    channel = DesktopChannel(QualifiedName.parse("DB.S.PROFILES"), QualifiedName.parse("DB.S.PROFILE_REGISTRY"))
    result = CompileCocoDesktopProfiles(profiles, skills, channel, catalog_published=frozenset()).run_result()
    assert not result.diagnostics.has_errors, result.diagnostics
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledCocoDesktopProfile)}


def test_a_profile_row_another_writer_filled_with_other_content_at_our_version_is_refused() -> None:
    compiled = _desktop_profiles()
    port = FakeSnowflake(existing=())
    original_rows = port.desktop_profile_rows

    def rival_content(registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        rows = [dict(row) for row in original_rows(registry)]
        for row in rows:
            row["DESCRIPTION"] = json.dumps("Another writer's description.")
        return tuple(rows)

    port.desktop_profile_rows = rival_content  # type: ignore[method-assign]
    handler = CocoDesktopProfileLifecycleHandler(port, compiled)
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    changeset = PlanArtifacts(port, lifecycle_handlers={"coco_desktop_profile": handler}).run(
        rendered, manifest, empty_state(), target(), fetched_at="now"
    )
    result = ApplyArtifacts(
        port,
        InMemoryStateStore(empty_state()),
        FixedClock(),
        state_table=STATE_TABLE,
        lifecycle_handlers={"coco_desktop_profile": handler},
    ).run(changeset, empty_state(), ApplyOptions())
    error = result.outcomes[0].error
    assert error is not None and error.code == "SST-APL012" and "with other content" in error.message
