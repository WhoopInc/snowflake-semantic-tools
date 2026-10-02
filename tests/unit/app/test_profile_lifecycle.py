"""Desktop profiles publish verified trees and one guarded, Desktop-readable row."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile, CompileProfiles, DesktopChannel
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompileSkills
from snowflake_semantic_tools.app.desktop_contract import desktop_view, is_pointer, stage_pointers
from snowflake_semantic_tools.app.lifecycle.profiles import ProfileLifecycleHandler, _shape_problem, _stale
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyResult,
    ChangeReason,
    ChangeSet,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.model.profile import (
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    SharedProfile,
)
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError
from snowflake_semantic_tools.domain.state import (
    DEACTIVATED,
    FAILED_AFTER_WRITE,
    STATE_SCHEMA_VERSION,
    AppliedEntry,
    State,
)
from tests.helpers.app_ports import FixedClock, InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.recorded_snowflake import PROFILE_REGISTRY_SHAPE, RecordedSnowflake

STAGE = QualifiedName.parse("DB.S.PROFILES")
REGISTRY = QualifiedName.parse("DB.S.PROFILE_REGISTRY")
CHANNEL = DesktopChannel(STAGE, REGISTRY)


def skill(name: str) -> Skill:
    files = (
        SkillFile("SKILL.md", f"---\nname: {name}\n---\nRead reference/a.md.\n".encode()),
        SkillFile("reference/a.md", b"a"),
    )
    return Skill(name, f"skills/{name}", name, "d", "Read reference/a.md.\n", files, Origin(f"skills/{name}/SKILL.md"))


SKILLS = SkillCatalog((skill("common"), skill("semantics")))


def catalog(**profile_fields: object) -> ProfileCatalog:
    values: dict[str, object] = {
        "name": "analyst",
        "directory": "profiles/analyst",
        "description": "Analyst.",
        "owner_team": "Data",
        "skills": ("semantics",),
        "mcp_servers": ("dbt",),
        "hooks": ("guard",),
        "prompt": "Be careful.\n",
        "origin": Origin("profiles/analyst/profile.yml", 1),
    }
    values.update(profile_fields)
    return ProfileCatalog(
        (DesktopProfile(**values),),  # type: ignore[arg-type]
        SharedProfile("Shared.\n", (("a.md", "Rule."),), ("common",), Origin("profiles/shared/profile.yml")),
        (
            HookDefinition(
                "guard", "hooks/guard", "PreToolUse", "bash", SkillFile("guard.sh", b"#!/bin/sh\n"), Origin("h")
            ),
        ),
        (McpConfig("dbt", "mcp-servers/dbt/mcp.json", {"dbt": {"command": "dbt-mcp"}}, Origin("m")),),
    )


def compile_profiles(profiles: ProfileCatalog) -> dict[str, CompiledProfile]:
    result = CompileProfiles(profiles, SKILLS, CHANNEL, catalog_channel=True).run_result()
    assert not result.diagnostics.has_errors, result.diagnostics
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledProfile)}


def state(applied: dict[str, AppliedEntry] | None = None) -> State:
    return State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, MappingProxyType(dict(applied or {})))


def publish(
    port: RecordedSnowflake, compiled: dict[str, CompiledProfile], previous: State, *, prune: bool = False
) -> tuple[ChangeSet, ApplyResult, State]:
    handler = ProfileLifecycleHandler(port, compiled)
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    changeset = PlanArtifacts(port, lifecycle_handlers={"profile": handler}).run(
        rendered, manifest, previous, target(), fetched_at="now", include_prune=prune
    )
    store = InMemoryStateStore(previous)
    result = ApplyArtifacts(
        port,
        store,
        FixedClock(),
        state_table=QualifiedName.parse("DB.S.SST_STATE"),
        lifecycle_handlers={"profile": handler},
    ).run(changeset, previous, ApplyOptions(allow_prune=prune))
    return changeset, result, store.state or previous


def test_compile_reports_desktop_registry_and_blocks_on_broken_members() -> None:
    compiled = compile_profiles(catalog())
    item = compiled["profile:analyst"]
    assert (item.name, item.artifact_type, item.member_keys, item.referenced_models, item.dbt_relations) == (
        "analyst",
        "profile",
        (),
        (),
        (),
    )
    assert item.source_files[-1] == "mcp-servers/dbt/mcp.json"
    artifact = item.rendered_for_publish("a" * 64)
    assert artifact.target == REGISTRY and artifact.statements == ()
    assert dict(artifact.component_fingerprints)["version"] == item.release.version
    notes = CompileProfiles(catalog(), SKILLS, CHANNEL, catalog_channel=True).run_result().diagnostics
    assert [note.code for note in notes] == ["SST-VAL854"]
    desktop = replace(CHANNEL, registry=QualifiedName.parse("CORTEX_CODE.CONFIG.PROFILE_REGISTRY"))
    assert CompileProfiles(catalog(), SKILLS, desktop, catalog_channel=True).run_result().diagnostics == ()
    blocked = CompileProfiles(catalog(), SKILLS, CHANNEL, catalog_channel=True, blocked_skills=frozenset(("common",)))
    result = blocked.run_result()
    assert result.compiled == () and [note.code for note in result.diagnostics] == ["SST-VAL854", "SST-VAL855"]
    assert CompileProfiles(catalog(), SKILLS, None, catalog_channel=False).run_result().compiled == ()


def test_a_profile_whose_plugin_cannot_be_bundled_does_not_publish() -> None:
    dangling = Skill(
        "dangling",
        "skills/dangling",
        "dangling",
        "d",
        "Read reference/missing.md.\n",
        (SkillFile("SKILL.md", b"---\nname: dangling\n---\nRead reference/missing.md.\n"),),
        Origin("skills/dangling/SKILL.md"),
    )
    kit = Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", "Kit.", "Data", ("dangling",), Origin("p"))
    skills = SkillCatalog((*SKILLS.skills, dangling), (kit,))
    profiles = catalog(plugins=("kit",))
    # Without the catalog channel the profile compile bundles the plugin itself.
    alone = CompileProfiles(profiles, skills, CHANNEL, catalog_channel=False).run_result()
    # With it, the CLI passes the plugins the skill compile could not bundle.
    extensions = CompileSkills(skills, CatalogChannel("DB", "S", QualifiedName.parse("DB.S.BUNDLES"))).run_result()
    blocked = frozenset(
        str(item.subject).removeprefix("plugin:")
        for item in extensions.diagnostics
        if item.severity is Severity.ERROR and str(item.subject).startswith("plugin:")
    )
    shared = CompileProfiles(profiles, skills, CHANNEL, catalog_channel=True, blocked_plugins=blocked).run_result()

    for bundle_errors, result in ((alone.diagnostics, alone), (extensions.diagnostics, shared)):
        assert ("SST-VAL836", "plugin:kit") in {(item.code, item.subject) for item in bundle_errors}
        assert result.compiled == ()
        assert [item.message for item in result.diagnostics if item.code == "SST-VAL855"] == [
            "profile 'analyst': plugin 'kit' has errors, so the profile cannot publish"
        ]


def test_first_publish_creates_registry_uploads_trees_and_reads_back_like_desktop() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_profiles(catalog())
    changeset, result, after = publish(port, compiled, state())
    assert [(change.action, change.reason) for change in changeset.changes] == [
        (Action.CREATE, ChangeReason.NOT_PRESENT)
    ]
    assert result.success, result.outcomes
    assert port.table_columns(REGISTRY) is not None
    assert port.stage_types[STAGE.sql] == "INTERNAL NO CSE"
    release = compiled["profile:analyst"].release
    assert len(port.uploads) == sum(len(tree.entries) for tree in release.trees)
    rows = port.desktop_profile_rows(REGISTRY)
    view = desktop_view(rows[0])
    assert view["VERSION"] == release.version
    assert len(stage_pointers(view)) == 5
    assert dict(after.applied["profile:analyst"].component_fingerprints)["version"] == release.version

    changeset, result, after = publish(port, compiled, after)
    assert [change.action for change in changeset.changes] == [Action.NOOP]

    changed = compile_profiles(catalog(prompt="Be even more careful.\n"))
    uploads = len(port.uploads)
    changeset, result, after = publish(port, changed, after)
    assert [change.action for change in changeset.changes] == [Action.UPDATE]
    assert result.success
    assert len(port.uploads) == uploads + 1  # only the new prompt tree; every other tree is reused

    changeset, result, _ = publish(port, compiled, after)
    assert result.success and desktop_view(port.desktop_profile_rows(REGISTRY)[0])["VERSION"] == release.version


def test_unmanaged_concurrent_wrong_shape_and_client_side_stage_block() -> None:
    compiled = compile_profiles(catalog())
    version = compiled["profile:analyst"].release.version
    port = RecordedSnowflake(existing=())
    port.ensure_profile_registry(REGISTRY)
    port.merge_profile_row(REGISTRY, {"CONFIG_NAME": "analyst", "VERSION": "66.00000"}, expected_version=None)
    changeset, _, _ = publish(port, compiled, state())
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN024"]
    assert changeset.changes[0].reason is ChangeReason.UNMANAGED_OBJECT

    owned = AppliedEntry(
        "f", REGISTRY.sql, "now", "run", "applied", "f", "m", component_fingerprints=(("version", "SST_OLD"),)
    )
    changeset, _, _ = publish(port, compiled, state({"profile:analyst": owned}))
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN028"]

    shape = RecordedSnowflake(existing=())
    shape.tables[REGISTRY.sql] = (("CONFIG_NAME", "VARCHAR"), ("VERSION", "NUMBER(38,0)"))
    changeset, _, _ = publish(shape, compiled, state())
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN029"]
    assert "lacks" in changeset.diagnostics[0].message

    wrong = tuple((name, "VARCHAR" if name == "ACTIVE" else kind) for name, kind in PROFILE_REGISTRY_SHAPE)
    assert _shape_problem(wrong) == "types ACTIVE VARCHAR differ"

    client_side = RecordedSnowflake(existing=())
    client_side.stage_types[STAGE.sql] = "INTERNAL"
    changeset, _, _ = publish(client_side, compiled, state())
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN026"]
    assert version.startswith("SST_")


def test_lost_race_bad_pointer_and_merge_failure_are_reported() -> None:
    compiled = compile_profiles(catalog())
    race = RecordedSnowflake(existing=())
    original = race.merge_profile_row

    def rival_first(registry: QualifiedName, row: Mapping[str, object], *, expected_version: str | None) -> int:
        original(registry, {"CONFIG_NAME": "analyst", "VERSION": "RIVAL"}, expected_version=None)
        return original(registry, row, expected_version=expected_version)

    race.merge_profile_row = rival_first  # type: ignore[method-assign]
    _, result, _ = publish(race, compiled, state())
    assert result.outcomes[0].error is not None and result.outcomes[0].error.code == "SST-APL012"

    dangling = RecordedSnowflake(existing=())
    original_rows = dangling.desktop_profile_rows

    def broken_pointer(registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        rows = [dict(row) for row in original_rows(registry)]
        for row in rows:
            row["SYSTEM_PROMPT_REPO"] = json.dumps({"snowflake_stage": "@DB.S.PROFILES/prompts/analyst/NOPE/AGENTS.md"})
        return tuple(rows)

    dangling.desktop_profile_rows = broken_pointer  # type: ignore[method-assign]
    _, result, _ = publish(dangling, compiled, state())
    assert result.outcomes[0].error is not None and "does not resolve" in result.outcomes[0].error.message

    vanished = RecordedSnowflake(existing=())
    vanished.desktop_profile_rows = lambda registry: ()  # type: ignore[method-assign]
    _, result, _ = publish(vanished, compiled, state())
    assert result.outcomes[0].error is not None and "no active row" in result.outcomes[0].error.message

    refusing = RecordedSnowflake(existing=())
    refusing.refused = ("MERGE",)
    _, result, _ = publish(refusing, compiled, state())
    assert result.outcomes[0].error is not None and result.outcomes[0].error.code == "SST-APL018"

    tampered = RecordedSnowflake(existing=())
    tampered.read_staged_file = lambda stage_path: b"x"  # type: ignore[method-assign]
    _, result, _ = publish(tampered, compiled, state())
    assert result.outcomes[0].error is not None and "byte for byte" in result.outcomes[0].error.message


def test_prune_deactivates_only_under_prune_and_only_the_recorded_version() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_profiles(catalog())
    _, _, after = publish(port, compiled, state())
    changeset, result, _ = publish(port, {}, after)
    assert changeset.changes == ()
    changeset, result, pruned = publish(port, {}, after, prune=True)
    assert [(change.action, change.prune_executable) for change in changeset.changes] == [(Action.PRUNE, True)]
    assert result.success and port.desktop_profile_rows(REGISTRY) == ()
    # The row is still SST's, so state keeps a tombstone rather than forgetting it.
    tombstone = pruned.applied["profile:analyst"]
    assert tombstone.outcome == DEACTIVATED
    assert tombstone.component_fingerprints == after.applied["profile:analyst"].component_fingerprints
    again, _, _ = publish(port, {}, pruned, prune=True)
    assert again.changes == ()

    # Restoring the profile reactivates SST's own row through the guarded MERGE.
    restored_plan, restored, restored_state = publish(port, compiled, pruned)
    assert [(change.action, change.reason) for change in restored_plan.changes] == [
        (Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS)
    ]
    assert restored.success and restored_state.applied["profile:analyst"].outcome == "applied"
    assert [row["CONFIG_NAME"] for row in port.desktop_profile_rows(REGISTRY)] == ["analyst"]

    handler = ProfileLifecycleHandler(port, {})
    entry = after.applied["profile:analyst"]
    change = handler.report_prune("profile:analyst", entry)
    assert handler.apply(change, ApplyOptions()).status is OutcomeStatus.SKIPPED
    port.deactivate_profile_row(
        REGISTRY, "analyst", expected_version=str(port.desktop_profile_rows(REGISTRY)[0]["VERSION"])
    )
    failed = handler.apply(change, ApplyOptions(allow_prune=True))
    assert failed.status is OutcomeStatus.FAILED and failed.error is not None and failed.error.code == "SST-APL012"
    unversioned = handler.report_prune("profile:analyst", replace(entry, component_fingerprints=()))
    refused = handler.apply(unversioned, ApplyOptions(allow_prune=True))
    assert refused.status is OutcomeStatus.FAILED and refused.error is not None
    assert "refusing to deactivate" in refused.error.message
    assert handler.merge_physical_resources((("STAGE", STAGE.sql),), entry) == (("STAGE", STAGE.sql),)


def test_stale_plan_and_desktop_pointer_rules() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_profiles(catalog())
    handler = ProfileLifecycleHandler(port, compiled)
    artifact = compiled["profile:analyst"].rendered_artifact
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    plan = handler.plan(artifact, None, manifest)
    port.ensure_profile_registry(REGISTRY)
    port.merge_profile_row(REGISTRY, {"CONFIG_NAME": "analyst", "VERSION": "RIVAL"}, expected_version=None)
    from snowflake_semantic_tools.domain.model.lifecycle import Change

    change = Change(
        artifact.key,
        "profile",
        plan.action,
        plan.reason,
        artifact,
        None,
        (),
        270,
        composite_observation=plan.observation,
    )
    outcome = handler.apply(change, ApplyOptions())
    assert outcome.error is not None and outcome.error.code == "SST-APL012"
    assert handler.apply(replace(change, action=Action.NOOP), ApplyOptions()).status is OutcomeStatus.SKIPPED
    assert (
        _stale(
            (("registry", "absent"), ("stage_type", "")), (("registry", "present"), ("stage_type", "INTERNAL NO CSE"))
        )
        is False
    )
    assert _stale((("row_version", ""),), (("row_version", "X"),)) is True
    # Trees are content-addressed and re-verified on apply, so a changed listing is not staleness.
    assert _stale((("complete_trees", ""),), (("complete_trees", "skills/analyst/H/"),)) is False
    assert is_pointer({"source": "github:x", "ref": "main"}) and not is_pointer({"source": "x", "ref": 1})
    assert not is_pointer("text") and desktop_view({"version": "1.0"})["VERSION"] == 1.0
    assert (
        stage_pointers({"HOOKS": {"PreToolUse": [{"hooks": [{"type": "prompt"}, "bad"]}, "bad"]}, "SKILL_REPOS": "x"})
        == ()
    )
    # PLUGINS holds strings; only stage paths are fetched, so only they must resolve.
    assert stage_pointers({"PLUGINS": ["@DB.S.P/plugins/a/H/kit/", "snow://skill_catalog/DB.S.KIT", 3]}) == (
        "@DB.S.P/plugins/a/H/kit/",
    )
    assert stage_pointers({"PLUGINS": "not a list"}) == ()
    # A pointer without a stage path, and a command hook whose source is no stage, are not followed.
    assert stage_pointers(
        {
            "SKILL_REPOS": [{"source": "github:x"}, {"snowflake_stage": "@DB.S.P/a/"}],
            "HOOKS": {"Stop": [{"hooks": [{"type": "command", "source": {"source": "github:x"}}]}]},
        }
    ) == ("@DB.S.P/a/",)


class FlakyProfilePort(RecordedSnowflake):
    """Injects one transient failure at a chosen step of a profile publish."""

    def __init__(self) -> None:
        super().__init__(existing=())
        self.fail_upload: str | None = None
        self.fail_read_back = False
        self.fail_after_merge = False

    def upload(self, stage_path: str, content: bytes) -> None:
        if self.fail_upload is not None and self.fail_upload in stage_path:
            self.fail_upload = None
            raise SnowflakePortError("transient PUT failure")
        super().upload(stage_path, content)

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        if self.fail_read_back:
            self.fail_read_back = False
            raise SnowflakePortError("transient SELECT failure")
        return super().desktop_profile_rows(registry)

    def merge_profile_row(
        self, registry: QualifiedName, row: Mapping[str, object], *, expected_version: str | None
    ) -> int:
        changed = super().merge_profile_row(registry, row, expected_version=expected_version)
        if self.fail_after_merge:
            # The server committed the MERGE; only its reply was lost.
            self.fail_after_merge = False
            raise SnowflakePortError("connection reset after MERGE")
        return changed


def test_a_failed_update_leaves_state_on_the_version_the_row_still_carries() -> None:
    port = FlakyProfilePort()
    _, _, first = publish(port, compile_profiles(catalog()), state())
    old = dict(first.applied["profile:analyst"].component_fingerprints)["version"]
    edited = compile_profiles(catalog(prompt="Be very careful.\n"))
    port.fail_upload = "prompts/"
    _, failed, after = publish(port, edited, first)
    outcome = failed.outcomes[0]
    assert outcome.status is OutcomeStatus.FAILED and not outcome.write_succeeded
    assert after.applied["profile:analyst"] == first.applied["profile:analyst"]
    assert port.desktop_profile_rows(REGISTRY)[0]["VERSION"] == old
    # Not a concurrent writer: the retry publishes the edit.
    retry, retried, _ = publish(port, edited, after)
    assert [change.diagnostics for change in retry.changes] == [DiagnosticBag()]
    assert retried.success
    assert port.desktop_profile_rows(REGISTRY)[0]["VERSION"] == edited["profile:analyst"].release.version


def test_a_failed_read_back_after_the_row_is_written_keeps_ownership() -> None:
    compiled = compile_profiles(catalog())
    for inject in ("fail_read_back", "fail_after_merge"):
        port = FlakyProfilePort()
        setattr(port, inject, True)
        _, failed, after = publish(port, compiled, state())
        outcome = failed.outcomes[0]
        assert outcome.status is OutcomeStatus.FAILED and outcome.write_succeeded, inject
        entry = after.applied["profile:analyst"]
        assert entry.outcome == FAILED_AFTER_WRITE
        assert dict(entry.component_fingerprints)["version"] == compiled["profile:analyst"].release.version
        retry, retried, converged = publish(port, compiled, after)
        assert [(change.action, change.reason) for change in retry.changes] == [
            (Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH)
        ], inject
        assert retried.success and converged.applied["profile:analyst"].outcome == "applied"


class DroppedConnection(RecordedSnowflake):
    """The MERGE commits, its reply is lost, and the re-read fails on the same dead connection."""

    def __init__(self) -> None:
        super().__init__(existing=())
        self.fail_after_merge = False
        self.dead = False

    def merge_profile_row(
        self, registry: QualifiedName, row: Mapping[str, object], *, expected_version: str | None
    ) -> int:
        changed = super().merge_profile_row(registry, row, expected_version=expected_version)
        if self.fail_after_merge:
            self.fail_after_merge = False
            self.dead = True
            raise SnowflakePortError("connection reset after MERGE")
        return changed

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        if self.dead:
            self.dead = False
            raise SnowflakePortError("connection reset")
        return super().read_profile_row(registry, name)


def test_an_unknown_merge_outcome_is_recorded_only_when_no_row_existed() -> None:
    compiled = compile_profiles(catalog())
    # First publish: no row existed, so the row now present can only be SST's.
    port = DroppedConnection()
    port.fail_after_merge = True
    _, failed, after = publish(port, compiled, state())
    assert failed.outcomes[0].write_succeeded and after.applied["profile:analyst"].outcome == FAILED_AFTER_WRITE
    retry, retried, _ = publish(port, compiled, after)
    assert [change.action for change in retry.changes] == [Action.UPDATE] and retried.success

    # Update: a row existed, so an unknown outcome keeps the entry SST knows is true.
    edited = compile_profiles(catalog(prompt="Be very careful.\n"))
    port.fail_after_merge = True
    _, failed, updated = publish(port, edited, after)
    assert not failed.outcomes[0].write_succeeded
    assert updated.applied["profile:analyst"] == after.applied["profile:analyst"]


def test_deactivating_a_row_that_is_gone_retires_it() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_profiles(catalog())
    _, _, after = publish(port, compiled, state())
    port.profile_rows.clear()
    _, result, pruned = publish(port, {}, after, prune=True)
    assert result.success and pruned.applied["profile:analyst"].outcome == DEACTIVATED


class NarrowRegistry(RecordedSnowflake):
    """Creating the registry leaves a table without the columns SST writes."""

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        super().ensure_profile_registry(qualified_name)
        self.tables[qualified_name.sql] = (("CONFIG_NAME", "VARCHAR"),)


def test_a_registry_or_stage_that_cannot_be_created_fails_before_any_row_is_written() -> None:
    compiled = compile_profiles(catalog())
    narrow = NarrowRegistry(existing=())
    _, result, after = publish(narrow, compiled, state())
    outcome = result.outcomes[0]
    assert outcome.error is not None and outcome.error.code == "SST-APL016"
    assert "lacks" in outcome.error.message and "after creation" in outcome.error.message
    assert (outcome.attempts, outcome.write_succeeded, after.applied) == (1, False, {})

    refusing = RecordedSnowflake(existing=())
    refusing.refused = ("CREATE STAGE",)
    _, result, after = publish(refusing, compiled, state())
    outcome = result.outcomes[0]
    assert outcome.error is not None and outcome.error.code == "SST-APL001"
    assert outcome.error.message.startswith("recorded refusal: CREATE STAGE")
    assert (outcome.attempts, outcome.write_succeeded, after.applied) == (2, False, {})
    assert refusing.uploads == []
