"""Skills and plugins publish content-addressed Cortex Extension versions safely."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.adapters.snowflake.memory import RecordedSnowflake
from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.extension_lifecycle import (
    ExtensionLifecycleHandler,
    _difference,
    _recorded_target,
    _stale,
)
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.app.skill_compile import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOptions, ChangeReason, OutcomeStatus
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.ports.snowflake import ExecResult, ExtensionVersion, SnowflakePortError
from snowflake_semantic_tools.domain.state.model import FAILED_AFTER_WRITE, STATE_SCHEMA_VERSION, AppliedEntry, State

from .conftest import FixedClock, InMemoryStateStore
from .helpers import target

STAGE = QualifiedName.parse("DB.S.SKILL_BUNDLES")
CHANNEL = CatalogChannel("DB", "S", STAGE)


def skill(name: str = "month-close", body: str = "Read reference/steps.md, then run run.py.\n") -> Skill:
    files = (
        SkillFile("SKILL.md", f"---\nname: {name}\ndescription: Close.\n---\n{body}".encode()),
        SkillFile("reference/steps.md", b"steps\n"),
        SkillFile("run.py", b"print(1)\n"),
    )
    return Skill(name, f"skills/{name}", name, "Close the month.", body, files, Origin(f"skills/{name}/SKILL.md", 1))


def compile_catalog(catalog: SkillCatalog, channel: CatalogChannel = CHANNEL) -> dict[str, CompiledExtension]:
    result = CompileSkills(catalog, channel).run_result()
    assert not result.diagnostics.has_errors, result.diagnostics
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def state(applied: dict[str, AppliedEntry] | None = None) -> State:
    return State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, MappingProxyType(dict(applied or {})))


def publish(port: RecordedSnowflake, compiled: dict[str, CompiledExtension], previous: State):
    releases = {key: item.release for key, item in compiled.items()}
    handlers = {kind: ExtensionLifecycleHandler(port, releases, kind) for kind in ("skill", "plugin")}
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    changeset = PlanArtifacts(port, lifecycle_handlers=handlers).run(
        rendered, manifest, previous, target(), fetched_at="now"
    )
    store = InMemoryStateStore(previous)
    result = ApplyArtifacts(
        port, store, FixedClock(), state_table=QualifiedName.parse("DB.S.SST_STATE"), lifecycle_handlers=handlers
    ).run(changeset, previous, ApplyOptions())
    return changeset, result, store.state or previous


def test_compiled_skill_and_plugin_render_bundle_manifests_with_aliases() -> None:
    catalog = SkillCatalog(
        (skill(), skill("month-open")),
        (
            Plugin(
                "finance-kit",
                "plugins/finance-kit",
                "plugins/finance-kit/plugin.yml",
                "Kit.",
                "Team",
                ("month-close", "ghost"),
                Origin("p"),
            ),
        ),
    )
    result = CompileSkills(catalog, CHANNEL).run_result()
    assert [item.code for item in result.diagnostics] == ["SST-VAL835"]
    catalog = replace(catalog, plugins=(replace(catalog.plugins[0], members=("month-close",)),))
    compiled = compile_catalog(catalog)
    skill_item = compiled["skill:month-close"]
    plugin_item = compiled["plugin:finance-kit"]
    assert (skill_item.name, skill_item.artifact_type, skill_item.has_scripts) == ("month-close", "skill", True)
    assert (skill_item.member_keys, skill_item.referenced_models, skill_item.dbt_relations) == ((), (), ())
    assert skill_item.release.prefix == f"@DB.S.SKILL_BUNDLES/month-close/{skill_item.release.alias}/"
    assert skill_item.release.target == QualifiedName.parse("DB.S.MONTH_CLOSE")
    artifact = skill_item.rendered_for_publish("a" * 64)
    document = json.loads(artifact.ddl)
    assert document["alias"] == skill_item.release.alias and document["type"] == "SKILL"
    assert dict(artifact.component_fingerprints)["alias"] == skill_item.release.alias
    assert artifact.statements == () and artifact.generic_apply_safe is False
    assert plugin_item.source_files[0] == "plugins/finance-kit/plugin.yml"
    assert json.loads(plugin_item.rendered_artifact.ddl)["type"] == "PLUGIN"
    certified = compile_catalog(catalog, replace(CHANNEL, certified=True))
    assert json.loads(certified["skill:month-close"].rendered_artifact.ddl)["certified"] is True


def test_catalog_channel_absent_or_invalid_prefix_compiles_nothing() -> None:
    catalog = SkillCatalog((skill(),))
    assert CompileSkills(catalog, None).run_result().compiled == ()
    invalid = CompileSkills(catalog, replace(CHANNEL, version_prefix="1X")).run_result()
    assert [item.code for item in invalid.diagnostics] == ["SST-CFG008"]
    broken = replace(skill(), files=(SkillFile("SKILL.md", b"---\n---\nRun scripts/missing.sh\n"),))
    blocked = CompileSkills(
        SkillCatalog((broken,), (Plugin("kit", "plugins/kit", "p", "d", None, ("month-close",), Origin("p")),)), CHANNEL
    ).run_result()
    assert blocked.compiled == ()
    assert {item.code for item in blocked.diagnostics} >= {"SST-VAL808", "SST-VAL836"}


def test_first_publish_then_noop_then_revert_is_a_state_only_update() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_catalog(SkillCatalog((skill(),)))
    changeset, result, after = publish(port, compiled, state())
    assert [change.action for change in changeset.changes] == [Action.CREATE]
    assert result.success, result.diagnostics
    statements = [statement for script in port.scripts for statement in script]
    assert statements[0].startswith(
        "CREATE STAGE IF NOT EXISTS DB.S.SKILL_BUNDLES ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
    )
    assert statements[1].startswith("CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'SKILL'")
    alias = compiled["skill:month-close"].release.alias
    assert (
        statements[2]
        == f"ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE ADD VERSION {alias} FROM @DB.S.SKILL_BUNDLES/month-close/{alias}/"
    )
    assert len(port.uploads) == 3
    entry = after.applied["skill:month-close"]
    assert dict(entry.component_fingerprints)["version"] == "VERSION$2"
    assert {resource.object_type for resource in entry.applied_resources} == {"CORTEX EXTENSION", "STAGE"}

    changeset, result, after = publish(port, compiled, after)
    assert [change.action for change in changeset.changes] == [Action.NOOP]
    assert len(port.uploads) == 3

    changed = compile_catalog(SkillCatalog((skill(body="Read reference/steps.md twice.\n"),)))
    changeset, result, after_change = publish(port, changed, after)
    assert [change.action for change in changeset.changes] == [Action.UPDATE]
    assert result.success
    assert len(port.extension_versions(QualifiedName.parse("DB.S.MONTH_CLOSE"))) == 3

    changeset, result, reverted = publish(port, compiled, after_change)
    assert [(change.action, change.reason) for change in changeset.changes] == [
        (Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH)
    ]
    assert result.success
    assert len(port.extension_versions(QualifiedName.parse("DB.S.MONTH_CLOSE"))) == 3
    assert (
        reverted.applied["skill:month-close"].fingerprint == compiled["skill:month-close"].rendered_artifact.fingerprint
    )
    assert [item.code for item in changeset.diagnostics] == ["SST-VAL841"]


def test_resume_after_partial_upload_uploads_only_missing_files() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_catalog(SkillCatalog((skill(),)))
    release = compiled["skill:month-close"].release
    port.stage_types[STAGE.sql] = "INTERNAL NO CSE"
    first = release.bundle.entries[0]
    port.upload(f"{release.prefix}{first.path}", first.content)
    port.uploads.clear()
    _, result, _ = publish(port, compiled, state())
    assert result.success
    assert [path for path, _ in port.uploads] == [
        f"{release.prefix}{entry.path}" for entry in release.bundle.entries[1:]
    ]


def test_unmanaged_type_mismatch_stage_and_damaged_alias_block() -> None:
    compiled = compile_catalog(SkillCatalog((skill(),)))
    release = compiled["skill:month-close"].release
    owned = AppliedEntry("f", release.target.sql, "now", "run", "applied", "f", "m")

    unmanaged = RecordedSnowflake(existing=())
    unmanaged.execute_script(("CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'SKILL' COMMENT = 'x'",))
    changeset, _, _ = publish(unmanaged, compiled, state())
    assert [(change.action, change.reason) for change in changeset.changes] == [
        (Action.BLOCKED, ChangeReason.UNMANAGED_OBJECT)
    ]
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN024"]

    mismatch = RecordedSnowflake(existing=())
    mismatch.execute_script(("CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'PLUGIN' COMMENT = 'x'",))
    changeset, _, _ = publish(mismatch, compiled, state({"skill:month-close": owned}))
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN002"]

    client_side = RecordedSnowflake(existing=())
    client_side.stage_types[STAGE.sql] = "INTERNAL"
    changeset, _, _ = publish(client_side, compiled, state())
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN026"]

    damaged = RecordedSnowflake(existing=())
    damaged.execute_script(
        ("CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'SKILL' COMMENT = 'Close the month.'",)
    )
    damaged.execute_script((f"ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE ADD VERSION {release.alias} FROM @DB.S.EMPTY/",))
    changeset, _, _ = publish(damaged, compiled, state({"skill:month-close": owned}))
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN027"]
    assert "missing" in changeset.diagnostics[0].message


def test_empty_version_and_upload_failures_are_partial_writes() -> None:
    compiled = compile_catalog(SkillCatalog((skill(),)))
    empty = RecordedSnowflake(existing=())
    original = empty._record_extension_statement

    def swallow_files(normalized: str, statement: str | None = None) -> None:
        # Reproduce the measured defect: ADD VERSION accepts a wrong path and
        # creates an empty version.
        original(normalized.replace("FROM @DB.S.SKILL_BUNDLES/", "FROM @DB.S.NOWHERE/"), statement)

    empty._record_extension_statement = swallow_files  # type: ignore[method-assign]
    _, result, after = publish(empty, compiled, state())
    assert not result.success
    assert result.outcomes[0].error is not None and result.outcomes[0].error.code == "SST-APL016"
    assert result.outcomes[0].write_succeeded is True
    assert after.applied["skill:month-close"].outcome == "failed_after_write"

    unreadable = RecordedSnowflake(existing=())
    unreadable.read_staged_file = lambda path: b"tampered"  # type: ignore[method-assign]
    _, result, _ = publish(unreadable, compiled, state())
    assert result.outcomes[0].error is not None and "byte for byte" in result.outcomes[0].error.message


def test_comment_drift_and_certification_with_readback() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_catalog(SkillCatalog((skill(),)))
    _, _, after = publish(port, compiled, state())
    port.execute_script(("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET COMMENT = 'drifted'",))
    certified = compile_catalog(SkillCatalog((skill(),)), replace(CHANNEL, certified=True))
    changeset, result, _ = publish(port, certified, after)
    assert [change.action for change in changeset.changes] == [Action.UPDATE]
    assert result.success
    statements = [statement for script in port.scripts for statement in script]
    assert statements[-2] == "ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET COMMENT = 'Close the month.'"
    alias = compiled["skill:month-close"].release.alias
    assert statements[-1].startswith(f"ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE VERSION {alias} SET TAG")

    refusing = RecordedSnowflake(existing=())
    refusing.refused = ("SET TAG",)
    _, result, _ = publish(refusing, certified, state())
    assert result.outcomes[0].error is not None and result.outcomes[0].error.code == "SST-APL007"

    silent = RecordedSnowflake(existing=())
    silent.refused = ()
    original = silent._record_extension_statement

    def ignore_tags(normalized: str, statement: str | None = None) -> None:
        if "SET TAG" not in normalized:
            original(normalized, statement)

    silent._record_extension_statement = ignore_tags  # type: ignore[method-assign]
    _, result, _ = publish(silent, certified, state())
    assert result.outcomes[0].error is not None and "reports certification" in result.outcomes[0].error.message


def test_plugin_falls_back_to_a_live_version_built_from_empty() -> None:
    catalog = SkillCatalog(
        (skill(),),
        (
            Plugin(
                "finance-kit",
                "plugins/finance-kit",
                "plugins/finance-kit/plugin.yml",
                "Kit.",
                None,
                ("month-close",),
                Origin("p"),
            ),
        ),
    )
    compiled = {key: item for key, item in compile_catalog(catalog).items() if key.startswith("plugin:")}
    port = RecordedSnowflake(existing=())
    port.refused = ("ADD VERSION SST_",)
    _, result, _ = publish(port, compiled, state())
    assert result.success, result.outcomes
    statements = [statement for script in port.scripts for statement in script]
    alias = compiled["plugin:finance-kit"].release.alias
    assert f"ALTER CORTEX EXTENSION DB.S.FINANCE_KIT ADD LIVE VERSION {alias}" in statements
    assert "FROM LAST" not in " ".join(statements)
    assert statements[-1] == "ALTER CORTEX EXTENSION DB.S.FINANCE_KIT COMMIT"
    assert any(path.startswith("snow://cortex_extension/DB.S.FINANCE_KIT/versions/live/") for path, _ in port.uploads)

    skills_only = compile_catalog(SkillCatalog((skill(),)))
    refused = RecordedSnowflake(existing=())
    refused.refused = ("ADD VERSION SST_",)
    _, result, _ = publish(refused, skills_only, state())
    assert result.outcomes[0].error is not None and "ADD VERSION failed" in result.outcomes[0].error.message


def test_stale_plan_prune_report_and_resource_merge() -> None:
    compiled = compile_catalog(SkillCatalog((skill(),)))
    port = RecordedSnowflake(existing=())
    releases = {key: item.release for key, item in compiled.items()}
    handler = ExtensionLifecycleHandler(port, releases, "skill")
    artifact = compiled["skill:month-close"].rendered_artifact
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    plan = handler.plan(artifact, None, manifest)
    port.stage_types[STAGE.sql] = "INTERNAL NO CSE"
    port.upload(f"{releases['skill:month-close'].prefix}SKILL.md", b"x")
    from snowflake_semantic_tools.domain.model.lifecycle import Change

    change = Change(
        artifact.key, "skill", plan.action, plan.reason, artifact, None, (), 250, composite_observation=plan.observation
    )
    outcome = handler.apply(change, ApplyOptions())
    assert outcome.status is OutcomeStatus.FAILED and outcome.error is not None and outcome.error.code == "SST-APL012"
    skipped = handler.apply(replace(change, action=Action.NOOP), ApplyOptions())
    assert skipped.status is OutcomeStatus.SKIPPED
    entry = AppliedEntry(
        "f",
        "DB.S.MONTH_CLOSE",
        "now",
        "run",
        "applied",
        "f",
        "m",
        physical_resources=(("CORTEX EXTENSION", "DB.S.MONTH_CLOSE"),),
    )
    prune = handler.report_prune("skill:gone", entry)
    assert (prune.action, prune.prune_executable, prune.order) == (Action.PRUNE, False, 250)
    assert ExtensionLifecycleHandler(port, releases, "plugin").report_prune("plugin:gone", entry).order == 260
    assert handler.merge_physical_resources((("STAGE", "DB.S.X"),), entry) == (("STAGE", "DB.S.X"),)
    assert _stale((("stage_type", ""),), (("stage_type", "INTERNAL NO CSE"),)) is False
    assert _stale((("stage_type", ""),), (("stage_type", "INTERNAL"),)) is True


def _plugin_catalog() -> SkillCatalog:
    kit = Plugin(
        "finance-kit",
        "plugins/finance-kit",
        "plugins/finance-kit/plugin.yml",
        "Kit.",
        None,
        ("month-close",),
        Origin("p"),
    )
    return SkillCatalog((skill(),), (kit,))


def _failure(result: object) -> str:
    outcome = result.outcomes[0]  # type: ignore[attr-defined]
    assert not result.success and outcome.error is not None  # type: ignore[attr-defined]
    return f"{outcome.error.code}: {outcome.error.message}"


def test_observation_failure_blocks_the_plan() -> None:
    compiled = compile_catalog(SkillCatalog((skill(),)))
    port = RecordedSnowflake(existing=())

    def unreachable(qualified_name: QualifiedName) -> None:
        raise SnowflakePortError("SHOW failed")

    port.observe_extension = unreachable  # type: ignore[assignment,method-assign]
    changeset, _, _ = publish(port, compiled, state())
    assert changeset.changes[0].action is Action.BLOCKED
    assert [item.code for item in changeset.changes[0].diagnostics] == ["SST-PLN001"]


def test_each_publish_step_fails_closed() -> None:
    compiled = compile_catalog(SkillCatalog((skill(),)))
    alias = compiled["skill:month-close"].release.alias

    stage_refused = RecordedSnowflake(existing=())
    stage_refused.refused = ("CREATE STAGE",)
    assert "CREATE STAGE" in _failure(publish(stage_refused, compiled, state())[1])

    client_side = RecordedSnowflake(existing=())
    created: list[str] = []
    execute = client_side.execute_script

    def create_client_side(statements: Sequence[str]) -> ExecResult:
        created.extend(statement for statement in statements if statement.startswith("CREATE STAGE"))
        return execute(statements)

    client_side.execute_script = create_client_side  # type: ignore[method-assign]
    client_side.stage_type = lambda stage: "INTERNAL" if created else None  # type: ignore[method-assign]
    assert "is INTERNAL after creation" in _failure(publish(client_side, compiled, state())[1])

    upload_refused = RecordedSnowflake(existing=())

    def refuse_upload(stage_path: str, content: bytes) -> None:
        raise SnowflakePortError("PUT refused")

    upload_refused.upload = refuse_upload  # type: ignore[method-assign]
    assert "upload of" in _failure(publish(upload_refused, compiled, state())[1])

    extra_file = RecordedSnowflake(existing=())
    listing = extra_file.list_location
    extra_file.list_location = lambda location: (  # type: ignore[method-assign]
        (*listing(location), "stray.txt") if location.startswith("@") else listing(location)
    )
    assert "unexpected stray.txt" in _failure(publish(extra_file, compiled, state())[1])

    create_refused = RecordedSnowflake(existing=())
    create_refused.refused = ("CREATE CORTEX EXTENSION",)
    assert "CREATE CORTEX EXTENSION" in _failure(publish(create_refused, compiled, state())[1])

    lost_alias = RecordedSnowflake(existing=())
    lost_alias.extension_versions = lambda qualified_name: ()  # type: ignore[method-assign]
    assert f"alias {alias} is absent" in _failure(publish(lost_alias, compiled, state())[1])

    drifted = RecordedSnowflake(existing=())
    _, _, after = publish(drifted, compiled, state())
    drifted.execute_script(("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET COMMENT = 'drifted'",))
    drifted.refused = ("SET COMMENT = 'Close",)
    assert "SET COMMENT" in _failure(publish(drifted, compiled, after)[1])


def test_plugin_fallback_aborts_on_every_failure() -> None:
    compiled = {key: item for key, item in compile_catalog(_plugin_catalog()).items() if key.startswith("plugin:")}

    live_refused = RecordedSnowflake(existing=())
    live_refused.refused = ("ADD VERSION SST_", "ADD LIVE VERSION")
    assert "ADD LIVE VERSION" in _failure(publish(live_refused, compiled, state())[1])

    live_upload = RecordedSnowflake(existing=())
    live_upload.refused = ("ADD VERSION SST_",)
    upload = live_upload.upload

    def refuse_live(stage_path: str, content: bytes) -> None:
        if stage_path.startswith("snow://"):
            raise SnowflakePortError("PUT refused")
        upload(stage_path, content)

    live_upload.upload = refuse_live  # type: ignore[method-assign]
    assert "into the live version failed" in _failure(publish(live_upload, compiled, state())[1])
    assert live_upload.scripts[-1] == ("ALTER CORTEX EXTENSION DB.S.FINANCE_KIT ABORT",)

    commit_refused = RecordedSnowflake(existing=())
    commit_refused.refused = ("ADD VERSION SST_", "COMMIT")
    assert "COMMIT" in _failure(publish(commit_refused, compiled, state())[1])
    assert commit_refused.scripts[-1] == ("ALTER CORTEX EXTENSION DB.S.FINANCE_KIT ABORT",)


def test_recorded_target_and_difference_helpers() -> None:
    release = compile_catalog(SkillCatalog((skill(),)))["skill:month-close"].release
    entry = AppliedEntry("f", "not a name", "now", "run", "applied", "f", "m")
    assert _recorded_target(entry, release) is False
    assert _difference(("a", "a"), ("a",)) == "file sets differ"
    assert _difference(("a", "b", "c", "d", "e"), ()) == "unexpected a, b, c ..."
    assert _difference((), ("a", "b")) == "missing a, b"


class FlakyVersions(RecordedSnowflake):
    """SHOW VERSIONS fails once, after CREATE CORTEX EXTENSION already succeeded."""

    def __init__(self) -> None:
        super().__init__(existing=())
        self.fail_next = False

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        if self.fail_next and qualified_name.sql in self.extensions:
            self.fail_next = False
            raise SnowflakePortError("transient SHOW VERSIONS failure")
        return super().extension_versions(qualified_name)


def test_a_read_back_failure_after_create_keeps_ownership_and_the_retry_converges() -> None:
    port = FlakyVersions()
    compiled = compile_catalog(SkillCatalog((skill(),)))
    port.fail_next = True
    _, failed, after = publish(port, compiled, state())
    outcome = failed.outcomes[0]
    assert outcome.status is OutcomeStatus.FAILED and outcome.write_succeeded
    assert "DB.S.MONTH_CLOSE" in port.extensions
    # State remembers the extension SST created, so it is not "unmanaged" next time.
    assert after.applied["skill:month-close"].outcome == FAILED_AFTER_WRITE
    retry, retried, converged = publish(port, compiled, after)
    assert [(change.action, change.reason) for change in retry.changes] == [
        (Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH)
    ]
    assert retried.success and converged.applied["skill:month-close"].outcome == "applied"
    final, _, _ = publish(port, compiled, converged)
    assert [change.action for change in final.changes] == [Action.NOOP]


class FlakyUploads(RecordedSnowflake):
    """The second upload fails once, before any extension exists."""

    def __init__(self) -> None:
        super().__init__(existing=())
        self.puts = 0

    def upload(self, stage_path: str, content: bytes) -> None:
        self.puts += 1
        if self.puts == 2:
            raise SnowflakePortError("transient PUT failure")
        super().upload(stage_path, content)


def test_uploads_alone_never_make_state_claim_an_extension() -> None:
    port = FlakyUploads()
    compiled = compile_catalog(SkillCatalog((skill(),)))
    _, failed, after = publish(port, compiled, state())
    assert failed.outcomes[0].status is OutcomeStatus.FAILED and not failed.outcomes[0].write_succeeded
    assert port.extensions == {} and after.applied == {}
    retry, retried, converged = publish(port, compiled, after)
    assert [change.action for change in retry.changes] == [Action.CREATE]
    assert retried.success and converged.applied["skill:month-close"].outcome == "applied"
