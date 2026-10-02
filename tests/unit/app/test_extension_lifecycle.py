"""Skills and plugins publish content-addressed Cortex Extension versions safely."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.extensions import (
    ExtensionLifecycleHandler,
    _difference,
    _recorded_target,
    _stale,
    _version_number,
)
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyResult,
    ChangeReason,
    ChangeSet,
    ExecResult,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.ports.snowflake import ExtensionObservation, ExtensionVersion, SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import FAILED_AFTER_WRITE, STATE_SCHEMA_VERSION, AppliedEntry, State
from tests.helpers.app_ports import FixedClock, InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.recorded_snowflake import RecordedSnowflake
from tests.helpers.sql_values import statement, texts

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


def publish(
    port: RecordedSnowflake, compiled: dict[str, CompiledExtension], previous: State
) -> tuple[ChangeSet, ApplyResult, State]:
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

    # A member that fails catalog validation blocks its plugin as well as itself.
    unsafe = replace(skill(), files=(*skill().files, SkillFile("reference/q1+q2.md", b"q")))
    kit = Plugin("kit", "plugins/kit", "p", "d", None, ("month-close",), Origin("p"))
    result = CompileSkills(SkillCatalog((unsafe,), (kit,)), CHANNEL).run_result()
    assert result.compiled == ()
    assert [(item.code, item.subject) for item in result.diagnostics if item.code != "SST-VAL813"] == [
        ("SST-VAL857", "skill:month-close"),
        ("SST-VAL836", "plugin:kit"),
    ]


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
    unmanaged.execute_script(
        (statement("CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'SKILL' COMMENT = 'x'"),)
    )
    changeset, _, _ = publish(unmanaged, compiled, state())
    assert [(change.action, change.reason) for change in changeset.changes] == [
        (Action.BLOCKED, ChangeReason.UNMANAGED_OBJECT)
    ]
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN024"]

    mismatch = RecordedSnowflake(existing=())
    mismatch.execute_script(
        (statement("CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'PLUGIN' COMMENT = 'x'"),)
    )
    changeset, _, _ = publish(mismatch, compiled, state({"skill:month-close": owned}))
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN002"]

    client_side = RecordedSnowflake(existing=())
    client_side.stage_types[STAGE.sql] = "INTERNAL"
    changeset, _, _ = publish(client_side, compiled, state())
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN026"]

    damaged = RecordedSnowflake(existing=())
    damaged.execute_script(
        (
            statement(
                "CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'SKILL' COMMENT = 'Close the month.'"
            ),
        )
    )
    damaged.execute_script(
        (statement(f"ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE ADD VERSION {release.alias} FROM @DB.S.EMPTY/"),)
    )
    changeset, _, _ = publish(damaged, compiled, state({"skill:month-close": owned}))
    assert [item.code for item in changeset.diagnostics] == ["SST-PLN027"]
    assert "missing" in changeset.diagnostics[0].message


def test_empty_version_and_upload_failures_are_partial_writes() -> None:
    compiled = compile_catalog(SkillCatalog((skill(),)))
    empty = RecordedSnowflake(existing=())
    record = empty._record_extension_statement

    def swallow_files(normalized: str, original: str | None = None) -> None:
        # Reproduce the measured defect: ADD VERSION accepts a wrong path and
        # creates an empty version.
        record(normalized.replace("FROM @DB.S.SKILL_BUNDLES/", "FROM @DB.S.NOWHERE/"), original)

    empty._record_extension_statement = swallow_files  # type: ignore[method-assign]
    _, result, after = publish(empty, compiled, state())
    assert not result.success
    assert result.outcomes[0].error is not None and result.outcomes[0].error.code == "SST-APL016"
    assert result.outcomes[0].write_succeeded is True
    assert after.applied["skill:month-close"].outcome == "failed_after_write"

    unreadable = RecordedSnowflake(existing=())
    unreadable.read_staged_file = lambda stage_path: b"tampered"  # type: ignore[method-assign]
    _, result, _ = publish(unreadable, compiled, state())
    assert result.outcomes[0].error is not None and "byte for byte" in result.outcomes[0].error.message


def test_comment_drift_and_certification_with_readback() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_catalog(SkillCatalog((skill(),)))
    _, _, after = publish(port, compiled, state())
    port.execute_script((statement("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET COMMENT = 'drifted'"),))
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
    record = silent._record_extension_statement

    def ignore_tags(normalized: str, original: str | None = None) -> None:
        if "SET TAG" not in normalized:
            record(normalized, original)

    silent._record_extension_statement = ignore_tags  # type: ignore[method-assign]
    _, result, _ = publish(silent, certified, state())
    assert result.outcomes[0].error is not None and "reports certification" in result.outcomes[0].error.message


def observation(port: RecordedSnowflake, name: QualifiedName) -> ExtensionObservation:
    """Return what `port` reports for the extension `name`, which the test has published."""
    observed = port.observe_extension(name)
    assert observed is not None
    return observed


def served_warnings(changeset: ChangeSet) -> list[str]:
    return [item.message for item in changeset.diagnostics if item.code == "SST-VAL841"]


def test_the_catalog_keeps_serving_a_certified_version_over_an_uncertified_one() -> None:
    port = RecordedSnowflake(existing=())
    certified_channel = replace(CHANNEL, certified=True)
    first = compile_catalog(SkillCatalog((skill(),)), certified_channel)
    changeset, result, after = publish(port, first, state())
    assert result.success and served_warnings(changeset) == []
    target = QualifiedName.parse("DB.S.MONTH_CLOSE")
    assert observation(port, target).latest_certified_version == "VERSION$2"

    uncertified = compile_catalog(SkillCatalog((skill(body="Read reference/steps.md twice.\n"),)))
    changeset, result, after_change = publish(port, uncertified, after)
    assert [change.action for change in changeset.changes] == [Action.UPDATE]
    alias = uncertified["skill:month-close"].release.alias
    assert served_warnings(changeset) == [
        f"skill:month-close: the catalog will serve VERSION$2 of DB.S.MONTH_CLOSE, not {alias}, "
        "because it is the latest certified version"
    ]
    observed = observation(port, target)
    assert (observed.effective_version, observed.latest_certified_version) == ("VERSION$2", "VERSION$2")

    # Certifying the newer version makes it the one the catalog serves.
    newer = compile_catalog(SkillCatalog((skill(body="Read reference/steps.md twice.\n"),)), certified_channel)
    changeset, result, _ = publish(port, newer, after_change)
    assert [change.action for change in changeset.changes] == [Action.UPDATE]
    assert result.success and served_warnings(changeset) == []
    assert observation(port, target).effective_version == "VERSION$3"

    # Reverting to the older version cannot outrank the later certified one.
    changeset, _, _ = publish(port, first, after_change)
    assert served_warnings(changeset) == [
        f"skill:month-close: the catalog will serve VERSION$3 of DB.S.MONTH_CLOSE, not "
        f"{first['skill:month-close'].release.alias}, because it is the latest certified version"
    ]


class LaggingCertification(RecordedSnowflake):
    """Some pipeline-tagged versions report an empty status while the extension names them."""

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        return tuple(replace(item, certification_status=None) for item in super().extension_versions(qualified_name))


def test_the_extension_latest_certified_version_counts_as_certified() -> None:
    port = LaggingCertification(existing=())
    certified = compile_catalog(SkillCatalog((skill(),)), replace(CHANNEL, certified=True))
    changeset, result, after = publish(port, certified, state())
    assert result.success, result.outcomes[0].error
    tags = [statement for script in port.scripts for statement in script if "SET TAG" in statement]
    assert len(tags) == 1
    changeset, result, _ = publish(port, certified, after)
    assert [change.action for change in changeset.changes] == [Action.NOOP]
    assert served_warnings(changeset) == []


def test_served_version_prediction_edges() -> None:
    port = RecordedSnowflake(existing=())
    compiled = compile_catalog(SkillCatalog((skill(),)))
    _, _, after = publish(port, compiled, state())
    changed = compile_catalog(SkillCatalog((skill(body="Read reference/steps.md twice.\n"),)))
    _, _, after_change = publish(port, changed, after)
    # Nothing is certified, so a revert is served only if it is the default.
    changeset, _, _ = publish(port, compiled, after_change)
    assert [message.split(", because ")[-1] for message in served_warnings(changeset)] == ["it is the default version"]
    # A certified revert with no later certified version becomes the one served.
    certified = compile_catalog(SkillCatalog((skill(),)), replace(CHANNEL, certified=True))
    changeset, result, _ = publish(port, certified, after_change)
    assert result.success and served_warnings(changeset) == []
    assert observation(port, QualifiedName.parse("DB.S.MONTH_CLOSE")).effective_version == "VERSION$2"
    assert _version_number("VERSION$12") == 12 and _version_number("LIVE") == -1


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


def test_prunes_and_changes_without_an_artifact_are_skipped() -> None:
    compiled = compile_catalog(SkillCatalog((skill(),)))
    port = RecordedSnowflake(existing=())
    handler = ExtensionLifecycleHandler(port, {key: item.release for key, item in compiled.items()}, "skill")
    artifact = compiled["skill:month-close"].rendered_artifact
    prune = handler.report_prune(
        "skill:month-close", AppliedEntry("f", "DB.S.MONTH_CLOSE", "now", "run", "a", "f", "m")
    )
    for change, ddl in (
        (prune, ""),
        # A prune never carries an artifact; if one does, its text is reported.
        (replace(prune, rendered=artifact), artifact.ddl),
        (replace(prune, action=Action.CREATE), ""),
    ):
        outcome = handler.apply(change, ApplyOptions(allow_prune=True))
        assert (outcome.status, outcome.attempts, outcome.ddl) == (OutcomeStatus.SKIPPED, 0, ddl)
    assert port.scripts == [] and port.uploads == []


class SiblingStage(RecordedSnowflake):
    """A sibling artifact creates the shared bundle stage just after this one's apply observes it.

    The first two stage reads are the plan's observation and the apply's; the third is
    the apply's, under the stage lock.
    """

    def __init__(self) -> None:
        super().__init__(existing=())
        self.reads = 0

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        self.reads += 1
        if self.reads == 3:
            self.stage_types[qualified_name.sql] = "INTERNAL NO CSE"
        return super().stage_type(qualified_name)


def test_a_bundle_stage_a_sibling_creates_during_apply_is_not_created_again() -> None:
    port = SiblingStage()
    _, result, after = publish(port, compile_catalog(SkillCatalog((skill(),))), state())
    assert result.success, result.outcomes
    assert [statement for script in port.scripts for statement in script if "CREATE STAGE" in statement] == []
    entry = after.applied["skill:month-close"]
    assert ("STAGE", "DB.S.SKILL_BUNDLES") in {tuple(resource) for resource in entry.applied_resources}


def test_a_certified_release_not_yet_published_predicts_no_other_served_version() -> None:
    port = RecordedSnowflake(existing=())
    _, _, after = publish(port, compile_catalog(SkillCatalog((skill(),))), state())
    certified = compile_catalog(
        SkillCatalog((skill(body="Read reference/steps.md twice.\n"),)), replace(CHANNEL, certified=True)
    )
    changeset, result, _ = publish(port, certified, after)
    assert [change.action for change in changeset.changes] == [Action.UPDATE]
    assert result.success and served_warnings(changeset) == []


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

    port.observe_extension = unreachable  # type: ignore[method-assign]
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

    def create_client_side(statements: Sequence[Sql]) -> ExecResult:
        created.extend(text for text in texts(statements) if text.startswith("CREATE STAGE"))
        return execute(statements)

    client_side.execute_script = create_client_side  # type: ignore[method-assign]
    client_side.stage_type = lambda qualified_name: "INTERNAL" if created else None  # type: ignore[method-assign]
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
    drifted.execute_script((statement("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET COMMENT = 'drifted'"),))
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
