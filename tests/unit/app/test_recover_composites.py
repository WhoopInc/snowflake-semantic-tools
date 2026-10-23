"""What each composite handler proves when `sst state recover` asks about an unrecorded artifact."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.coco_desktop_profiles import CompiledCocoDesktopProfile
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.skills import CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.coco_desktop_profiles import CocoDesktopProfileLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.composite import CompositeHandler
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.model.eval import EvalGroundTruth, EvalQuestion
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    ABSENT,
    BUNDLE_DIFFERS,
    BUNDLE_PREFIX_MISSING,
    CONFIG_DIFFERS,
    OTHER_DEFINITION,
    PROVENANCE_DIFFERS,
    RECORDED,
    ROW_DIFFERS,
    UNMARKED,
    UNOBSERVABLE,
    Action,
    ApplyOptions,
    Change,
    CompositeObservation,
    RecoveryProof,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import changeset, empty_state, target
from tests.helpers.clocks import FixedClock
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import compile_eval, resolved_eval
from tests.helpers.publications import (
    PROFILE_REGISTRY,
    PROFILE_STAGE,
    compiled_profile,
    compiled_skill,
    publish_profile,
    publish_skill,
    skill,
)
from tests.helpers.recover_changes import composite
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.snowflake_fake.extensions import versions

_MANIFEST = build_manifest(CompileSkills(SkillCatalog(), None).run_result())


def _planned(port: FakeSnowflake, handler: CompositeLifecycleHandler, rendered: RenderedArtifact) -> Change:
    """Plan one artifact with state lost: nothing recorded for it."""
    plan = PlanArtifacts(port, lifecycle_handlers={handler.artifact_type: handler})
    [change] = plan.run({rendered.key: rendered}, _MANIFEST, empty_state(), target(), fetched_at="now").changes
    return change


def _as_applied(proof: RecoveryProof, entry: AppliedEntry) -> None:
    """Require the proof to record exactly the parts and resources apply recorded."""
    assert proof.found == RECORDED
    assert tuple(sorted(proof.component_fingerprints)) == entry.component_fingerprints
    assert list(proof.physical_resources) == [
        (item.object_type, item.qualified_name) for item in entry.applied_resources
    ]


def test_a_handler_that_reads_no_proof_of_its_own_proves_nothing() -> None:
    assert CompositeHandler.recover(EvalLifecycleHandler(FakeSnowflake()), composite("S")) == RecoveryProof(UNMARKED)


# -- skills and plugins -------------------------------------------------------------------------


def _published_skill(
    compiled: CompiledExtension | None = None,
) -> tuple[FakeSnowflake, CompiledExtension, AppliedEntry]:
    port = FakeSnowflake()
    compiled = compiled or compiled_skill()
    _, result, state = publish_skill(port, compiled)
    assert result.success, result.diagnostics
    return port, compiled, state.applied[compiled.artifact_key]


def _skill_proof(port: FakeSnowflake, compiled: CompiledExtension) -> RecoveryProof:
    handler = ExtensionLifecycleHandler(port, {compiled.artifact_key: compiled.release}, "skill")
    return handler.recover(_planned(port, handler, compiled.rendered_artifact))


def test_a_published_skill_is_proved_by_its_version_and_bundle_prefix_as_apply_recorded_it() -> None:
    port, compiled, entry = _published_skill()
    handler = ExtensionLifecycleHandler(port, {compiled.artifact_key: compiled.release}, "skill")
    planned = _planned(port, handler, compiled.rendered_artifact)
    assert [item.code for item in planned.diagnostics] == ["SST-PLN024"]
    proof = _skill_proof(port, compiled)
    _as_applied(proof, entry)
    assert proof.qualified_name == compiled.release.target.sql and dict(proof.component_fingerprints)["version"]


def test_an_uncertified_version_whose_content_is_exact_is_recorded_for_the_next_apply_to_certify() -> None:
    port, compiled, _ = _published_skill()
    certified = compiled_skill(certified=True)
    assert certified.release.alias == compiled.release.alias
    assert _skill_proof(port, certified).found == RECORDED


def _edit_comment(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    port.extensions[compiled.release.target.sql]["comment"] = "edited by hand"


def _edit_prefix_bytes(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    path = compiled.release.paths[-1]
    port.staged_file_contents[f"{compiled.release.prefix}{path}"] = b"other\n"


def _add_to_prefix(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    port.stage_file(f"{compiled.release.prefix}skills/month-close/extra.md", b"x")


def _empty_prefix(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    port.stage_files = {path for path in port.stage_files if not path.startswith(compiled.release.prefix)}


def _retype(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    port.extensions[compiled.release.target.sql]["type"] = "PLUGIN"


def _version_files(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    [version] = versions(port.extensions[compiled.release.target.sql])
    version["files"] = list(compiled.release.paths[:1])


def _client_encrypted(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    port.stage_types[compiled.release.stage.sql] = "INTERNAL"


def _stage_dropped(port: FakeSnowflake, compiled: CompiledExtension) -> None:
    port.stage_types.pop(compiled.release.stage.sql, None)
    port.stage_formats.pop(compiled.release.stage.sql, None)
    port.existing = set()


@pytest.mark.parametrize(
    ("edit", "found"),
    [
        (_edit_comment, OTHER_DEFINITION),
        (_retype, OTHER_DEFINITION),
        (_version_files, BUNDLE_DIFFERS),
        (_edit_prefix_bytes, BUNDLE_DIFFERS),
        (_add_to_prefix, BUNDLE_DIFFERS),
        (_empty_prefix, BUNDLE_PREFIX_MISSING),
        (_client_encrypted, OTHER_DEFINITION),
        (_stage_dropped, BUNDLE_PREFIX_MISSING),
    ],
    ids=[
        "comment differs",
        "wrong type",
        "version files differ",
        "prefix bytes differ",
        "prefix holds more",
        "prefix missing",
        "client-encrypted stage",
        "stage dropped",
    ],
)
def test_a_skill_whose_live_content_is_not_exactly_the_release_is_not_recovered(
    edit: Callable[[FakeSnowflake, CompiledExtension], None], found: str
) -> None:
    port, compiled, _ = _published_skill()
    edit(port, compiled)
    proof = _skill_proof(port, compiled)
    assert (proof.found, proof.component_fingerprints, proof.physical_resources) == (found, (), ())


def test_an_extension_holding_only_an_older_alias_holds_another_definition() -> None:
    port, _, _ = _published_skill()
    edited = compiled_skill(steps=b"new steps\n")
    assert _skill_proof(port, edited).found == OTHER_DEFINITION


def test_an_extension_is_proved_under_its_own_target_only() -> None:
    # The same bundle compiled into another schema: the content is exact, the object is absent.
    port, compiled, _ = _published_skill()
    elsewhere = replace(compiled, release=replace(compiled.release, target=QualifiedName.parse("DB.OTHER.MONTH_CLOSE")))
    assert elsewhere.release.alias == compiled.release.alias
    assert _skill_proof(port, elsewhere).found == ABSENT


def test_an_extension_the_plan_could_not_observe_is_unobservable() -> None:
    port, compiled, _ = _published_skill()
    handler = ExtensionLifecycleHandler(port, {compiled.artifact_key: compiled.release}, "skill")
    planned = replace(
        _planned(port, handler, compiled.rendered_artifact),
        composite_observation=CompositeObservation(compiled.artifact_key),
    )
    assert handler.recover(planned).found == UNOBSERVABLE
    assert handler.recover(replace(planned, composite_observation=None)).found == UNOBSERVABLE


def test_a_bundle_prefix_that_cannot_be_listed_raises_for_recover_to_report() -> None:
    port, compiled, _ = _published_skill()
    handler = ExtensionLifecycleHandler(port, {compiled.artifact_key: compiled.release}, "skill")
    planned = _planned(port, handler, compiled.rendered_artifact)
    port.refuse("list_location")
    with pytest.raises(SnowflakePortError):
        handler.recover(planned)


# -- profiles -----------------------------------------------------------------------------------


def _published_profile() -> tuple[FakeSnowflake, CompiledCocoDesktopProfile, AppliedEntry]:
    port = FakeSnowflake()
    compiled = compiled_profile(skill())
    _, result, state = publish_profile(port, compiled)
    assert result.success, result.diagnostics
    return port, compiled, state.applied[compiled.artifact_key]


def _profile_proof(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> RecoveryProof:
    handler = CocoDesktopProfileLifecycleHandler(port, {compiled.artifact_key: compiled})
    return handler.recover(_planned(port, handler, compiled.rendered_artifact))


def _row(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> dict[str, object]:
    return port.profile_rows[PROFILE_REGISTRY.sql][compiled.name]


def test_a_published_profile_row_desktop_reads_as_the_release_is_proved_as_apply_recorded_it() -> None:
    port, compiled, entry = _published_profile()
    proof = _profile_proof(port, compiled)
    _as_applied(proof, entry)
    assert proof.qualified_name == f"{PROFILE_REGISTRY.sql} row 'analyst'"


def test_an_inactive_row_whose_content_is_exact_is_recorded_for_the_next_apply_to_reactivate() -> None:
    port, compiled, _ = _published_profile()
    _row(port, compiled)["ACTIVE"] = False
    assert _profile_proof(port, compiled).found == RECORDED


def _edit_description(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> None:
    _row(port, compiled)["DESCRIPTION"] = "Edited by hand."


def _older_version(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> None:
    _row(port, compiled)["VERSION"] = "SST_000000000000"


def _drop_column(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> None:
    port.tables[PROFILE_REGISTRY.sql] = tuple(item for item in port.tables[PROFILE_REGISTRY.sql] if item[0] != "HOOKS")


def _drop_registry(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> None:
    del port.tables[PROFILE_REGISTRY.sql]


def _delete_row(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> None:
    del port.profile_rows[PROFILE_REGISTRY.sql][compiled.name]


def _encrypt_stage(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> None:
    port.stage_types[PROFILE_STAGE.sql] = "INTERNAL"


def _drop_stage(port: FakeSnowflake, compiled: CompiledCocoDesktopProfile) -> None:
    port.stage_types.pop(PROFILE_STAGE.sql, None)
    port.stage_formats.pop(PROFILE_STAGE.sql, None)
    port.existing = set()


@pytest.mark.parametrize(
    ("edit", "found"),
    [
        (_edit_description, ROW_DIFFERS),
        (_drop_column, OTHER_DEFINITION),
        (_drop_registry, ABSENT),
        (_delete_row, ABSENT),
        (_encrypt_stage, OTHER_DEFINITION),
        (_drop_stage, BUNDLE_PREFIX_MISSING),
    ],
    ids=[
        "VERSION kept, content edited",
        "registry shape",
        "no registry",
        "no row",
        "client-encrypted stage",
        "no stage",
    ],
)
def test_a_profile_row_that_is_not_exactly_the_release_is_not_recovered(
    edit: Callable[[FakeSnowflake, CompiledCocoDesktopProfile], None], found: str
) -> None:
    port, compiled, _ = _published_profile()
    edit(port, compiled)
    proof = _profile_proof(port, compiled)
    assert (proof.found, proof.component_fingerprints, proof.physical_resources) == (found, (), ())


def test_a_profile_row_at_another_version_with_the_release_content_is_recovered_at_that_version() -> None:
    # VERSION is a counter apply chose; only the content Desktop loads says whose row it is.
    port, compiled, _ = _published_profile()
    _older_version(port, compiled)
    proof = _profile_proof(port, compiled)
    assert proof.found == RECORDED
    assert dict(proof.component_fingerprints)["version"] == "SST_000000000000"


def test_a_profile_name_more_than_one_row_carries_raises_for_recover_to_report() -> None:
    port, compiled, _ = _published_profile()
    handler = CocoDesktopProfileLifecycleHandler(port, {compiled.artifact_key: compiled})
    planned = _planned(port, handler, compiled.rendered_artifact)
    port.fail("read_profile_row", SnowflakePortError("2 rows have CONFIG_NAME 'analyst'"))
    with pytest.raises(SnowflakePortError):
        handler.recover(planned)


# -- evals --------------------------------------------------------------------------------------


def _eval(*, mint: str | None = None) -> RenderedArtifact:
    """The eval as plan renders it for publishing; with `mint`, its dataset's `mint` setting."""
    resolved = resolved_eval()
    if mint is not None:
        assert resolved.config.dataset is not None
        resolved = replace(
            resolved, config=replace(resolved.config, dataset=replace(resolved.config.dataset, mint=mint))
        )
    result = compile_eval(resolved)
    return compiled_as(result, CompiledEval).rendered_for_publish(build_manifest(result).manifest_id)


def _published_eval(
    artifact: RenderedArtifact, port: FakeSnowflake | None = None
) -> tuple[FakeSnowflake, AppliedEntry]:
    """Plan and apply the eval on an empty target, or on `port`; return it and what apply recorded."""
    if port is None:
        port = FakeSnowflake()
        port.existing = set()
    handler = EvalLifecycleHandler(port)
    store = InMemoryStateStore()
    result = ApplyArtifacts(
        port, store, FixedClock(), state_table=artifact.target, lifecycle_handlers={"eval": handler}
    ).run(changeset(_planned(port, handler, artifact)), empty_state(), ApplyOptions())
    assert result.success, result.diagnostics
    assert store.state is not None
    return port, store.state.applied[artifact.key]


def _eval_proof(port: FakeSnowflake, artifact: RenderedArtifact) -> RecoveryProof:
    handler = EvalLifecycleHandler(port)
    return handler.recover(_planned(port, handler, artifact))


def _config_path(artifact: RenderedArtifact) -> str:
    return EvalLifecycleHandler(FakeSnowflake()).config_path(artifact)


def test_a_minted_eval_is_proved_by_its_dataset_provenance_and_staged_config_as_apply_recorded_it() -> None:
    artifact = _eval()
    port, entry = _published_eval(artifact)
    planned = _planned(port, EvalLifecycleHandler(port), artifact)
    assert planned.action is Action.UPDATE
    _as_applied(_eval_proof(port, artifact), entry)


def _another_agent(port: FakeSnowflake, artifact: RenderedArtifact) -> None:
    dataset = dict(artifact.physical_resources)["DATASET"].sql
    version = dict(artifact.component_fingerprints)["dataset_version"]
    document = json.loads(port.dataset_version_metadata_text[(dataset, version)])
    port.dataset_version_metadata_text[(dataset, version)] = json.dumps({**document, "agent": "DB.S.OTHER_AGENT"})


def _drop_table(port: FakeSnowflake, artifact: RenderedArtifact) -> None:
    assert port.existing is not None
    port.existing.discard(dict(artifact.physical_resources)["TABLE"].sql)


def _edit_config(port: FakeSnowflake, artifact: RenderedArtifact) -> None:
    # The same size, one byte changed.
    content = artifact.ddl.encode("utf-8")
    port.stage_file(_config_path(artifact), b"#" + content[1:] if content[:1] != b"#" else b" " + content[1:])


def _resize_config(port: FakeSnowflake, artifact: RenderedArtifact) -> None:
    port.stage_file(_config_path(artifact), b"x")


def _remove_config(port: FakeSnowflake, artifact: RenderedArtifact) -> None:
    path = _config_path(artifact)
    port.stage_files.discard(path)
    port.staged_file_contents.pop(path)
    port.staged_file_metadata.pop(path)


@pytest.mark.parametrize(
    ("edit", "found"),
    [
        (_another_agent, PROVENANCE_DIFFERS),
        (_drop_table, PROVENANCE_DIFFERS),
        (_edit_config, CONFIG_DIFFERS),
        (_resize_config, CONFIG_DIFFERS),
        (_remove_config, CONFIG_DIFFERS),
    ],
    ids=["METADATA for another agent", "table gone", "config md5 differs", "config size differs", "config absent"],
)
def test_a_minted_eval_whose_objects_or_config_differ_is_not_recovered(
    edit: Callable[[FakeSnowflake, RenderedArtifact], None], found: str
) -> None:
    artifact = _eval()
    port, _ = _published_eval(artifact)
    edit(port, artifact)
    proof = _eval_proof(port, artifact)
    assert (proof.found, proof.component_fingerprints, proof.physical_resources) == (found, (), ())


def test_an_eval_published_from_earlier_questions_leaves_nothing_to_recover() -> None:
    port, _ = _published_eval(_eval())
    question = EvalQuestion(
        resolved_eval().dataset.questions[0].origin,
        "Changed question",
        EvalGroundTruth(resolved_eval().dataset.questions[0].origin, (), "Answer"),
    )
    changed = resolved_eval()
    changed = replace(changed, dataset=replace(changed.dataset, questions=(question,)))
    result = compile_eval(changed)
    artifact = compiled_as(result, CompiledEval).rendered_for_publish(build_manifest(result).manifest_id)
    assert _eval_proof(port, artifact).found == ABSENT


def test_an_eval_that_does_not_mint_is_proved_by_its_config_alone() -> None:
    artifact = _eval(mint="never")
    port = FakeSnowflake()
    port.existing = {dict(artifact.physical_resources)["DATASET"].sql}
    assert _eval_proof(port, artifact).found == ABSENT
    _, entry = _published_eval(artifact, port)
    proof = _eval_proof(port, artifact)
    _as_applied(proof, entry)
    assert proof.physical_resources == ()
    _edit_config(port, artifact)
    assert _eval_proof(port, artifact).found == CONFIG_DIFFERS


def test_an_eval_whose_observation_failed_or_found_errors_is_not_recovered() -> None:
    artifact = _eval()
    port, _ = _published_eval(artifact)
    handler = EvalLifecycleHandler(port)
    planned = _planned(port, handler, artifact)
    assert planned.composite_observation is not None
    assert (
        handler.recover(replace(planned, composite_observation=CompositeObservation(artifact.key))).found
        == UNOBSERVABLE
    )
    port.stage_formats[handler.config_stage(artifact).sql] = "TYPE=JSON"
    assert _eval_proof(port, artifact).found == OTHER_DEFINITION
