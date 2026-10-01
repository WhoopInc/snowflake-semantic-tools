from __future__ import annotations

import re
from dataclasses import replace
from hashlib import md5, sha256
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.evals import CompileEvals
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT, EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.eval import EvalCatalog, EvalGroundTruth, EvalQuestion
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ChangeReason,
    CompositeObservation,
    ExecResult,
    ExecutionError,
    OutcomeStatus,
    QueryResult,
)
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError
from snowflake_semantic_tools.domain.state import (
    STATE_SCHEMA_VERSION,
    AppliedEntry,
    AppliedResource,
    ResourceStatus,
    State,
)

from .conftest import FixedClock, InMemorySnowflake, InMemoryStateStore
from .helpers import changeset, target
from .test_eval_compile import compile_eval, resolved_eval

ORIGIN = Origin("dataset.yml", 1, 1)


def setup_eval():
    result = compile_eval()
    manifest = build_manifest(result)
    compiled = result.compiled[0]
    artifact = compiled.rendered_for_publish(manifest.manifest_id)
    port = InMemorySnowflake()
    port.existing = set()
    handler = EvalLifecycleHandler(port)
    return manifest, artifact, port, handler


def applied_entry(artifact, manifest_id: str) -> AppliedEntry:
    return AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        manifest_id,
        component_fingerprints=(
            *artifact.component_fingerprints,
            ("config_stage_md5", md5(artifact.ddl.encode("utf-8"), usedforsecurity=False).hexdigest()),
        ),
        physical_resources=tuple((kind, name.sql) for kind, name in artifact.physical_resources),
    )


def state_with(entry: AppliedEntry | None, manifest_id: str) -> State:
    applied = {} if entry is None else {"eval:sales_agent": entry}
    return State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest_id,
        "cfg",
        None,
        MappingProxyType(applied),
    )


def planned_change(artifact, manifest, port, handler, prior: State):
    return (
        PlanArtifacts(port, lifecycle_handlers={"eval": handler})
        .run(
            {artifact.key: artifact},
            manifest,
            prior,
            target(),
            fetched_at="now",
        )
        .changes[0]
    )


def seed_existing_resources(artifact, port) -> None:
    for _, name in artifact.physical_resources:
        port.existing.add(name.sql)
    port.table_row_counts[artifact.physical_resources[0][1].sql] = 1


def test_eval_plan_uses_component_fingerprints_and_live_resources() -> None:
    manifest, artifact, port, handler = setup_eval()
    assert handler.plan(artifact, None, manifest).action is Action.CREATE

    entry = applied_entry(artifact, manifest.manifest_id)
    for _, name in artifact.physical_resources:
        port.existing.add(name.sql)
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    config_path = handler._config_path(artifact)
    port.stage_files.add(config_path)
    port.staged_file_sizes[config_path] = len(artifact.ddl.encode("utf-8"))
    port.staged_file_md5s[config_path] = md5(artifact.ddl.encode("utf-8"), usedforsecurity=False).hexdigest()
    port.staged_file_contents[config_path] = artifact.ddl.encode("utf-8")
    assert handler.plan(artifact, entry, manifest).action is Action.NOOP

    changed = replace(
        artifact,
        component_fingerprints=(
            ("dataset", dict(artifact.component_fingerprints)["dataset"]),
            ("config", "c" * 64),
        ),
    )
    assert handler.plan(changed, entry, manifest).action is Action.UPDATE


def test_eval_apply_mints_dataset_creates_exact_stage_uploads_yaml_and_verifies() -> None:
    manifest, artifact, port, handler = setup_eval()
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    store = InMemoryStateStore()
    result = ApplyArtifacts(
        port,
        store,
        FixedClock(),
        state_table=artifact.target,
        lifecycle_handlers={"eval": handler},
    ).run(replace(changeset(change), manifest_id=manifest.manifest_id), state_with(None, ""))
    assert result.success
    assert len(port.scripts) == 4
    assert port.scripts[0][0].startswith("CREATE TABLE")
    assert port.scripts[1][0].startswith("INSERT INTO")
    assert "SYSTEM$CREATE_EVALUATION_DATASET" in port.scripts[2][0]
    assert port.scripts[3] == (
        f"CREATE STAGE IF NOT EXISTS DB.S.EVAL_CONFIGS FILE_FORMAT = ({EVAL_STAGE_FILE_FORMAT})",
    )
    assert port.uploads[0][0].endswith(".yaml")
    assert store.state is not None
    entry = store.state.applied[artifact.key]
    assert dict(entry.component_fingerprints)["dataset"] == dict(artifact.component_fingerprints)["dataset"]
    assert dict(entry.component_fingerprints)["config"] == dict(artifact.component_fingerprints)["config"]
    assert (
        dict(entry.component_fingerprints)["config_stage_md5"] == port.staged_file_md5s[handler._config_path(artifact)]
    )
    assert len(entry.physical_resources) == 2

    replanned = planned_change(
        artifact,
        manifest,
        port,
        handler,
        store.state,
    )
    assert replanned.action is Action.NOOP
    assert len(port.scripts) == 4
    assert len(port.uploads) == 1


def test_eval_ignores_encrypted_stage_size_when_readback_bytes_match() -> None:
    manifest, artifact, port, handler = setup_eval()
    entry = applied_entry(artifact, manifest.manifest_id)
    for _, name in artifact.physical_resources:
        port.existing.add(name.sql)
    port.table_row_counts[artifact.physical_resources[0][1].sql] = 1
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    config_path = handler._config_path(artifact)
    port.stage_files.add(config_path)
    port.staged_file_sizes[config_path] = 1
    port.staged_file_md5s[config_path] = "0" * 32
    port.staged_file_contents[config_path] = artifact.ddl.encode("utf-8")
    change = planned_change(artifact, manifest, port, handler, state_with(entry, manifest.manifest_id))

    outcome = handler.apply(change, ApplyOptions())

    assert change.action is Action.NOOP
    assert outcome.status is OutcomeStatus.SKIPPED
    assert port.uploads == []


def test_eval_same_size_wrong_staged_config_is_overwritten() -> None:
    manifest, artifact, port, handler = setup_eval()
    entry = applied_entry(artifact, manifest.manifest_id)
    for _, name in artifact.physical_resources:
        port.existing.add(name.sql)
    port.table_row_counts[artifact.physical_resources[0][1].sql] = 1
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    config_path = handler._config_path(artifact)
    port.stage_files.add(config_path)
    port.staged_file_sizes[config_path] = len(artifact.ddl.encode("utf-8"))
    port.staged_file_md5s[config_path] = "0" * 32
    port.staged_file_contents[config_path] = b"x" * len(artifact.ddl.encode("utf-8"))

    change = planned_change(artifact, manifest, port, handler, state_with(entry, manifest.manifest_id))
    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.APPLIED
    assert port.uploads == [(config_path, artifact.ddl.encode("utf-8"))]


def test_eval_apply_rejects_same_size_wrong_bytes_after_upload() -> None:
    manifest, artifact, port, handler = setup_eval()
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    original_upload = port.upload

    def corrupt_upload(stage_path: str, content: bytes) -> None:
        original_upload(stage_path, content)
        port.staged_file_contents[stage_path] = b"x" * len(content)

    port.upload = corrupt_upload  # type: ignore[method-assign]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL016"
    assert "bytes do not match" in outcome.error.message


def test_eval_config_only_update_uploads_no_dataset_sql() -> None:
    manifest, artifact, port, handler = setup_eval()
    entry = applied_entry(artifact, manifest.manifest_id)
    for _, name in artifact.physical_resources:
        port.existing.add(name.sql)
    port.table_row_counts[artifact.physical_resources[0][1].sql] = 1
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    changed = replace(
        artifact,
        ddl=artifact.ddl + "# changed\n",
        component_fingerprints=(
            ("dataset", dict(artifact.component_fingerprints)["dataset"]),
            ("config", "c" * 64),
        ),
    )
    prior = state_with(entry, manifest.manifest_id)
    change = planned_change(changed, manifest, port, handler, prior)
    assert change.action is Action.UPDATE
    outcome = handler.apply(change, ApplyOptions())
    assert outcome.status is OutcomeStatus.APPLIED
    assert port.scripts == []
    assert len(port.uploads) == 1


def test_eval_dataset_revision_retains_prior_immutable_resources_in_state() -> None:
    manifest, artifact, port, handler = setup_eval()
    previous_entry = applied_entry(artifact, manifest.manifest_id)
    changed_eval = replace(
        resolved_eval(),
        dataset=replace(
            resolved_eval().dataset,
            questions=(EvalQuestion(ORIGIN, "Changed question", EvalGroundTruth(ORIGIN, (), "Answer")),),
        ),
    )
    changed_result = compile_eval(changed_eval)
    changed_manifest = build_manifest(changed_result)
    changed = changed_result.compiled[0].rendered_for_publish(changed_manifest.manifest_id)
    change = planned_change(changed, manifest, port, handler, state_with(previous_entry, manifest.manifest_id))
    result = ApplyArtifacts(
        port,
        InMemoryStateStore(),
        FixedClock(),
        state_table=changed.target,
        lifecycle_handlers={"eval": handler},
    ).run(
        replace(changeset(change), manifest_id=manifest.manifest_id), state_with(previous_entry, manifest.manifest_id)
    )

    assert result.success
    saved = port.remote_state[artifact.key]
    assert saved.physical_resources[-2:] == tuple(
        AppliedResource(resource.object_type, resource.qualified_name, ResourceStatus.RETAINED)
        for resource in previous_entry.physical_resources
    )


def test_eval_dataset_revision_blocks_unmanaged_desired_resource() -> None:
    manifest, artifact, port, handler = setup_eval()
    previous_entry = applied_entry(artifact, manifest.manifest_id)
    changed_eval = replace(
        resolved_eval(),
        dataset=replace(
            resolved_eval().dataset,
            questions=(EvalQuestion(ORIGIN, "Changed question", EvalGroundTruth(ORIGIN, (), "Answer")),),
        ),
    )
    changed_result = compile_eval(changed_eval)
    changed = changed_result.compiled[0].rendered_for_publish(build_manifest(changed_result).manifest_id)
    port.existing.add(changed.physical_resources[0][1].sql)

    planned = handler.plan(changed, previous_entry, manifest)

    assert planned.action is Action.BLOCKED
    assert planned.reason is ChangeReason.UNMANAGED_OBJECT
    assert planned.diagnostics[0].code == "SST-PLN024"


def test_eval_partial_source_state_repairs_missing_dataset() -> None:
    manifest, artifact, port, handler = setup_eval()
    source_type, source_table = artifact.physical_resources[0]
    partial = replace(
        applied_entry(artifact, manifest.manifest_id),
        outcome="failed_after_write",
        physical_resources=((source_type, source_table.sql),),
    )
    port.existing.add(source_table.sql)
    port.table_row_counts[source_table.sql] = 1

    planned = handler.plan(artifact, partial, manifest)

    assert planned.action is Action.UPDATE
    assert planned.reason is ChangeReason.NOT_PRESENT


def test_eval_wrong_stage_format_blocks_without_altering_or_uploading() -> None:
    manifest, artifact, port, handler = setup_eval()
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = "TYPE='CSV' FIELD_DELIMITER=','"
    planned = handler.plan(artifact, None, manifest)
    assert planned.action is Action.BLOCKED
    assert planned.diagnostics[0].code == "SST-APL028"
    assert port.scripts == [] and port.uploads == []


def test_eval_stage_format_accepts_connector_describe_values() -> None:
    manifest, artifact, port, handler = setup_eval()
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = (
        "TYPE=CSV FIELD_DELIMITER=NONE RECORD_DELIMITER=\\n SKIP_HEADER=0 "
        "FIELD_OPTIONALLY_ENCLOSED_BY=NONE ESCAPE_UNENCLOSED_FIELD=NONE"
    )

    planned = handler.plan(artifact, None, manifest)

    assert planned.action is Action.CREATE


def test_eval_stage_format_drift_after_plan_is_rejected_as_observation_drift() -> None:
    manifest, artifact, port, handler = setup_eval()
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = "TYPE='CSV' FIELD_DELIMITER=','"
    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL012"


def two_evals_in_one_schema():
    first = resolved_eval()
    second = replace(
        first,
        agent=replace(first.agent, name="ops_agent"),
        dataset=replace(first.dataset, agent="ops_agent"),
        config=replace(first.config, agent="ops_agent"),
    )
    result = CompileEvals(
        EvalCatalog((first, second), first.custom_metrics),
        agent_targets={
            "sales_agent": QualifiedName.parse("DB.S.SALES_AGENT"),
            "ops_agent": QualifiedName.parse("DB.S.OPS_AGENT"),
        },
    ).run_result()
    manifest = build_manifest(result)
    return manifest, {item.artifact_key: item.rendered_for_publish(manifest.manifest_id) for item in result.compiled}


def test_evals_sharing_a_config_stage_this_run_creates_all_apply() -> None:
    manifest, artifacts = two_evals_in_one_schema()
    port = InMemorySnowflake()
    port.existing = set()
    handler = EvalLifecycleHandler(port)
    prior = state_with(None, "")
    # Both plans see the shared stage absent; the first apply then creates it.
    planned = PlanArtifacts(port, lifecycle_handlers={"eval": handler}).run(
        artifacts, manifest, prior, target(), fetched_at="now"
    )
    assert [(change.key, change.action) for change in planned.changes] == [
        ("eval:ops_agent", Action.CREATE),
        ("eval:sales_agent", Action.CREATE),
    ]

    result = ApplyArtifacts(
        port,
        InMemoryStateStore(),
        FixedClock(),
        state_table=artifacts["eval:sales_agent"].target,
        lifecycle_handlers={"eval": handler},
    ).run(planned, prior, ApplyOptions(parallelism=1))

    assert [item.code for item in result.diagnostics] == []
    assert [outcome.status for outcome in result.outcomes] == [OutcomeStatus.APPLIED, OutcomeStatus.APPLIED]
    assert [script for script in port.scripts if script[0].startswith("CREATE STAGE")] == [
        (f"CREATE STAGE IF NOT EXISTS DB.S.EVAL_CONFIGS FILE_FORMAT = ({EVAL_STAGE_FILE_FORMAT})",)
    ]
    assert len(port.uploads) == 2


def test_a_config_stage_created_outside_the_run_after_plan_is_still_drift() -> None:
    manifest, artifact, port, handler = setup_eval()
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    # The exact format SST would create, but made by someone else after the plan.
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL012"
    assert port.scripts == [] and port.uploads == []


def test_eval_partial_dataset_failure_records_only_written_source_table() -> None:
    manifest, artifact, port, handler = setup_eval()
    port.execute_results = [ExecResult(True), ExecResult(False, error=ExecutionError("dataset denied", "28000"))]
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    store = InMemoryStateStore()
    result = ApplyArtifacts(
        port,
        store,
        FixedClock(),
        state_table=artifact.target,
        lifecycle_handlers={"eval": handler},
    ).run(replace(changeset(change), manifest_id=manifest.manifest_id), state_with(None, ""))
    assert not result.success
    assert result.outcomes[0].write_succeeded
    assert result.diagnostics[-1].code == "SST-APL016"
    assert result.outcomes[0].physical_resources == (("TABLE", artifact.physical_resources[0][1].sql),)
    assert store.state is not None
    assert store.state.applied[artifact.key].outcome == "failed_after_write"


def test_eval_prune_is_report_only_even_when_prune_is_allowed() -> None:
    manifest, artifact, port, handler = setup_eval()
    entry = applied_entry(artifact, manifest.manifest_id)
    prior = state_with(entry, manifest.manifest_id)
    changeset_value = PlanArtifacts(port, lifecycle_handlers={"eval": handler}).run(
        {},
        manifest,
        prior,
        target(),
        fetched_at="now",
        include_prune=True,
        prune_types=frozenset(("eval",)),
    )
    prune = changeset_value.changes[0]
    assert prune.action is Action.PRUNE and not prune.prune_executable
    result = ApplyArtifacts(
        port,
        InMemoryStateStore(),
        FixedClock(),
        state_table=artifact.target,
        lifecycle_handlers={"eval": handler},
    ).run(changeset_value, prior, ApplyOptions(allow_prune=True))
    assert result.outcomes[0].status is OutcomeStatus.SKIPPED
    assert port.scripts == []


def test_eval_plan_blocks_snowflake_observation_errors() -> None:
    manifest, artifact, port, handler = setup_eval()

    def fail_observation(_object_type, _qualified_name):
        raise SnowflakePortError("observation failed")

    port.object_exists = fail_observation  # type: ignore[method-assign]

    planned = handler.plan(artifact, None, manifest)

    assert planned.action is Action.BLOCKED
    assert planned.reason is ChangeReason.VALIDATION_ERRORS
    assert planned.diagnostics[0].code == "SST-PLN001"
    assert "observation failed" in planned.diagnostics[0].message


def test_eval_plan_updates_when_prior_state_has_no_component_fingerprints() -> None:
    manifest, artifact, port, handler = setup_eval()
    entry = replace(applied_entry(artifact, manifest.manifest_id), component_fingerprints=())

    planned = handler.plan(artifact, entry, manifest)

    assert planned.action is Action.UPDATE
    assert planned.reason is ChangeReason.NO_PRIOR_STATE


def test_eval_plan_blocks_when_recorded_resource_target_moved() -> None:
    manifest, artifact, port, handler = setup_eval()
    entry = replace(
        applied_entry(artifact, manifest.manifest_id),
        physical_resources=(
            *applied_entry(artifact, manifest.manifest_id).physical_resources,
            ("TABLE", "DB.S.OLD_EVAL_SOURCE"),
        ),
    )

    planned = handler.plan(artifact, entry, manifest)

    assert planned.action is Action.BLOCKED
    assert planned.reason is ChangeReason.TARGET_MOVED
    assert planned.diagnostics[0].code == "SST-PLN025"


def test_eval_apply_rejects_missing_rendered_artifact() -> None:
    manifest, artifact, _port, handler = setup_eval()
    change = replace(
        handler.report_prune(artifact.key, applied_entry(artifact, manifest.manifest_id)), action=Action.CREATE
    )

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.message == "missing rendered eval artifact"
    assert outcome.ddl == ""


@pytest.mark.parametrize("action", (Action.NOOP, Action.BLOCKED))
def test_eval_apply_skips_non_writing_actions(action: Action) -> None:
    manifest, artifact, port, handler = setup_eval()
    change = replace(planned_change(artifact, manifest, port, handler, state_with(None, "")), action=action)

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.SKIPPED
    assert outcome.action is action
    assert outcome.ddl == artifact.ddl


def test_eval_apply_surfaces_planned_composite_diagnostics() -> None:
    manifest, artifact, port, handler = setup_eval()
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    observation = replace(
        change.composite_observation,
        diagnostics=DiagnosticBag((D("SST-APL028", value="DB.S.EVAL_CONFIGS", found="wrong", expected="right"),)),
    )

    outcome = handler.apply(replace(change, composite_observation=observation), ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL028"


@pytest.mark.parametrize(
    ("execution_result", "expected_code", "write_succeeded"),
    (
        (ExecResult(False), "SST-APL001", False),
        (ExecResult(False, ("query-1",)), "SST-APL016", True),
        (ExecResult(False, rows_affected=1), "SST-APL016", True),
    ),
)
def test_eval_source_create_failure_tracks_write_evidence(
    execution_result: ExecResult,
    expected_code: str,
    write_succeeded: bool,
) -> None:
    manifest, artifact, port, handler = setup_eval()
    port.execute_results = [execution_result]
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == expected_code
    assert outcome.error.message == "source-table publication failed"
    assert outcome.write_succeeded is write_succeeded
    assert outcome.physical_resources == ()


def test_eval_source_table_must_exist_after_create() -> None:
    manifest, artifact, port, handler = setup_eval()
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    original_execute = port.execute_script

    def omit_source_create(statements):
        if statements[0].lstrip().upper().startswith("CREATE TABLE "):
            port.scripts.append(tuple(statements))
            return ExecResult(True)
        return original_execute(statements)

    port.execute_script = omit_source_create  # type: ignore[method-assign]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL016"
    assert "is absent after publication" in outcome.error.message


@pytest.mark.parametrize(("preexisting", "expected_code"), ((False, "SST-APL016"), (True, "SST-APL001")))
def test_eval_source_row_count_port_errors_are_classified_by_write_state(
    preexisting: bool,
    expected_code: str,
) -> None:
    manifest, artifact, port, handler = setup_eval()
    if preexisting:
        seed_existing_resources(artifact, port)
        prior = state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id)
    else:
        prior = state_with(None, "")
    change = planned_change(artifact, manifest, port, handler, prior)
    port.query_error = SnowflakePortError("count denied")

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == expected_code
    assert "row count is unreadable: count denied" in outcome.error.message


def test_eval_source_row_count_must_match_rendered_questions() -> None:
    manifest, artifact, port, handler = setup_eval()
    seed_existing_resources(artifact, port)
    source_table = artifact.physical_resources[0][1].sql
    port.table_row_counts[source_table] = 0
    prior = state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id)
    change = planned_change(artifact, manifest, port, handler, prior)

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL016"
    assert f"{source_table} has 0 rows, expected 1" in outcome.error.message


@pytest.mark.parametrize(
    ("query_result", "expected_message"),
    (
        (QueryResult(), "COUNT(*) returned no row"),
        (QueryResult(("ROW_COUNT",), ((),)), "COUNT(*) returned no row"),
        (QueryResult(("ROW_COUNT",), (("1",),)), "COUNT(*) returned str"),
        (QueryResult(("ROW_COUNT",), ((True,),)), "COUNT(*) returned bool"),
    ),
)
def test_eval_source_row_count_rejects_unusable_query_results(
    query_result: QueryResult,
    expected_message: str,
) -> None:
    _manifest, artifact, port, handler = setup_eval()

    def return_result(_sql, _params=None):
        return query_result

    port.query = return_result  # type: ignore[method-assign]

    with pytest.raises(SnowflakePortError, match=re.escape(expected_message)):
        handler._source_row_count(artifact.physical_resources[0][1])


def test_eval_dataset_execution_failure_after_source_write_is_partial() -> None:
    manifest, artifact, port, handler = setup_eval()
    port.execute_results = [ExecResult(True), ExecResult(True), ExecResult(False)]
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL016"
    assert outcome.error.message == "dataset publication failed"
    assert outcome.write_succeeded
    assert outcome.physical_resources == (("TABLE", artifact.physical_resources[0][1].sql),)


def test_eval_dataset_execution_failure_without_prior_write_is_not_partial() -> None:
    manifest, artifact, port, handler = setup_eval()
    source_type, source_table = artifact.physical_resources[0]
    partial_entry = replace(
        applied_entry(artifact, manifest.manifest_id),
        physical_resources=((source_type, source_table.sql),),
    )
    port.existing.add(source_table.sql)
    port.table_row_counts[source_table.sql] = 1
    port.execute_results = [ExecResult(False, error=ExecutionError("dataset denied", "28000"))]
    change = planned_change(artifact, manifest, port, handler, state_with(partial_entry, manifest.manifest_id))

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL001"
    assert outcome.error.message == "dataset denied"
    assert not outcome.write_succeeded


def test_eval_dataset_must_exist_after_successful_creation() -> None:
    manifest, artifact, port, handler = setup_eval()
    change = planned_change(artifact, manifest, port, handler, state_with(None, ""))
    original_execute = port.execute_script

    def omit_dataset_create(statements):
        if "SYSTEM$CREATE_EVALUATION_DATASET" in statements[0].upper():
            port.scripts.append(tuple(statements))
            return ExecResult(True)
        return original_execute(statements)

    port.execute_script = omit_dataset_create  # type: ignore[method-assign]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL016"
    assert "is absent after publication" in outcome.error.message


@pytest.mark.parametrize(
    "execution_result",
    (ExecResult(False), ExecResult(False, error=ExecutionError("stage denied", "28000"))),
)
def test_eval_stage_creation_failure_is_reported(execution_result: ExecResult) -> None:
    manifest, artifact, port, handler = setup_eval()
    seed_existing_resources(artifact, port)
    prior = state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id)
    change = planned_change(artifact, manifest, port, handler, prior)
    port.execute_results = [execution_result]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.message in {"config-stage creation failed", "stage denied"}
    assert outcome.write_succeeded


def test_eval_stage_must_exist_after_successful_creation() -> None:
    manifest, artifact, port, handler = setup_eval()
    seed_existing_resources(artifact, port)
    prior = state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id)
    change = planned_change(artifact, manifest, port, handler, prior)
    original_execute = port.execute_script

    def omit_stage_create(statements):
        if statements[0].lstrip().upper().startswith("CREATE STAGE "):
            port.scripts.append(tuple(statements))
            return ExecResult(True)
        return original_execute(statements)

    port.execute_script = omit_stage_create  # type: ignore[method-assign]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL016"
    assert "is absent after creation" in outcome.error.message


def test_eval_stage_format_is_verified_after_creation() -> None:
    manifest, artifact, port, handler = setup_eval()
    seed_existing_resources(artifact, port)
    prior = state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id)
    change = planned_change(artifact, manifest, port, handler, prior)
    original_execute = port.execute_script

    def create_wrong_stage(statements):
        result = original_execute(statements)
        if statements[0].lstrip().upper().startswith("CREATE STAGE "):
            port.stage_formats["DB.S.EVAL_CONFIGS"] = "TYPE='CSV' FIELD_DELIMITER=','"
        return result

    port.execute_script = create_wrong_stage  # type: ignore[method-assign]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL028"
    assert outcome.write_succeeded


def test_eval_upload_port_error_is_reported() -> None:
    manifest, artifact, port, handler = setup_eval()
    seed_existing_resources(artifact, port)
    prior = state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id)
    change = planned_change(artifact, manifest, port, handler, prior)

    def fail_upload(_stage_path, _content):
        raise SnowflakePortError("upload denied")

    port.upload = fail_upload  # type: ignore[method-assign]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.message == "upload denied"
    assert outcome.write_succeeded


@pytest.mark.parametrize(
    ("failure_mode", "expected_message"),
    (
        ("absent", "is absent after upload"),
        ("unreadable", "is unreadable after upload"),
        ("size", "has 1 bytes, expected"),
    ),
)
def test_eval_uploaded_config_readback_is_verified(failure_mode: str, expected_message: str) -> None:
    manifest, artifact, port, handler = setup_eval()
    seed_existing_resources(artifact, port)
    prior = state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id)
    change = planned_change(artifact, manifest, port, handler, prior)

    def broken_upload(stage_path, content):
        port.uploads.append((stage_path, content))
        if failure_mode == "absent":
            return
        port.stage_files.add(stage_path)
        port.staged_file_sizes[stage_path] = len(content)
        port.staged_file_md5s[stage_path] = md5(content, usedforsecurity=False).hexdigest()
        if failure_mode == "size":
            port.staged_file_contents[stage_path] = b"x"

    port.upload = broken_upload  # type: ignore[method-assign]

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "SST-APL016"
    assert expected_message in outcome.error.message


def test_eval_apply_defensively_rejects_missing_config_fingerprint() -> None:
    manifest, artifact, port, handler = setup_eval()
    seed_existing_resources(artifact, port)
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    artifact = replace(
        artifact,
        component_fingerprints=(("dataset", dict(artifact.component_fingerprints)["dataset"]),),
    )
    observation = CompositeObservation(
        artifact.key,
        resources=tuple(handler._observe(replace(artifact, component_fingerprints=(("config", "c" * 64),))).resources),
        stage_exists=True,
        stage_file_format=EVAL_STAGE_FILE_FORMAT,
        config_path="@DB.S.EVAL_CONFIGS/sales_agent/config.yaml",
    )

    def fixed_config_path(_artifact):
        return "@DB.S.EVAL_CONFIGS/sales_agent/config.yaml"

    handler._config_path = fixed_config_path  # type: ignore[method-assign]
    change = replace(
        planned_change(
            replace(artifact, component_fingerprints=(("config", "c" * 64),)),
            manifest,
            port,
            handler,
            state_with(applied_entry(artifact, manifest.manifest_id), manifest.manifest_id),
        ),
        rendered=artifact,
        composite_observation=observation,
    )

    outcome = handler.apply(change, ApplyOptions())

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.message == "eval artifact has no config fingerprint"


def test_eval_recorded_resource_identity_ignores_non_active_eval_resources() -> None:
    manifest, artifact, _port, handler = setup_eval()
    source_name = artifact.physical_resources[0][1].sql
    entry = replace(
        applied_entry(artifact, manifest.manifest_id),
        physical_resources=(
            AppliedResource("TABLE", "DB.S.RETAINED", ResourceStatus.RETAINED),
            AppliedResource("STAGE", "DB.S.EVAL_CONFIGS"),
            AppliedResource("TABLE", ""),
            AppliedResource("TABLE", source_name),
        ),
    )

    identities = handler._recorded_resource_identity(entry)

    assert identities == frozenset((("TABLE", artifact.physical_resources[0][1].folded),))


def test_eval_merge_retains_only_superseded_table_and_dataset_resources() -> None:
    manifest, artifact, _port, handler = setup_eval()
    source_type, source_name = artifact.physical_resources[0]
    current = ((source_type, source_name.sql),)
    previous = replace(
        applied_entry(artifact, manifest.manifest_id),
        physical_resources=(
            (source_type, source_name.sql),
            ("DATASET", "DB.S.OLD_DATASET"),
            ("STAGE", "DB.S.EVAL_CONFIGS"),
        ),
    )

    merged = handler.merge_physical_resources(current, previous)

    assert merged == (
        (source_type, source_name.sql),
        ("DATASET", "DB.S.OLD_DATASET", ResourceStatus.RETAINED),
    )


def test_eval_prune_report_falls_back_to_artifact_key_without_resources() -> None:
    manifest, artifact, _port, handler = setup_eval()
    entry = replace(applied_entry(artifact, manifest.manifest_id), physical_resources=())

    change = handler.report_prune(artifact.key, entry)

    assert change.diagnostics[0].code == "SST-PLN021"
    assert artifact.key in change.diagnostics[0].message


def test_eval_helper_invariants_reject_malformed_artifacts() -> None:
    _manifest, artifact, _port, handler = setup_eval()
    without_config = replace(
        artifact,
        component_fingerprints=(("dataset", dict(artifact.component_fingerprints)["dataset"]),),
    )
    with pytest.raises(ValueError, match="requires a config fingerprint"):
        handler.config_path(without_config)

    with pytest.raises(ValueError, match="one TABLE and one DATASET"):
        handler._resources_by_type(replace(artifact, physical_resources=artifact.physical_resources[:1]))

    without_statements = replace(artifact, create_statements=("",))
    with pytest.raises(ValueError, match="CREATE TABLE and INSERT"):
        handler._source_statements(without_statements)
    with pytest.raises(ValueError, match="requires a dataset statement"):
        handler._dataset_statement(without_statements)

    assert handler._expected_row_count(()) == 0


def test_eval_config_path_sanitizes_unsafe_agent_names() -> None:
    _manifest, artifact, _port, handler = setup_eval()
    unsafe_name = "sales/team"
    artifact = replace(artifact, depends_on=(f"agent:{unsafe_name}",))
    digest = sha256(unsafe_name.encode("utf-8")).hexdigest()[:8]

    config_path = handler.config_path(artifact)

    assert f"/sales_team_{digest}/" in config_path
