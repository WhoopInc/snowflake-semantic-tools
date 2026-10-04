"""`sst test`: the offline golden suite and the connected smoke and eval suites."""

from __future__ import annotations

import json
import shutil
from hashlib import md5
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunResult, EvalSuiteResult
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.compile import compile_result as _compile_result
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest as _build_manifest
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EvalBaselineRecord,
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from tests.helpers.cli_projects import common, compile_project, invoke_counting_closes, invoke_with_port
from tests.helpers.eval_builders import EvalSnowflake
from tests.helpers.eval_state_store import InMemoryEvalStateStore
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, FIXTURE, REPO_ROOT, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def test_golden_suite_compares_every_compiled_view() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "test",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(DBT_MANIFEST),
            "--suite",
            "golden",
            "--golden-dir",
            str(REPO_ROOT / "tests" / "golden" / "expected" / "ddl"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "golden suite passed for 14 artifact(s)" in result.output


def test_golden_suite_reports_a_diff(tmp_path: Path) -> None:
    expected_root = tmp_path / "expected"
    shutil.copytree(REPO_ROOT / "tests" / "golden" / "expected", expected_root)
    golden_dir = expected_root / "ddl"
    path = golden_dir / "jaffle_minimal.sql"
    path.write_text(path.read_text(encoding="utf-8").replace("Menu products only", "Drifted"), encoding="utf-8")
    result = CliRunner().invoke(
        cli,
        [
            "test",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(DBT_MANIFEST),
            "--suite",
            "golden",
            "--golden-dir",
            str(golden_dir),
        ],
    )
    assert result.exit_code != 0
    assert "golden suite failed" in result.output
    assert "-  COMMENT = 'Drifted" in result.output


def test_golden_suite_compares_eval_source_sql(tmp_path: Path) -> None:
    expected_root = tmp_path / "expected"
    shutil.copytree(REPO_ROOT / "tests" / "golden" / "expected", expected_root)
    source = expected_root / "eval" / "jaffle_analytics_source.sql"
    source.write_text(
        source.read_text(encoding="utf-8").replace("GROUND_TRUTH VARIANT", "GROUND_TRUTH VARCHAR"),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli,
        [
            "test",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(DBT_MANIFEST),
            "--suite",
            "golden",
            "--golden-dir",
            str(expected_root / "ddl"),
        ],
    )

    assert result.exit_code == 1
    assert "jaffle_analytics_source.sql" in result.output
    assert "GROUND_TRUTH VARCHAR" in result.output


def target() -> TargetIdentity:
    return TargetIdentity("verify", "account", Identifier.parse("db"), Identifier.parse("schema"))


def test_smoke_suite_is_separate_from_apply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
    from snowflake_semantic_tools.domain.state import AppliedEntry

    applied_state = {
        key: AppliedEntry(
            value["fingerprint"],
            value["publish_target"]["qualified_name"],
            "now",
            "run",
            "applied",
            value["fingerprint"],
            manifest_id,
        )
        for key, value in manifest["artifacts"].items()
    }
    markers = {
        value["publish_target"]["qualified_name"]: OwnershipMarker(
            manifest_id,
            value["fingerprint"],
        )
        for value in manifest["artifacts"].values()
    }
    port = FakeSnowflake(state=applied_state, markers=markers)
    result = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--output", "json"],
    )
    assert result.exit_code == 0
    attempted = json.loads(result.output)["data"]["attempted"]
    assert attempted > 3
    assert len(port.queries) == attempted

    unapplied = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--output", "json"],
    )
    assert unapplied.exit_code == 1
    assert json.loads(unapplied.output)["diagnostics"][0]["code"] == "SST-APL012"

    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    views.write_text(
        views.read_text(encoding="utf-8").replace("Menu products only", "Changed after compile"),
        encoding="utf-8",
    )
    stale = invoke_with_port(
        monkeypatch,
        FakeSnowflake(),
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--output", "json"],
    )
    assert stale.exit_code == 4
    assert "compiled SST manifest is stale" in json.loads(stale.output)["data"]["error"]


def test_eval_suite_uses_common_json_envelope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.project.git_sha", lambda path: "0000000")
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.run._compact_timestamp", lambda: "20260928T010203Z")
    compile_project(project)
    compiled = _compile_result(project_paths(project), "dev", DBT_MANIFEST)
    compiled_eval = next(item for item in compiled.compiled if isinstance(item, CompiledEval))
    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    eval_manifest = manifest["artifacts"][compiled_eval.artifact_key]
    config_content = compiled_eval.rendered.config_yaml.encode("utf-8")
    config_md5 = md5(config_content, usedforsecurity=False).hexdigest()
    config_path = (
        f"@SST_REF_DEV.JAFFLE.EVAL_CONFIGS/jaffle_analytics_agent/{compiled_eval.rendered.config_fingerprint}.yaml"
    )
    entry = AppliedEntry(
        compiled_eval.rendered_artifact.fingerprint,
        compiled_eval.dataset_target.sql,
        "now",
        "run",
        "applied",
        compiled_eval.rendered_artifact.fingerprint,
        manifest_id,
        component_fingerprints=(
            *compiled_eval.rendered_artifact.component_fingerprints,
            ("config_stage_md5", config_md5),
        ),
        physical_resources=tuple(
            (resource["object_type"], resource["qualified_name"]) for resource in eval_manifest["physical_resources"]
        ),
    )
    existing = tuple(resource["qualified_name"] for resource in eval_manifest["physical_resources"])
    port = EvalSnowflake(
        [],
        role="RECORDED_ROLE",
        account_locator="RECORDED_ACCOUNT",
        state={compiled_eval.artifact_key: entry},
        existing=(*existing, "SST_REF_DEV.JAFFLE.EVAL_CONFIGS"),
        stage_formats={"SST_REF_DEV.JAFFLE.EVAL_CONFIGS": EVAL_STAGE_FILE_FORMAT},
        staged_file_metadata={
            config_path: StagedFileMetadata(config_path, config_path[1:], len(config_content), config_md5)
        },
        staged_file_contents={config_path: config_content},
        agent_versions={(compiled_eval.agent_target.sql, "committed"): "VERSION$1"},
    )
    # The publish that finished left SST's version on the dataset.
    port.dataset_version_names[compiled_eval.dataset_target.sql] = [compiled_eval.dataset_version]
    result_columns = (
        "RECORD_ID",
        "INPUT_ID",
        "REQUEST_ID",
        "TIMESTAMP",
        "DURATION_MS",
        "INPUT",
        "OUTPUT",
        "ERROR",
        "GROUND_TRUTH",
        "METRIC_NAME",
        "EVAL_AGG_SCORE",
        "METRIC_TYPE",
        "METRIC_STATUS",
        "METRIC_CALLS",
        "TOTAL_INPUT_TOKENS",
        "TOTAL_OUTPUT_TOKENS",
        "LLM_CALL_COUNT",
    )
    metric_types = {
        **{
            str(metric.name): "system"
            for metric in compiled_eval.resolved.config.system_metrics
            if metric.name is not None
        },
        **{metric.name: "custom" for metric in compiled_eval.resolved.custom_metrics},
    }
    result_rows: tuple[tuple[object, ...], ...] = tuple(
        (
            f"record-{row_index}",
            f"question-{row_index}",
            f"request-{row_index}",
            "now",
            50,
            row["input_query"],
            "Answer",
            None,
            json.dumps(row["ground_truth"], sort_keys=True, separators=(",", ":")),
            metric_name,
            0.9,
            metric_type,
            {"status": 200},
            [],
            10,
            5,
            1,
        )
        for row_index, row in enumerate(json.loads(compiled_eval.rendered.dataset_payload))
        for metric_name, metric_type in metric_types.items()
    )
    response_values: list[QueryResult] = []
    for attempt_number in range(1, 6):
        suffix = "" if attempt_number == 1 else f"_R{attempt_number}"
        response_values.extend(
            (
                QueryResult(
                    ("RUN_NAME", "AGENT_NAME", "AGENT_TYPE", "STATUS", "STATUS_DETAILS"),
                    (
                        (
                            f"EVAL_JAFFLE_ANALYTICS_AGENT_0000000_ci_20260928T010203Z{suffix}",
                            "JAFFLE_ANALYTICS_AGENT",
                            "CORTEX AGENT",
                            "COMPLETED",
                            [],
                        ),
                    ),
                ),
                QueryResult(result_columns, result_rows),
            )
        )
    port.query_results.extend(response_values)
    store = InMemoryEvalStateStore()
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.test.SnowflakeEvalStateStore", lambda *args: store)
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.capture_baseline",
        lambda item, item_result, **kwargs: EvalBaselineRecord(
            item.artifact_key,
            item.rendered.dataset_fingerprint,
            item.rendered.config_fingerprint,
            item.resolved.config.agent_version or "",
            (),
            (),
            tuple(attempt.run_name for attempt in item_result.attempts),
            "2026-09-01T00:00:00Z",
            "2026-10-01T00:00:00Z",
            kwargs["reason"],
        ),
    )
    result = invoke_with_port(
        monkeypatch,
        port,
        [
            "test",
            *common(project),
            "--target",
            "dev",
            "--suite",
            "evals",
            "--capture-baseline",
            "--reason",
            "initial",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["suite"] == "evals"
    assert payload["data"]["attempt_count"] == 5
    assert payload["data"]["evals"][0]["attempts"][0]["terminal_status"] == "COMPLETED"
    assert payload["data"]["cost_totals"]["total_input_tokens"] == 200
    assert payload["data"]["regression_count"] == 0
    assert payload["data"]["gate_verdict"] == "captured"
    assert payload["data"]["captured_baselines"] == ["eval:jaffle_analytics_agent"]


def test_eval_suite_refuses_unpublished_eval_before_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = FakeSnowflake(state={})

    result = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--suite", "evals", "--output", "json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["diagnostics"][0]["code"] == "SST-APL012"
    assert payload["data"]["attempt_count"] == 0
    assert port.scripts == []


def test_eval_suite_reports_every_attempt_in_human_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compiled_eval = _compile_result(project_paths(project), "dev", DBT_MANIFEST)
    eval_item = next(item for item in compiled_eval.compiled if isinstance(item, CompiledEval))
    attempt = EvalRunAttempt(
        "EVAL_RUN_R2",
        2,
        "COMPLETED",
        (
            EvalResultRow(
                "question",
                "Question",
                (EvalMetricResult("question", "answer_correctness", 0.9, True),),
            ),
        ),
        EvalCostSummary(duration_ms=50, total_tokens=8, total_input_tokens=10, total_output_tokens=5, llm_call_count=1),
        agent_version="LAST",
    )
    suite = EvalSuiteResult(
        (EvalRunResult(eval_item.artifact_key, (attempt,), DiagnosticBag(), True),), DiagnosticBag()
    )
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.run.RunEvalSuite.run", lambda *args, **kwargs: suite)
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.validate_eval_publication", lambda *args: DiagnosticBag()
    )
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.read_state",
        lambda *args, **kwargs: (State.empty(target()), DiagnosticBag()),
    )
    monkeypatch.setattr(
        "snowflake_semantic_tools.cli.wiring.manifest.ManifestFileStore.read",
        lambda self: _build_manifest(project_paths(project), compiled_eval, DBT_MANIFEST),
    )
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.capture_baseline",
        lambda item, item_result, **kwargs: EvalBaselineRecord(
            item.artifact_key,
            item.rendered.dataset_fingerprint,
            item.rendered.config_fingerprint,
            item.resolved.config.agent_version or "",
            (),
            (),
            tuple(attempt.run_name for attempt in item_result.attempts),
            "2026-09-01T00:00:00Z",
            "2026-10-01T00:00:00Z",
            kwargs["reason"],
        ),
    )

    result = invoke_with_port(
        monkeypatch,
        FakeSnowflake(),
        [
            "test",
            *common(project),
            "--target",
            "dev",
            "--suite",
            "evals",
            "--capture-baseline",
            "--reason",
            "initial",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "attempt 2: EVAL_RUN_R2 COMPLETED" in result.output
    assert "answer_correctness: passed=1/1 average=0.9" in result.output
    assert "duration_ms=50 tokens=8" in result.output


def test_eval_suite_preflight_failure_keeps_json_schema_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    result = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["test", *common(project), "--target", "dev", "--suite", "evals", "--output", "json"],
    )

    payload = json.loads(result.output)
    assert result.exit_code == 1
    assert payload["data"]["cost_totals"] == {
        "duration_ms": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "llm_call_count": 0,
        "credits": None,
        "credit_attribution": "not_requested",
    }
    assert payload["data"]["regression_count"] == 0
    assert payload["data"]["gate_verdict"] == "not_evaluated"


def test_eval_baseline_capture_requires_reason(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(
        cli,
        ["test", *common(project), "--suite", "evals", "--capture-baseline", "--output", "json"],
    )
    assert result.exit_code == 3
    assert "requires --reason" in json.loads(result.output)["data"]["error"]


def test_the_eval_suite_closes_its_connection_when_it_fails_before_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    evals = ["test", *common(project), "--target", "dev", "--suite", "evals", "--output", "json"]
    StateFileStore(project / "target" / "sst" / "state.dev.json").acquire_lock("apply-run", break_stale=False)
    locked, closes = invoke_counting_closes(monkeypatch, FakeSnowflake(state={}), evals)
    assert locked.exit_code == 4 and "apply-run holds the target lock" in json.loads(locked.output)["data"]["error"]
    assert closes == ["closed"]

    def unwritable(self: StateFileStore, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        raise PermissionError("target/sst is read-only")

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.project.StateFileStore.acquire_lock", unwritable)
    failed, closes = invoke_counting_closes(monkeypatch, FakeSnowflake(state={}), evals)
    assert failed.exit_code == 4 and json.loads(failed.output)["data"]["error"] == "target/sst is read-only"
    assert closes == ["closed"]


def test_golden_json_failure_missing_file_and_smoke_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    empty_golden = tmp_path / "golden"
    empty_golden.mkdir()
    missing = CliRunner().invoke(
        cli,
        ["test", *common(project), "--suite", "golden", "--golden-dir", str(empty_golden), "--output", "json"],
    )
    assert missing.exit_code == 4
    assert len(json.loads(missing.output)["data"]["failures"]) == 15
    assert len(json.loads(missing.output)["data"]["missing"]) == 15

    from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
    from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
    from snowflake_semantic_tools.domain.state import AppliedEntry

    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    state = {
        key: AppliedEntry(
            value["fingerprint"],
            value["publish_target"]["qualified_name"],
            "now",
            "run",
            "applied",
            value["fingerprint"],
            manifest_id,
        )
        for key, value in manifest["artifacts"].items()
    }
    markers = {
        value["publish_target"]["qualified_name"]: OwnershipMarker(
            manifest_id,
            value["fingerprint"],
        )
        for value in manifest["artifacts"].values()
    }
    port = FakeSnowflake(state=state, markers=markers)
    port.query = lambda sql, params=None: (_ for _ in ()).throw(SnowflakePortError("broken"))  # type: ignore[method-assign]
    smoke = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--fail-fast"],
    )
    assert smoke.exit_code == 1 and "SST-APL100" in smoke.output
