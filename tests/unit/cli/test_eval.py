"""`sst eval`: gated eval runs of published agents, and the flags it refuses together."""

from __future__ import annotations

import dataclasses
import json
from hashlib import md5
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunResult, EvalSuiteResult
from snowflake_semantic_tools.app.evals.suite import EvalGateOutcome
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.compile import compile_result as _compile_result
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest as _build_manifest
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EvalBaselineRecord,
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.artifact_builders import target
from tests.helpers.cli_projects import (
    common,
    compile_project,
    invoke_counting_closes,
    invoke_with_port,
)
from tests.helpers.eval_builders import EvalSnowflake
from tests.helpers.eval_state_store import InMemoryEvalStateStore
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.snowflake_fake.world import Sent


class _EvalAnsweredByRunName(EvalSnowflake):
    """`EvalSnowflake` answering each eval status and results query by the run name it binds."""

    run_answers: dict[tuple[str, str], QueryResult]

    def query(self, sql: Any, params: object = None) -> QueryResult:
        text = str(sql)
        for marker, key, index in (
            ("EXECUTE_AI_EVALUATION('STATUS'", "STATUS", 0),
            ("GET_AI_EVALUATION_DATA", "RESULTS", -1),
        ):
            if isinstance(params, tuple) and marker in text:
                self.log.append(Sent("query", (text,), params))
                return self.run_answers[(key, str(params[index]))]
        return super().query(sql, params)


@pytest.mark.parametrize("output", ["json", "table"])
def test_eval_uses_common_json_envelope_and_prints_progress_only_as_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: str
) -> None:
    project = project_copy(tmp_path)
    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.project.git_sha", lambda path: "0000000")
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.run._compact_timestamp", lambda: "20260928T010203Z")
    compile_project(project)
    compiled = _compile_result(project_paths(project), "dev", DBT_MANIFEST)
    compiled_eval = next(item for item in compiled.compiled if isinstance(item, CompiledEval))
    manifest = json.loads((project / "sst_target" / "manifest.json").read_text(encoding="utf-8"))
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
    port = _EvalAnsweredByRunName(
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
    # The attempts run concurrently, so each status and results query is answered by the run
    # name it binds, not by a shared queue whose order depends on how the threads interleave.
    port.run_answers = {}
    for attempt_number in range(1, 6):
        suffix = "" if attempt_number == 1 else f"_R{attempt_number}"
        run_name = f"EVAL_JAFFLE_ANALYTICS_AGENT_0000000_ci_20260928T010203Z{suffix}"
        port.run_answers[("STATUS", run_name)] = QueryResult(
            ("RUN_NAME", "AGENT_NAME", "AGENT_TYPE", "STATUS", "STATUS_DETAILS"),
            ((run_name, "JAFFLE_ANALYTICS_AGENT", "CORTEX AGENT", "COMPLETED", []),),
        )
        port.run_answers[("RESULTS", run_name)] = QueryResult(result_columns, result_rows)
    store = InMemoryEvalStateStore()
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.eval.SnowflakeEvalStateStore", lambda *args: store)
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.capture_baseline",
        lambda item, item_result, **kwargs: EvalBaselineRecord(
            item.artifact_key,
            item.rendered.dataset_fingerprint,
            item.rendered.config_fingerprint,
            item.resolved.options.agent_version or "",
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
            "eval",
            *common(project),
            "--target",
            "dev",
            "--capture-baseline",
            "--reason",
            "initial",
            "--output",
            output,
        ],
    )

    assert result.exit_code == 0, result.output
    progress = [line for line in result.stderr.splitlines() if line.startswith(("eval run ", "waiting on "))]
    first = "EVAL_JAFFLE_ANALYTICS_AGENT_0000000_ci_20260928T010203Z"
    if output == "table":
        # Runs may start in any order; each start and each end is printed once. The capture
        # needs five completed runs and allows one retry.
        names = [first, *(f"{first}_R{number}" for number in range(2, 6))]
        assert sorted(line for line in progress if ": starting " in line) == [
            f"eval run {name}: starting (attempt {number} of 6)" for number, name in enumerate(names, start=1)
        ]
        assert sum(": ended COMPLETED after " in line for line in progress) == 5
        return
    # JSON output carries the envelope alone: no progress line reaches either stream.
    assert not progress
    payload = json.loads(result.stdout)
    assert payload["data"]["attempt_count"] == 5
    assert payload["data"]["evals"][0]["attempts"][0]["terminal_status"] == "COMPLETED"
    assert payload["data"]["cost_totals"]["total_input_tokens"] == 200
    assert payload["data"]["regression_count"] == 0
    assert payload["data"]["gate_verdict"] == "captured"
    assert payload["data"]["captured_baselines"] == ["eval:jaffle_analytics_agent"]


def test_eval_refuses_unpublished_eval_before_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = FakeSnowflake(state={})

    result = invoke_with_port(
        monkeypatch,
        port,
        ["eval", *common(project), "--target", "dev", "--output", "json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert [item["code"] for item in payload["diagnostics"]] == ["SST-APL037"]
    assert payload["data"]["attempt_count"] == 0
    assert port.scripts == []


def test_eval_breaks_an_expired_run_lock_only_with_break_stale_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    args = ["eval", *common(project), "--target", "dev", "--output", "json"]
    probe = FakeSnowflake(state={})
    tables: list[QualifiedName] = []
    claim = probe.run_locks.acquire_run_lock

    def recording(table: QualifiedName, *rest: Any, **options: Any) -> Any:
        tables.append(table)
        return claim(table, *rest, **options)

    probe.run_locks.acquire_run_lock = recording  # type: ignore[method-assign,assignment]
    invoke_with_port(monkeypatch, probe, args)

    def expired() -> FakeSnowflake:
        port = FakeSnowflake(state={})
        port.run_locks.acquire_run_lock(tables[0], "dev", LockClaim("crashed", "R", "h", 10), break_stale=False)
        port.run_locks.now = 60.0
        return port

    kept = invoke_with_port(monkeypatch, expired(), args)
    assert [item["code"] for item in json.loads(kept.output)["diagnostics"]][0] == "SST-APL011"
    broke = invoke_with_port(monkeypatch, expired(), [*args, "--break-stale-lock"])
    codes = [item["code"] for item in json.loads(broke.output)["diagnostics"]]
    assert codes.count("SST-APL010") == 1 and "SST-APL011" not in codes


def test_eval_reports_every_attempt_in_human_output(
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
    attempts = tuple(dataclasses.replace(attempt, run_name=f"EVAL_RUN_R{n}", attempt=n) for n in range(1, 6))
    # The fixture eval's `baseline_runs: 5` needs five completed attempts to capture.
    suite = EvalSuiteResult((EvalRunResult(eval_item.artifact_key, attempts, DiagnosticBag(), True),), DiagnosticBag())
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
            item.resolved.options.agent_version or "",
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
            "eval",
            *common(project),
            "--target",
            "dev",
            "--capture-baseline",
            "--reason",
            "initial",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (
        "eval:jaffle_analytics_agent  attempt 2  COMPLETED  1/1 question passed\n├── run  EVAL_RUN_R2\n"
        in result.output
    )
    assert "├── answer_correctness  1/1 passed  average 0.900\n" in result.output
    # Each count names whose it is: the agent's time and tokens, the judge's tokens.
    assert "├── cost  0.1s agent time · agent 10 in / 5 out · 1 LLM call · judge 8 tokens\n" in result.output
    assert "└── the question passed  (-v shows it)" in result.output


def test_eval_preflight_failure_keeps_json_schema_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    result = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["eval", *common(project), "--target", "dev", "--output", "json"],
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
        "unmeasured": [],
        "credits": None,
        "credit_attribution": "not_requested",
    }
    assert payload["data"]["regression_count"] == 0
    assert payload["data"]["gate_verdict"] == "not_evaluated"


def test_eval_baseline_capture_requires_reason(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(
        cli,
        ["eval", *common(project), "--capture-baseline", "--output", "json"],
    )
    assert result.exit_code == 3
    assert "requires --reason" in json.loads(result.output)["data"]["error"]


def test_eval_closes_its_connection_when_it_fails_before_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    evals = ["eval", *common(project), "--target", "dev", "--output", "json"]
    StateFileStore(project / "sst_target" / "state.dev.json").acquire_lock("apply-run", break_stale=False)
    locked, closes = invoke_counting_closes(monkeypatch, FakeSnowflake(state={}), evals)
    # Refused by the lock, the run exits 1 as `sst apply` does, and names the holder.
    envelope = json.loads(locked.output)
    assert locked.exit_code == 1 and "apply-run holds the target lock" in envelope["data"]["error"]
    assert [item["code"] for item in envelope["diagnostics"]] == ["SST-APL011"]
    assert closes == ["closed"]

    def unwritable(self: StateFileStore, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        raise PermissionError("sst_target is read-only")

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.project.StateFileStore.acquire_lock", unwritable)
    failed, closes = invoke_counting_closes(monkeypatch, FakeSnowflake(state={}), evals)
    assert failed.exit_code == 4 and json.loads(failed.output)["data"]["error"] == "sst_target is read-only"
    assert closes == ["closed"]


@pytest.mark.parametrize(
    ("state_table", "expected"),
    [
        ("DB.SCH.SST_STATE", "SST_STATE_EVALS"),
        ('DB.SCH."sst_State"', '"sst_State_EVALS"'),
        ('DB.SCH."sst state"', '"sst state_EVALS"'),
    ],
)
def test_the_eval_state_table_keeps_a_quoted_state_tables_exact_name(state_table: str, expected: str) -> None:
    from snowflake_semantic_tools.cli.commands.eval import _eval_state_table

    source = QualifiedName.parse(state_table)
    table = _eval_state_table(source)
    assert (table.database, table.schema, table.name.sql) == (source.database, source.schema, expected)


@pytest.mark.parametrize("output", ["json", "table"])
def test_a_gate_that_needs_a_new_baseline_names_the_capture_as_its_next_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: str
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    refused = EvalGateOutcome(
        DiagnosticBag((D("SST-VAL758", artifact="eval:jaffle_analytics_agent"),)),
        EvalSuiteResult((EvalRunResult("eval:jaffle_analytics_agent", (), DiagnosticBag()),), DiagnosticBag()),
        False,
        {"gate_verdict": "no_signal", "gate_reasons": ["baseline_absent"]},
        ("eval:jaffle_analytics_agent",),
    )
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.eval.RunEvalGate.run", lambda *args, **kwargs: refused)
    args = ["eval", *common(project), "--target", "dev", "--output", output]

    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)

    assert result.exit_code == 1
    step = (
        "once the change is intended, `sst eval --select eval:jaffle_analytics_agent "
        '--capture-baseline --reason "<why>"` captures new baselines; it writes what later gates compare '
        "with, so run it only when you mean to"
    )
    if output == "json":
        assert json.loads(result.stdout)["next_steps"] == [step]
    else:
        assert f"next: {step}" in result.stderr


@pytest.mark.parametrize(
    ("data", "headline"),
    [
        ({"gate_verdict": "passed", "regression_count": 0}, "eval gate passed: 1 eval, no regressions"),
        ({"gate_verdict": "regressed", "regression_count": 2}, "eval gate passed: 1 eval, 2 report-tier regressions"),
        ({"gate_verdict": "captured", "captured_baselines": ["eval:jaffle_analytics_agent"]}, "captured 1 baseline"),
    ],
)
def test_an_eval_run_that_passed_closes_with_the_gates_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: dict[str, object], headline: str
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    ran = EvalSuiteResult((EvalRunResult("eval:jaffle_analytics_agent", (), DiagnosticBag(), True),), DiagnosticBag())
    evals = [{"eval_key": "eval:jaffle_analytics_agent", "attempts": []}]
    passed = EvalGateOutcome(DiagnosticBag(), ran, True, {"evals": evals, **data})
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.eval.RunEvalGate.run", lambda *args, **kwargs: passed)

    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["eval", *common(project), "--target", "dev"])

    assert result.exit_code == 0, result.output
    assert f"✓ {headline} · " in result.stderr


def test_the_lock_and_baseline_options_are_listed_in_help() -> None:
    shown = CliRunner().invoke(cli, ["eval", "--help"])
    for option in ("--capture-baseline", "--reason", "--break-stale-lock", "--unpublished", "--compare-models"):
        assert option in shown.output
    for command in ("apply", "drop"):
        assert "--break-stale-lock" in CliRunner().invoke(cli, [command, "--help"]).output


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--reason", "why"], "--reason requires --capture-baseline"),
        (["--unpublished", "--capture-baseline", "--reason", "why"], "--unpublished never captures a baseline"),
        (["--compare-runs", "2"], "add --compare-models"),
        (["--compare-models", "--capture-baseline", "--reason", "why"], "a comparison gates nothing"),
        (["--compare-models", "--unpublished"], "drop --unpublished"),
        (["--compare-models", "--refresh-cost", "compare.json"], "drop --compare-models"),
    ],
)
def test_flags_that_never_run_together_are_refused(tmp_path: Path, args: list[str], message: str) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["eval", *common(project), *args, "--output", "json"])
    assert result.exit_code == 3, result.output
    payload = json.loads(result.output)
    assert message in payload["data"]["error"]
    assert [item["code"] for item in payload["diagnostics"]] == ["SST-PRT100"]
