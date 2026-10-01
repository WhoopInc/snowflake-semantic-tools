from __future__ import annotations

import json
from collections import deque
from dataclasses import replace
from hashlib import md5
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.evals.retrieve import (
    _expected_question_map,
    _metric_passed,
    _nonnegative_int,
    _optional_float,
    _question_identity,
    _result_question_key,
    _row_cost,
    _single_row,
    _status_details,
    _variant,
)
from snowflake_semantic_tools.app.evals.run import (
    EvalRunOptions,
    EvalRunResult,
    EvalSuiteResult,
    RunEvalSuite,
    _compact_timestamp,
    _suite_concurrency,
    empty_eval_suite_json,
    eval_suite_json,
    validate_eval_publication,
)
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT, EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EvalCostSummary,
    EvalDefaults,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError, StagedFileMetadata
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, State

from .conftest import FixedClock, InMemorySnowflake
from .helpers import target
from .test_eval_compile import compile_eval

STATUS_COLUMNS = ("RUN_NAME", "AGENT_NAME", "AGENT_TYPE", "STATUS", "STATUS_DETAILS")
RESULT_COLUMNS = (
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


class EvalSnowflake(InMemorySnowflake):
    def __init__(self, results: list[QueryResult | Exception]) -> None:
        super().__init__()
        self.results = deque(results)
        self.start_results: deque[ExecResult] = deque()

    def execute_script(self, statements) -> ExecResult:
        self.scripts.append(tuple(statements))
        return self.start_results.popleft() if self.start_results else ExecResult(True)

    def query(self, sql: str, params: object = None) -> QueryResult:
        self.queries.append((sql, params))
        if not self.results:
            raise AssertionError(f"unexpected query: {sql}")
        result = self.results.popleft()
        if isinstance(result, Exception):
            raise result
        return result

    def query_in_context(self, scope, sql: str, params: object = None) -> QueryResult:
        del scope
        return self.query(sql, params)


class ResolveErrorSnowflake(EvalSnowflake):
    def resolve_agent_version(self, qualified_name, selector: str) -> str:
        del qualified_name, selector
        raise SnowflakePortError("version lookup failed")


class ConfigErrorSnowflake(EvalSnowflake):
    def observe_staged_file(self, stage_path: str):
        del stage_path
        raise SnowflakePortError("config observation failed")


class ConfigReadbackSnowflake(EvalSnowflake):
    def __init__(
        self,
        observations: list[StagedFileMetadata | None],
        *,
        staged_content: bytes | None = None,
    ) -> None:
        super().__init__([])
        self.observations = deque(observations)
        self.staged_content = staged_content
        self.upload_content: bytes | None = None

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        del stage_path
        if not self.observations:
            raise AssertionError("unexpected staged-file observation")
        return self.observations.popleft()

    def upload(self, stage_path: str, content: bytes) -> None:
        self.uploads.append((stage_path, content))
        self.upload_content = content

    def read_staged_file(self, stage_path: str) -> bytes | None:
        del stage_path
        return self.upload_content if self.upload_content is not None else self.staged_content


def status_result(status: str, run_name: str = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z") -> QueryResult:
    return QueryResult(STATUS_COLUMNS, ((run_name, "SALES_AGENT", "CORTEX AGENT", status, []),))


def result_row(
    metric: str = "answer_correctness",
    score: object = 0.9,
    *,
    record_id: str = "record",
    request_id: str = "request",
    input_query: str = "Question",
    ground_truth: object | None = None,
    metric_type: str = "system",
    metric_status: object = None,
    error: object = None,
    calls: object = None,
) -> QueryResult:
    if ground_truth is None:
        ground_truth = {"ground_truth_invocations": [], "ground_truth_output": "Answer"}
    if metric_status is None:
        metric_status = {"status": 200, "message": "ok"}
    if calls is None:
        calls = [
            {
                "criteria": "correct",
                "full_metadata": {
                    "prompt_tokens": 5,
                    "completion_tokens": 3,
                    "total_tokens": 8,
                },
            }
        ]
    return QueryResult(
        RESULT_COLUMNS,
        (
            (
                record_id,
                "question-1",
                request_id,
                "2026-01-01T00:00:00Z",
                123,
                input_query,
                "Answer",
                error,
                json.dumps(ground_truth) if not isinstance(ground_truth, str) else ground_truth,
                metric,
                score,
                metric_type,
                metric_status,
                calls,
                11,
                7,
                2,
            ),
        ),
    )


def result_rows() -> QueryResult:
    system = result_row().rows[0]
    custom = list(system)
    custom[9] = "grounding"
    custom[10] = 4.0
    custom[11] = "custom"
    return QueryResult(RESULT_COLUMNS, (system, tuple(custom)))


def runner(port: EvalSnowflake):
    compiled = compile_eval().compiled[0]
    return RunEvalSuite(port, FixedClock()), compiled


def run_once(port: EvalSnowflake, compiled=None):
    use_case, default_compiled = runner(port)
    return use_case.run(
        (compiled or default_compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )


def retrieval_error(rows: tuple[tuple[object, ...], ...], compiled=None) -> str:
    result = run_once(
        EvalSnowflake([status_result("COMPLETED"), QueryResult(RESULT_COLUMNS, rows)]),
        compiled,
    )
    error = result.evals[0].attempts[0].retrieval_error
    assert error is not None
    return error


def test_eval_runner_polls_retrieves_scores_and_costs() -> None:
    port = EvalSnowflake([status_result("CREATED"), status_result("COMPLETED"), result_rows()])
    use_case, compiled = runner(port)

    result = use_case.run(
        (compiled,),
        options=EvalRunOptions("abcdef012345", timestamp="20260928T010203Z", poll_interval_ms=25),
    )

    assert result.success
    attempt = result.evals[0].attempts[0]
    assert attempt.run_name == "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    assert attempt.terminal_status == "COMPLETED"
    assert attempt.rows[0].metrics[0].passed
    assert attempt.cost.duration_ms == 123
    assert attempt.cost.total_tokens == 16
    assert attempt.cost.total_input_tokens == 11
    assert attempt.cost.total_output_tokens == 7
    assert attempt.cost.llm_call_count == 2
    assert len(port.uploads) == 1
    assert port.scripts[0][0] == "USE DATABASE DB"
    assert port.scripts[0][1] == "USE SCHEMA DB.S"
    assert "EXECUTE_AI_EVALUATION('START'" in port.scripts[0][2]
    assert port.queries[0][1] == (
        attempt.run_name,
        "@DB.S.EVAL_CONFIGS/sales_agent/" + dict(compiled.rendered_artifact.component_fingerprints)["config"] + ".yaml",
    )


def test_eval_runner_reports_every_retry_and_partial_status() -> None:
    compiled = compile_eval().compiled[0]
    run = compiled.resolved.config.run
    assert run is not None
    compiled = replace(
        compiled,
        resolved=replace(compiled.resolved, config=replace(compiled.resolved.config, run=replace(run, retry=1))),
    )
    port = EvalSnowflake(
        [
            status_result("PARTIALLY_COMPLETED"),
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R2"),
            result_rows(),
        ]
    )

    result = RunEvalSuite(port, FixedClock()).run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    assert result.success
    attempts = result.evals[0].attempts
    assert [attempt.terminal_status for attempt in attempts] == ["PARTIALLY_COMPLETED", "COMPLETED"]
    assert attempts[1].run_name.endswith("_R2")
    assert result.diagnostics[0].code == "SST-APL024"
    assert result.evals[0].accepted


def test_eval_runner_stops_after_cancellation_and_preserves_details() -> None:
    port = EvalSnowflake(
        [
            QueryResult(
                STATUS_COLUMNS,
                (
                    (
                        "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z",
                        "SALES_AGENT",
                        "CORTEX AGENT",
                        "CANCELLED",
                        ["user cancelled"],
                    ),
                ),
            )
        ]
    )
    use_case, compiled = runner(port)
    run = compiled.resolved.config.run
    assert run is not None
    compiled = replace(
        compiled,
        resolved=replace(compiled.resolved, config=replace(compiled.resolved.config, run=replace(run, retry=2))),
    )

    result = use_case.run((compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"))

    assert not result.success
    assert len(result.evals[0].attempts) == 1
    assert result.evals[0].attempts[0].status_details == ("user cancelled",)


def test_eval_runner_runs_and_retains_every_configured_attempt() -> None:
    port = EvalSnowflake(
        [
            status_result("COMPLETED"),
            result_rows(),
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R2"),
            result_rows(),
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R3"),
            result_rows(),
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R4"),
            result_rows(),
        ]
    )
    use_case, compiled = runner(port)
    run = compiled.resolved.config.run
    assert run is not None
    compiled = replace(
        compiled,
        resolved=replace(compiled.resolved, config=replace(compiled.resolved.config, run=replace(run, retry=3))),
    )

    result = use_case.run((compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"))

    assert result.success
    assert len(result.evals[0].attempts) == 4
    assert not port.results


def test_baseline_capture_runs_until_configured_completed_attempt_count() -> None:
    compiled = compile_eval().compiled[0]
    run = compiled.resolved.config.run
    assert run is not None
    compiled = replace(
        compiled,
        resolved=replace(
            compiled.resolved,
            config=replace(compiled.resolved.config, run=replace(run, retry=1, baseline_runs=3)),
        ),
    )
    port = EvalSnowflake(
        [
            status_result("COMPLETED"),
            result_rows(),
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R2"),
            result_rows(),
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R3"),
            result_rows(),
        ]
    )

    result = RunEvalSuite(port, FixedClock()).run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
        baseline_capture=True,
    )

    assert result.success
    assert len(result.evals[0].attempts) == 3


def test_eval_runner_distinguishes_start_status_and_retrieval_failures() -> None:
    start_port = EvalSnowflake([])
    start_port.start_results.append(ExecResult(False, error=ExecutionError("cannot start")))
    start_runner, compiled = runner(start_port)
    start = start_runner.run((compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"))
    assert start.evals[0].attempts[0].terminal_status == "START_FAILED"
    assert start.diagnostics[0].code == "SST-APL023"

    retrieve_port = EvalSnowflake([status_result("COMPLETED"), QueryResult(("INPUT_ID",), (("q",),))])
    retrieve_runner, compiled = runner(retrieve_port)
    retrieve = retrieve_runner.run((compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"))
    assert retrieve.evals[0].attempts[0].terminal_status == "COMPLETED"
    assert retrieve.evals[0].attempts[0].retrieval_error
    assert retrieve.diagnostics[0].code == "SST-SNO001"


def test_eval_runner_rejects_unknown_status_and_times_out() -> None:
    port = EvalSnowflake([status_result("UNKNOWN")])
    use_case, compiled = runner(port)

    result = use_case.run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z", max_polls=1),
    )

    assert result.evals[0].attempts[0].terminal_status == "STATUS_FAILED"
    assert "did not reach" in result.evals[0].attempts[0].retrieval_error


def test_eval_cost_deduplicates_agent_usage_across_metric_rows() -> None:
    first, second = result_rows().rows
    port = EvalSnowflake(
        [
            status_result("COMPLETED"),
            QueryResult(RESULT_COLUMNS, (first, second)),
        ]
    )
    use_case, compiled = runner(port)

    result = use_case.run((compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"))

    cost = result.evals[0].attempts[0].cost
    assert cost.duration_ms == 123
    assert cost.total_input_tokens == 11
    assert cost.total_output_tokens == 7
    assert cost.llm_call_count == 2
    assert cost.total_tokens == 16
    payload = eval_suite_json(result)
    assert payload["attempt_count"] == 1
    assert payload["regression_count"] == 0
    assert payload["gate_verdict"] == "not_evaluated"
    assert payload["cost_totals"]["total_tokens"] == 16  # type: ignore[index]


def test_question_key_is_stable_across_snowflake_input_ids() -> None:
    first = result_rows().rows
    second = []
    for row in first:
        changed = list(row)
        changed[1] = "snowflake-id-from-another-run"
        second.append(tuple(changed))
    first_port = EvalSnowflake([status_result("COMPLETED"), QueryResult(RESULT_COLUMNS, first)])
    second_port = EvalSnowflake(
        [
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010204Z"),
            QueryResult(RESULT_COLUMNS, tuple(second)),
        ]
    )
    first_runner, compiled = runner(first_port)
    second_runner, _ = runner(second_port)

    first_result = first_runner.run((compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"))
    second_result = second_runner.run((compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010204Z"))

    assert (
        first_result.evals[0].attempts[0].rows[0].question_key
        == second_result.evals[0].attempts[0].rows[0].question_key
    )


def test_result_question_key_accepts_exact_flattened_output_and_rejects_drift() -> None:
    compiled = compile_eval().compiled[0]
    ground_truth = {"ground_truth_invocations": [], "ground_truth_output": "Answer"}
    expected = {"Question": (_question_identity("Question", ground_truth), ground_truth)}

    assert _result_question_key({"INPUT": "Question", "GROUND_TRUTH": "Answer"}, expected) in {
        key for key, _ in _expected_question_map(compiled).values()
    }
    with pytest.raises(ValueError, match="unrecognized flattened ground truth"):
        _result_question_key({"INPUT": "Question", "GROUND_TRUTH": "Different"}, expected)


def test_eval_publication_preflight_refuses_unapplied_state() -> None:
    compiled = compile_eval().compiled[0]
    manifest = build_manifest(compile_eval())
    state = State(STATE_SCHEMA_VERSION, target(), manifest.manifest_id, "cfg", None, {})

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(InMemorySnowflake()),
    )

    assert diagnostics[0].code == "SST-APL012"


def test_eval_runner_rejects_duplicate_metric_rows_and_nonfinite_scores() -> None:
    row = result_row().rows[0]
    duplicate_port = EvalSnowflake([status_result("COMPLETED"), QueryResult(RESULT_COLUMNS, (row, row))])
    duplicate_runner, compiled = runner(duplicate_port)

    duplicate = duplicate_runner.run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    assert duplicate.evals[0].attempts[0].retrieval_error
    assert "duplicate metric row" in duplicate.evals[0].attempts[0].retrieval_error

    nonfinite = list(row)
    nonfinite[10] = float("nan")
    nonfinite_port = EvalSnowflake([status_result("COMPLETED"), QueryResult(RESULT_COLUMNS, (tuple(nonfinite),))])
    nonfinite_runner, _ = runner(nonfinite_port)

    invalid = nonfinite_runner.run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    assert "not finite" in invalid.evals[0].attempts[0].retrieval_error


def test_eval_runner_rejects_status_for_a_different_run() -> None:
    port = EvalSnowflake([status_result("COMPLETED", "some-other-run")])
    use_case, compiled = runner(port)

    result = use_case.run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    assert result.evals[0].attempts[0].terminal_status == "STATUS_FAILED"
    assert "some-other-run" in result.evals[0].attempts[0].retrieval_error


def test_custom_metric_threshold_default_controls_pass_vector() -> None:
    compiled = compile_eval().compiled[0]
    custom = replace(compiled.resolved.custom_metrics[0], threshold_default=ThresholdRange(min=4.5))
    compiled = replace(compiled, resolved=replace(compiled.resolved, custom_metrics=(custom,)))
    rows = result_rows().rows
    port = EvalSnowflake([status_result("COMPLETED"), QueryResult(RESULT_COLUMNS, rows)])

    result = RunEvalSuite(port, FixedClock()).run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    assert result.success
    metrics = {metric.metric_name: metric for metric in result.evals[0].attempts[0].rows[0].metrics}
    assert not metrics["grounding"].passed


def test_eval_runner_reports_missing_run_version_and_config_failures() -> None:
    compiled = compile_eval().compiled[0]
    without_run = replace(
        compiled,
        resolved=replace(compiled.resolved, config=replace(compiled.resolved.config, run=None)),
    )

    missing_run = run_once(EvalSnowflake([]), without_run)
    version_failure = run_once(ResolveErrorSnowflake([]), compiled)
    config_failure = run_once(ConfigErrorSnowflake([]), compiled)

    assert "run configuration is absent" in missing_run.diagnostics[0].message
    assert "version lookup failed" in version_failure.diagnostics[0].message
    assert "config observation failed" in config_failure.diagnostics[0].message
    assert not missing_run.evals[0].attempts
    assert not version_failure.evals[0].attempts
    assert not config_failure.evals[0].attempts


def test_eval_runner_fail_fast_stops_before_the_next_eval(monkeypatch) -> None:
    compiled = compile_eval().compiled[0]
    use_case = RunEvalSuite(EvalSnowflake([]), FixedClock())
    calls = []

    def fail(*args):
        calls.append(args[0].artifact_key)
        return EvalRunResult(args[0].artifact_key, (), DiagnosticBag(), False)

    monkeypatch.setattr(use_case, "_run_eval", fail)

    result = use_case.run(
        (compiled, compiled),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
        fail_fast=True,
    )

    assert calls == [compiled.artifact_key]
    assert len(result.evals) == 1
    assert not result.success


def test_eval_runner_parallelizes_with_the_requested_concurrency(monkeypatch) -> None:
    first = compile_eval().compiled[0]
    first_run = first.resolved.config.run
    assert first_run is not None
    second = replace(
        first,
        resolved=replace(
            first.resolved,
            config=replace(first.resolved.config, run=replace(first_run, concurrency=3)),
        ),
    )
    use_case = RunEvalSuite(EvalSnowflake([]), FixedClock())

    def succeed(*args):
        return EvalRunResult(args[0].artifact_key, (), DiagnosticBag(), True)

    monkeypatch.setattr(use_case, "_run_eval", succeed)

    result = use_case.run(
        (first, second),
        defaults=EvalDefaults(),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    assert len(result.evals) == 2
    assert result.success


@pytest.mark.parametrize(
    ("initial", "trusted_digest", "expected_uploads"),
    (
        (StagedFileMetadata("@config", "config", 6, "stale"), None, [("@config", b"config")]),
        (
            StagedFileMetadata("@config", "config", 5, "stale"),
            md5(b"config", usedforsecurity=False).hexdigest(),
            [],
        ),
        (
            StagedFileMetadata("@config", "config", 6, "stale"),
            md5(b"config", usedforsecurity=False).hexdigest(),
            [],
        ),
    ),
)
def test_ensure_config_uses_readback_bytes_not_encrypted_list_metadata(
    initial,
    trusted_digest,
    expected_uploads,
) -> None:
    content = b"config"
    digest = trusted_digest or "repaired"
    repaired = StagedFileMetadata("@config", "config", len(content), digest)
    port = ConfigReadbackSnowflake([initial, repaired], staged_content=content)

    RunEvalSuite(port, FixedClock())._ensure_config("@config", content, trusted_digest)

    assert port.uploads == expected_uploads


def test_ensure_config_trusts_matching_metadata_and_verifies_bytes_without_upload() -> None:
    content = b"config"
    observed = StagedFileMetadata("@config", "config", len(content), "digest")
    port = ConfigReadbackSnowflake([observed], staged_content=content)

    RunEvalSuite(port, FixedClock())._ensure_config(
        "@config",
        content,
        md5(content, usedforsecurity=False).hexdigest(),
    )

    assert not port.uploads


@pytest.mark.parametrize(
    ("observations", "trusted_digest", "staged_content", "message"),
    (
        ([None, None], None, b"config", "is absent"),
        (
            [
                StagedFileMetadata("@config", "config", 6, "actual"),
                StagedFileMetadata("@config", "config", 6, "actual"),
            ],
            "expected",
            b"config",
            "has digest 2245023265ae4cf87d02c8b6ba991139, expected expected",
        ),
        (
            [
                StagedFileMetadata("@config", "config", 6, "digest"),
                StagedFileMetadata("@config", "config", 6, "digest"),
            ],
            "digest",
            b"broken",
            "has digest 2245023265ae4cf87d02c8b6ba991139, expected digest",
        ),
    ),
)
def test_ensure_config_rejects_failed_repairs_and_readback_mismatches(
    observations,
    trusted_digest,
    staged_content,
    message,
) -> None:
    port = ConfigReadbackSnowflake(observations, staged_content=staged_content)

    with pytest.raises(SnowflakePortError, match=message):
        RunEvalSuite(port, FixedClock())._ensure_config("@config", b"config", trusted_digest)


def test_eval_runner_reports_start_failure_without_an_error_payload() -> None:
    port = EvalSnowflake([])
    port.start_results.append(ExecResult(False))

    result = run_once(port)

    assert result.evals[0].attempts[0].terminal_status == "START_FAILED"
    assert "returned no result" in result.evals[0].attempts[0].retrieval_error


@pytest.mark.parametrize(
    ("agent_name", "agent_type", "message"),
    (
        ("OTHER_AGENT", "CORTEX AGENT", "different agent"),
        ("SALES_AGENT", "OTHER", "different agent type"),
    ),
)
def test_eval_runner_rejects_status_for_a_different_agent_identity(agent_name, agent_type, message) -> None:
    run_name = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    status = QueryResult(STATUS_COLUMNS, ((run_name, agent_name, agent_type, "COMPLETED", []),))

    result = run_once(EvalSnowflake([status]))

    assert result.evals[0].attempts[0].terminal_status == "STATUS_FAILED"
    assert message in result.evals[0].attempts[0].retrieval_error


@pytest.mark.parametrize("rows", ((), (("run", "agent", "type", "status", []),) * 2))
def test_single_row_rejects_missing_and_duplicate_status_rows(rows) -> None:
    with pytest.raises(SnowflakePortError, match="expected 1"):
        _single_row(STATUS_COLUMNS, rows, STATUS_COLUMNS, "evaluation status")


def test_eval_runner_rejects_blank_required_status_values() -> None:
    run_name = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    status = QueryResult(STATUS_COLUMNS, ((run_name, " ", "CORTEX AGENT", "COMPLETED", []),))

    result = run_once(EvalSnowflake([status]))

    assert "omitted AGENT_NAME" in result.evals[0].attempts[0].retrieval_error


def test_eval_runner_normalizes_null_and_json_status_details() -> None:
    run_name = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    null_details = QueryResult(STATUS_COLUMNS, ((run_name, "SALES_AGENT", "CORTEX AGENT", "CANCELLED", None),))
    json_details = QueryResult(
        STATUS_COLUMNS,
        ((run_name, "SALES_AGENT", "CORTEX AGENT", "CANCELLED", '["cancelled"]'),),
    )

    null_result = run_once(EvalSnowflake([null_details]))
    json_result = run_once(EvalSnowflake([json_details]))

    assert null_result.evals[0].attempts[0].status_details == ()
    assert json_result.evals[0].attempts[0].status_details == ("cancelled",)


def test_eval_runner_rejects_non_array_status_details() -> None:
    run_name = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    status = QueryResult(STATUS_COLUMNS, ((run_name, "SALES_AGENT", "CORTEX AGENT", "CANCELLED", {}),))

    result = run_once(EvalSnowflake([status]))

    assert "STATUS_DETAILS must be an array" in result.evals[0].attempts[0].retrieval_error


def test_eval_runner_rejects_empty_result_sets() -> None:
    error = retrieval_error(())

    assert "returned no result rows" in error


def test_eval_runner_rejects_record_and_question_identity_conflicts() -> None:
    first = result_row(input_query="Question", record_id="record-1").rows[0]
    other_question = result_row(input_query="Other", record_id="record-1").rows[0]
    other_record = result_row(input_query="Question", record_id="record-2").rows[0]

    assert "unexpected input 'Other'" in retrieval_error((first, other_question))
    assert "maps to multiple records" in retrieval_error((first, other_record))


@pytest.mark.parametrize(
    ("row", "message"),
    (
        (result_row(metric="unknown").rows[0], "unknown metric"),
        (result_row(metric_type="custom").rows[0], "has type 'custom', expected 'system'"),
    ),
)
def test_eval_runner_rejects_unknown_metrics_and_wrong_metric_types(row, message) -> None:
    assert message in retrieval_error((row,))


def test_eval_runner_rejects_conflicting_inputs_for_one_record() -> None:
    class ChangingText:
        def __init__(self, *values: str) -> None:
            self.values = deque(values)

        def __str__(self) -> str:
            return self.values.popleft()

    system = list(result_row().rows[0])
    custom = list(result_row(metric="grounding", metric_type="custom").rows[0])
    system[5] = ChangingText("Question", "first input")
    custom[5] = ChangingText("Question", "second input")

    assert "contains conflicting inputs" in retrieval_error((tuple(system), tuple(custom)))


def test_eval_runner_rejects_question_and_metric_set_mismatches() -> None:
    changed_rows = []
    for row in result_rows().rows:
        changed = list(row)
        changed[5] = "Unexpected question"
        changed_rows.append(tuple(changed))

    assert "unexpected input 'Unexpected question'" in retrieval_error(tuple(changed_rows))
    assert "metric set differs" in retrieval_error((result_row().rows[0],))


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (json.dumps({"input_query": "Question"}), "must be an array"),
        (json.dumps(["Question"]), "invalid question row"),
        (json.dumps([{"input_query": "Question", "ground_truth": []}]), "invalid ground truth"),
        (
            json.dumps(
                [
                    {"input_query": "Question", "ground_truth": {}},
                    {"input_query": "Question", "ground_truth": {}},
                ]
            ),
            "duplicate question inputs",
        ),
    ),
)
def test_expected_question_map_rejects_malformed_compiled_payloads(payload, message) -> None:
    compiled = compile_eval().compiled[0]
    compiled = replace(compiled, rendered=replace(compiled.rendered, dataset_payload=payload))

    with pytest.raises(ValueError, match=message):
        _expected_question_map(compiled)


def test_metric_passed_fails_closed_for_errors_status_codes_and_missing_scores() -> None:
    compiled = compile_eval().compiled[0]

    assert not _metric_passed(compiled, "answer_correctness", 0.9, {"status": 200}, "failed")
    assert not _metric_passed(compiled, "answer_correctness", 0.9, {"status": 500}, None)
    assert not _metric_passed(compiled, "answer_correctness", 0.9, {"status": "bad"}, None)
    assert not _metric_passed(compiled, "answer_correctness", None, None, None)


def test_metric_passed_applies_bounded_system_thresholds() -> None:
    compiled = compile_eval().compiled[0]
    system_metric = replace(compiled.resolved.config.system_metrics[0], threshold=ThresholdRange(min=0.5, max=1.0))
    compiled = replace(
        compiled,
        resolved=replace(
            compiled.resolved,
            config=replace(compiled.resolved.config, system_metrics=(system_metric,)),
        ),
    )

    assert _metric_passed(compiled, "answer_correctness", 0.5, None, None)
    assert not _metric_passed(compiled, "answer_correctness", 1.1, None, None)
    assert _metric_passed(compiled, "unknown", 0.1, None, None)


def test_row_cost_normalizes_json_calls_and_skips_malformed_metadata() -> None:
    calls = json.dumps(
        [
            None,
            {"full_metadata": "not-json"},
            {"full_metadata": json.dumps({"prompt_tokens": "2", "completion_tokens": 3, "total_tokens": 5})},
        ]
    )

    cost = _row_cost(
        {
            "DURATION_MS": "9",
            "METRIC_CALLS": calls,
            "TOTAL_INPUT_TOKENS": None,
            "TOTAL_OUTPUT_TOKENS": 4,
            "LLM_CALL_COUNT": 1,
        }
    )

    assert cost == EvalCostSummary(9, 2, 3, 5, 0, 4, 1)
    assert _row_cost({"METRIC_CALLS": {}}) == EvalCostSummary()


def test_variant_returns_malformed_json_unchanged() -> None:
    assert _variant("not-json") == "not-json"


@pytest.mark.parametrize(
    ("value", "expected"),
    ((None, 0), ("2", 2), (2.9, 2)),
)
def test_nonnegative_int_normalizes_supported_values(value, expected) -> None:
    assert _nonnegative_int(value) == expected


@pytest.mark.parametrize("value", (True, object(), "not-an-int", -1))
def test_nonnegative_int_rejects_invalid_values(value) -> None:
    with pytest.raises(ValueError, match="evaluation cost value"):
        _nonnegative_int(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    ((None, None), ("1.25", 1.25)),
)
def test_optional_float_normalizes_supported_values(value, expected) -> None:
    assert _optional_float(value) == expected


@pytest.mark.parametrize("value", (True, object(), "not-a-number"))
def test_optional_float_rejects_invalid_values(value) -> None:
    with pytest.raises(ValueError, match="evaluation score"):
        _optional_float(value)


def test_status_details_helper_normalizes_and_rejects_values() -> None:
    assert _status_details(None) == ()
    assert _status_details('["one", 2]') == ("one", "2")
    with pytest.raises(ValueError, match="must be an array"):
        _status_details({})


def test_compact_timestamp_uses_utc_compact_format() -> None:
    value = _compact_timestamp()

    assert len(value) == 16
    assert value.endswith("Z")


def test_eval_publication_preflight_refuses_manifest_mismatch() -> None:
    compiled = compile_eval().compiled[0]
    manifest = build_manifest(compile_eval())
    artifact = compiled.rendered_for_publish(manifest.manifest_id)
    entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        "different",
        component_fingerprints=artifact.component_fingerprints,
        physical_resources=tuple((kind, name.sql) for kind, name in artifact.physical_resources),
    )
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        "",
        "cfg",
        None,
        MappingProxyType({compiled.artifact_key: entry}),
    )

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(InMemorySnowflake()),
    )

    assert diagnostics[0].code == "SST-APL012"
    assert compiled.artifact_key in diagnostics[0].message


def test_eval_publication_preflight_allows_unrelated_rows_from_older_manifests() -> None:
    compiled = compile_eval().compiled[0]
    manifest = build_manifest(compile_eval())
    artifact = compiled.rendered_for_publish(manifest.manifest_id)
    content = artifact.ddl.encode("utf-8")
    stage_digest = md5(content, usedforsecurity=False).hexdigest()
    eval_entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        manifest.manifest_id,
        component_fingerprints=(*artifact.component_fingerprints, ("config_stage_md5", stage_digest)),
        physical_resources=tuple((kind, name.sql) for kind, name in artifact.physical_resources),
    )
    unrelated = replace(eval_entry, qualified_name="DB.S.OLD", manifest_id="older")
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        "",
        "cfg",
        None,
        MappingProxyType({compiled.artifact_key: eval_entry, "agent:old": unrelated}),
    )
    port = InMemorySnowflake()
    port.existing = {name.sql for _, name in artifact.physical_resources}
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    config_path = EvalLifecycleHandler(port).config_path(artifact)
    port.stage_files.add(config_path)
    port.staged_file_contents[config_path] = content

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(port),
    )

    assert not diagnostics


def test_eval_publication_preflight_accepts_a_live_noop() -> None:
    compiled = compile_eval().compiled[0]
    manifest = build_manifest(compile_eval())
    artifact = compiled.rendered_for_publish(manifest.manifest_id)
    content = artifact.ddl.encode("utf-8")
    stage_digest = md5(content, usedforsecurity=False).hexdigest()
    entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        manifest.manifest_id,
        component_fingerprints=(*artifact.component_fingerprints, ("config_stage_md5", stage_digest)),
        physical_resources=tuple((kind, name.sql) for kind, name in artifact.physical_resources),
    )
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({compiled.artifact_key: entry}),
    )
    port = InMemorySnowflake()
    port.existing = {name.sql for _, name in artifact.physical_resources}
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    config_path = EvalLifecycleHandler(port).config_path(artifact)
    port.stage_files.add(config_path)
    port.staged_file_sizes[config_path] = len(content)
    port.staged_file_md5s[config_path] = stage_digest
    port.staged_file_contents[config_path] = content

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(port),
    )

    assert not diagnostics


def test_eval_publication_preflight_reports_plan_diagnostics() -> None:
    compiled = compile_eval().compiled[0]
    manifest = build_manifest(compile_eval())
    artifact = compiled.rendered_for_publish(manifest.manifest_id)
    entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        manifest.manifest_id,
        component_fingerprints=artifact.component_fingerprints,
        physical_resources=tuple((kind, name.sql) for kind, name in artifact.physical_resources),
    )
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({compiled.artifact_key: entry}),
    )
    port = InMemorySnowflake()
    port.existing = {name.sql for _, name in artifact.physical_resources}
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = "TYPE='CSV' FIELD_DELIMITER=','"

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(port),
    )

    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-APL028"]


def test_eval_publication_preflight_refuses_a_live_non_noop_plan() -> None:
    compiled = compile_eval().compiled[0]
    manifest = build_manifest(compile_eval())
    artifact = compiled.rendered_for_publish(manifest.manifest_id)
    entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        manifest.manifest_id,
        component_fingerprints=artifact.component_fingerprints,
        physical_resources=tuple((kind, name.sql) for kind, name in artifact.physical_resources),
    )
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({compiled.artifact_key: entry}),
    )

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(InMemorySnowflake()),
    )

    assert diagnostics[0].code == "SST-APL012"


def test_eval_suite_json_normalizes_metrics_costs_and_empty_results() -> None:
    rows = (
        EvalResultRow(
            "question-1",
            "Question",
            (
                EvalMetricResult("question-1", "answer_correctness", 1.0, True),
                EvalMetricResult("question-1", "no_score", None, False),
            ),
        ),
        EvalResultRow(
            "question-2",
            "Other",
            (EvalMetricResult("question-2", "answer_correctness", 0.0, False),),
        ),
    )
    attempt = EvalRunAttempt("run", 1, "COMPLETED", rows, EvalCostSummary(total_tokens=8))
    suite = EvalSuiteResult((EvalRunResult("eval:sales_agent", (attempt,), DiagnosticBag(), True),), DiagnosticBag())

    payload = eval_suite_json(suite)

    summaries = {item["metric_name"]: item for item in payload["evals"][0]["attempts"][0]["metric_summaries"]}
    assert summaries["answer_correctness"] == {
        "metric_name": "answer_correctness",
        "record_count": 2,
        "passed_count": 1,
        "average_score": 0.5,
    }
    assert summaries["no_score"]["average_score"] is None
    assert payload["cost_totals"]["credits"] is None
    assert empty_eval_suite_json() == {
        "evals": [],
        "attempt_count": 0,
        "cost_totals": {
            "duration_ms": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "llm_call_count": 0,
            "credits": None,
            "credit_attribution": "not_requested",
        },
        "regression_count": 0,
        "gate_verdict": "not_evaluated",
    }


def test_suite_concurrency_prefers_the_project_setting_then_the_largest_request() -> None:
    first = compile_eval().compiled[0]
    run = first.resolved.config.run
    assert run is not None and run.concurrency is None
    requesting = replace(
        first,
        resolved=replace(first.resolved, config=replace(first.resolved.config, run=replace(run, concurrency=3))),
    )
    without_run = replace(first, resolved=replace(first.resolved, config=replace(first.resolved.config, run=None)))

    assert _suite_concurrency((first, requesting), EvalDefaults(concurrency=5)) == 5
    assert _suite_concurrency((first, requesting), EvalDefaults(concurrency=-2)) == 1
    assert _suite_concurrency((first, requesting, without_run), EvalDefaults()) == 3
    assert _suite_concurrency((first, without_run), EvalDefaults()) == 1


class FixedReadbackSnowflake(ConfigReadbackSnowflake):
    def __init__(self, readback: bytes | None) -> None:
        observed = StagedFileMetadata("@config", "config", 6, "digest")
        super().__init__([observed, observed])
        self.readback = readback

    def read_staged_file(self, stage_path: str) -> bytes | None:
        del stage_path
        return self.readback


@pytest.mark.parametrize(
    ("readback", "message"),
    (
        (None, "is unreadable"),
        (b"conf", "has 4 bytes, expected 6"),
        (b"CONFIG", "bytes do not match rendered config"),
    ),
)
def test_ensure_config_rejects_a_readback_that_is_missing_short_or_different(readback, message) -> None:
    port = FixedReadbackSnowflake(readback)

    with pytest.raises(SnowflakePortError, match=message):
        RunEvalSuite(port, FixedClock())._ensure_config("@config", b"config", None)

    assert port.uploads == [("@config", b"config")]


def test_result_question_key_rejects_a_missing_input_and_different_whole_ground_truth() -> None:
    ground_truth = {"ground_truth_invocations": [], "ground_truth_output": "Answer"}
    expected = {"Question": (_question_identity("Question", ground_truth), ground_truth)}

    with pytest.raises(ValueError, match="omitted INPUT"):
        _result_question_key({"INPUT": None, "GROUND_TRUTH": "Answer"}, expected)
    with pytest.raises(ValueError, match="returned different ground truth"):
        _result_question_key(
            {"INPUT": "Question", "GROUND_TRUTH": json.dumps({"ground_truth_output": "Other"})}, expected
        )
    assert _result_question_key({"INPUT": "Question", "GROUND_TRUTH": json.dumps(ground_truth)}, expected) == (
        expected["Question"][0]
    )


def test_eval_runner_rejects_a_record_answering_two_questions_and_an_unanswered_question() -> None:
    compiled = compile_eval().compiled[0]
    dataset = json.loads(compiled.rendered.dataset_payload)
    dataset.append(
        {"input_query": "Other", "ground_truth": {"ground_truth_invocations": [], "ground_truth_output": "Answer"}}
    )
    compiled = replace(compiled, rendered=replace(compiled.rendered, dataset_payload=json.dumps(dataset)))
    first = result_row(input_query="Question", record_id="record-1").rows[0]
    second = result_row(input_query="Other", record_id="record-1").rows[0]

    assert "maps to multiple questions" in retrieval_error((first, second), compiled)
    assert "question set differs from the authored dataset" in retrieval_error(result_rows().rows, compiled)
