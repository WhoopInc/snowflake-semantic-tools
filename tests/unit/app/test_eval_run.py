from __future__ import annotations

import json
from collections import deque
from dataclasses import replace
from hashlib import md5
from types import MappingProxyType
from typing import Any

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
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
    EvalRunResult,
    EvalSuiteResult,
    RunEvalSuite,
    _compact_timestamp,
    empty_eval_suite_json,
    eval_suite_json,
    retention_class,
    suite_concurrency,
    validate_eval_publication,
)
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT, EvalLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalCostSummary,
    EvalDefaults,
    EvalMetricResult,
    EvalResultRow,
    EvalRetention,
    EvalRunAttempt,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, State
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock, PollClock
from tests.helpers.eval_builders import (
    RESULT_COLUMNS,
    STATUS_COLUMNS,
    EvalSnowflake,
    compile_eval,
    compiled_eval_of,
    result_row,
    result_rows,
    seed_dataset_version,
    status_result,
)
from tests.helpers.snowflake_fake import FakeSnowflake, Sent


class ResolveErrorSnowflake(EvalSnowflake):
    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        del qualified_name, selector
        raise SnowflakePortError("version lookup failed")


class ConfigErrorSnowflake(EvalSnowflake):
    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
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
        self.log.append(Sent("upload", (stage_path,), content=content))
        self.upload_content = content

    def read_staged_file(self, stage_path: str) -> bytes | None:
        del stage_path
        return self.upload_content if self.upload_content is not None else self.staged_content


def runner(port: EvalSnowflake) -> tuple[RunEvalSuite, CompiledEval]:
    compiled = compiled_eval_of()
    return RunEvalSuite(port, FixedClock()), compiled


def run_once(port: EvalSnowflake, compiled: CompiledEval | None = None, **options: int) -> EvalSuiteResult:
    use_case, default_compiled = runner(port)
    return use_case.run(
        (compiled or default_compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z", **options),
    )


def retrieval_error(rows: tuple[tuple[object, ...], ...], compiled: CompiledEval | None = None) -> str:
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
    assert port.scripts[0][0] == "IN DB.S"
    assert "EXECUTE_AI_EVALUATION('START'" in port.scripts[0][1]
    assert port.queries[0][1] == (
        attempt.run_name,
        "@DB.S.EVAL_CONFIGS/sales_agent/" + dict(compiled.rendered_artifact.component_fingerprints)["config"] + ".yaml",
    )


def test_eval_runner_reports_every_retry_and_partial_status() -> None:
    compiled = compiled_eval_of()
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
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z", partial_settle_ms=0),
    )

    assert result.success
    attempts = result.evals[0].attempts
    assert [attempt.terminal_status for attempt in attempts] == ["PARTIALLY_COMPLETED", "COMPLETED"]
    assert attempts[1].run_name.endswith("_R2")
    [retried] = result.diagnostics
    assert (retried.code, retried.severity) == ("SST-APL029", Severity.WARNING)
    assert retried.message == (
        "eval 'eval:sales_agent': run 'EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z' ended PARTIALLY_COMPLETED"
        " and was retried: no status details"
    )
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


def with_run(compiled: CompiledEval, **changes: Any) -> CompiledEval:
    run = compiled.resolved.config.run
    assert run is not None
    config = replace(compiled.resolved.config, run=replace(run, **changes))
    return replace(compiled, resolved=replace(compiled.resolved, config=config))


def test_a_gate_run_stops_at_its_first_attempt_that_completed_and_was_read() -> None:
    port = EvalSnowflake([status_result("COMPLETED"), result_rows()])

    result = run_once(port, with_run(compiled_eval_of(), retry=3))

    assert result.success
    assert [attempt.run_name for attempt in result.evals[0].attempts] == [
        "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    ]
    assert len(port.scripts) == 1 and not port.query_results


def test_a_gate_run_retries_an_attempt_that_failed_and_reports_both() -> None:
    first = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    port = EvalSnowflake(
        [
            QueryResult(STATUS_COLUMNS, ((first, "SALES_AGENT", "CORTEX AGENT", "FAILED", ["Invocation failed"]),)),
            status_result("COMPLETED", f"{first}_R2"),
            result_rows(),
        ]
    )

    result = run_once(port, with_run(compiled_eval_of(), retry=3))

    attempts = result.evals[0].attempts
    assert result.evals[0].accepted and [(attempt.attempt, attempt.terminal_status) for attempt in attempts] == [
        (1, "FAILED"),
        (2, "COMPLETED"),
    ]
    # The failed attempt is reported, as a warning: the retry completed in its place.
    assert result.success and not result.diagnostics.has_errors
    assert [(item.code, item.message) for item in result.diagnostics] == [
        ("SST-APL029", f"eval 'eval:sales_agent': run '{first}' ended FAILED and was retried: Invocation failed")
    ]
    assert len(port.scripts) == 2 and not port.query_results


def test_a_config_accepting_a_partial_status_errs_even_when_a_retry_completed() -> None:
    port = EvalSnowflake(
        [
            status_result("PARTIALLY_COMPLETED"),
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R2"),
            result_rows(),
        ]
    )

    result = run_once(
        port,
        with_run(compiled_eval_of(), retry=1, accept_statuses=("COMPLETED", "PARTIALLY_COMPLETED")),
        partial_settle_ms=0,
    )

    # The retry absorbed the partial run, but the config that would pass a partial is still wrong.
    assert result.evals[0].accepted and not result.success
    assert [item.code for item in result.diagnostics] == ["SST-VAL730", "SST-APL029"]


def test_a_retry_after_an_unstarted_or_unread_attempt_reports_why_the_first_did_not_pass() -> None:
    unreadable = QueryResult(("INPUT_ID",), (("q",),))
    port = EvalSnowflake(
        [
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R2"),
            unreadable,
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R3"),
            result_rows(),
        ]
    )
    port.execute_results.append(ExecResult(False, error=ExecutionError("cannot start")))

    result = run_once(port, with_run(compiled_eval_of(), retry=2))

    assert [attempt.terminal_status for attempt in result.evals[0].attempts] == [
        "START_FAILED",
        "COMPLETED",
        "COMPLETED",
    ]
    assert result.success
    started, unread = result.diagnostics
    assert (started.code, started.severity, unread.code) == ("SST-APL029", Severity.WARNING, "SST-APL029")
    assert "ended START_FAILED and was retried: " in started.message and "cannot start" in started.message
    assert "_R2' ended COMPLETED and was retried: " in unread.message


def test_a_gate_run_retries_an_unread_attempt_until_its_retries_run_out() -> None:
    unreadable = QueryResult(("INPUT_ID",), (("q",),))
    port = EvalSnowflake(
        [
            status_result("COMPLETED"),
            unreadable,
            status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R2"),
            unreadable,
        ]
    )

    result = run_once(port, with_run(compiled_eval_of(), retry=1))

    assert not result.success
    assert [attempt.attempt for attempt in result.evals[0].attempts] == [1, 2]
    assert all(attempt.retrieval_error for attempt in result.evals[0].attempts)
    # No retry completed, so each attempt reports the error its outcome is.
    assert [item.code for item in result.diagnostics] == ["SST-SNO001", "SST-SNO001"]


def test_baseline_capture_runs_until_configured_completed_attempt_count() -> None:
    compiled = compiled_eval_of()
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
    start_port.execute_results.append(ExecResult(False, error=ExecutionError("cannot start")))
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


def test_eval_runner_ends_an_undocumented_status_and_fails_the_eval() -> None:
    port = EvalSnowflake([status_result("UNKNOWN"), status_result("COMPLETED")])
    use_case, compiled = runner(port)

    result = use_case.run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    [attempt] = result.evals[0].attempts
    assert attempt.terminal_status == "UNKNOWN"
    assert not result.success
    [diagnostic] = result.diagnostics
    assert diagnostic.code == "SST-APL023"
    assert diagnostic.message == (
        "eval 'eval:sales_agent': run 'EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z' ended UNKNOWN: no status details"
    )


def test_eval_runner_stops_polling_a_failed_run_and_reports_its_details() -> None:
    run_name = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    # MEASURED: a run whose agent could not be invoked reports FAILED, which the docs omit.
    failed = QueryResult(STATUS_COLUMNS, ((run_name, "SALES_AGENT", "CORTEX AGENT", "FAILED", "Invocation failed"),))
    port = EvalSnowflake([status_result("INVOCATION_IN_PROGRESS"), failed])
    use_case, compiled = runner(port)

    result = use_case.run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    [attempt] = result.evals[0].attempts
    assert (attempt.terminal_status, attempt.status_details) == ("FAILED", ("Invocation failed",))
    assert not result.evals[0].accepted
    assert [diagnostic.message for diagnostic in result.diagnostics] == [
        f"eval 'eval:sales_agent': run '{run_name}' ended FAILED: Invocation failed"
    ]
    assert len([query for query, _ in port.queries if "'STATUS'" in query]) == 2


def test_eval_runner_polls_every_in_progress_status_until_its_deadline() -> None:
    statuses = ("CREATED", "INVOCATION_IN_PROGRESS", "INVOCATION_COMPLETED", "COMPUTATION_IN_PROGRESS")
    port = EvalSnowflake([status_result(status) for status in statuses])
    compiled = compiled_eval_of()

    result = RunEvalSuite(port, PollClock()).run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z", deadline_ms=15_000),
    )

    # Reads at 0, 5, 10 and, the last one, 15 seconds.
    assert result.evals[0].attempts[0].terminal_status == "STATUS_FAILED"
    assert result.evals[0].attempts[0].retrieval_error == (
        "evaluation run 'EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z' did not reach a terminal status "
        "within 15s; it was COMPUTATION_IN_PROGRESS"
    )


def test_eval_run_name_outside_a_git_work_tree_carries_the_eval_fingerprint() -> None:
    port = EvalSnowflake([status_result("CANCELLED", run_name="unused"), status_result("CANCELLED", run_name="unused")])
    use_case, compiled = runner(port)

    result = use_case.run((compiled,), options=EvalRunOptions("WORKTREE", timestamp="20260928T010203Z"))

    run_name = result.evals[0].attempts[0].run_name
    assert run_name == f"EVAL_SALES_AGENT_{compiled.rendered_artifact.fingerprint[:7]}_ci_20260928T010203Z"
    assert "WORKTRE" not in run_name
    blank = use_case.run((compiled,), options=EvalRunOptions("", timestamp="20260928T010203Z"))
    assert blank.evals[0].attempts[0].run_name == run_name


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
    compiled = compiled_eval_of()
    ground_truth = {"ground_truth_invocations": [], "ground_truth_output": "Answer"}
    expected = {"Question": (_question_identity("Question", ground_truth), ground_truth)}

    assert _result_question_key({"INPUT": "Question", "GROUND_TRUTH": "Answer"}, expected) in {
        key for key, _ in _expected_question_map(compiled).values()
    }
    with pytest.raises(ValueError, match="unrecognized flattened ground truth"):
        _result_question_key({"INPUT": "Question", "GROUND_TRUTH": "Different"}, expected)


def test_eval_publication_preflight_refuses_unapplied_state() -> None:
    compiled = compiled_eval_of()
    manifest = build_manifest(compile_eval())
    state = State(STATE_SCHEMA_VERSION, target(), manifest.manifest_id, "cfg", None, {})

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(FakeSnowflake()),
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

    assert "not finite" in (invalid.evals[0].attempts[0].retrieval_error or "")


def test_eval_runner_rejects_status_for_a_different_run() -> None:
    port = EvalSnowflake([status_result("COMPLETED", "some-other-run")])
    use_case, compiled = runner(port)

    result = use_case.run(
        (compiled,),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )

    assert result.evals[0].attempts[0].terminal_status == "STATUS_FAILED"
    assert "some-other-run" in (result.evals[0].attempts[0].retrieval_error or "")


def test_custom_metric_threshold_default_controls_pass_vector() -> None:
    compiled = compiled_eval_of()
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
    compiled = compiled_eval_of()
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


def test_eval_runner_fail_fast_stops_before_the_next_eval() -> None:
    port = EvalSnowflake([status_result("FAILED")])

    result = RunEvalSuite(port, FixedClock()).run(
        (compiled_eval_of(), compiled_eval_of()),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
        fail_fast=True,
    )

    assert len(result.evals) == 1 and len(port.scripts) == 1
    assert not result.success


def test_an_interrupt_before_any_run_started_propagates_as_it_came() -> None:
    class Interrupted(EvalSnowflake):
        def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
            del qualified_name, selector
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt) as raised:
        run_once(Interrupted([]))

    assert type(raised.value) is KeyboardInterrupt


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
    initial: StagedFileMetadata,
    trusted_digest: str | None,
    expected_uploads: list[tuple[str, bytes]],
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
    observations: list[StagedFileMetadata | None],
    trusted_digest: str | None,
    staged_content: bytes | None,
    message: str,
) -> None:
    port = ConfigReadbackSnowflake(observations, staged_content=staged_content)

    with pytest.raises(SnowflakePortError, match=message):
        RunEvalSuite(port, FixedClock())._ensure_config("@config", b"config", trusted_digest)


def test_eval_runner_reports_a_refused_start_with_the_error_snowflake_gave() -> None:
    port = EvalSnowflake([])
    port.execute_results.append(ExecResult(False, error=ExecutionError("Insufficient privileges to operate on task")))

    result = run_once(port)

    assert result.evals[0].attempts[0].terminal_status == "START_FAILED"
    assert "Insufficient privileges to operate on task" in (result.evals[0].attempts[0].retrieval_error or "")


@pytest.mark.parametrize(
    ("agent_name", "agent_type", "message"),
    (
        ("OTHER_AGENT", "CORTEX AGENT", "different agent"),
        ("SALES_AGENT", "OTHER", "different agent type"),
    ),
)
def test_eval_runner_rejects_status_for_a_different_agent_identity(
    agent_name: str, agent_type: str, message: str
) -> None:
    run_name = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    status = QueryResult(STATUS_COLUMNS, ((run_name, agent_name, agent_type, "COMPLETED", []),))

    result = run_once(EvalSnowflake([status]))

    assert result.evals[0].attempts[0].terminal_status == "STATUS_FAILED"
    assert message in (result.evals[0].attempts[0].retrieval_error or "")


@pytest.mark.parametrize("rows", ((), (("run", "agent", "type", "status", []),) * 2))
def test_single_row_rejects_missing_and_duplicate_status_rows(rows: tuple[tuple[object, ...], ...]) -> None:
    with pytest.raises(SnowflakePortError, match="expected 1"):
        _single_row(STATUS_COLUMNS, rows, STATUS_COLUMNS, "evaluation status")


def test_eval_runner_rejects_blank_required_status_values() -> None:
    run_name = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
    status = QueryResult(STATUS_COLUMNS, ((run_name, " ", "CORTEX AGENT", "COMPLETED", []),))

    result = run_once(EvalSnowflake([status]))

    assert "omitted AGENT_NAME" in (result.evals[0].attempts[0].retrieval_error or "")


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

    assert "STATUS_DETAILS must be an array" in (result.evals[0].attempts[0].retrieval_error or "")


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
def test_eval_runner_rejects_unknown_metrics_and_wrong_metric_types(row: tuple[object, ...], message: str) -> None:
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
def test_expected_question_map_rejects_malformed_compiled_payloads(payload: str, message: str) -> None:
    compiled = compiled_eval_of()
    compiled = replace(compiled, rendered=replace(compiled.rendered, dataset_payload=payload))

    with pytest.raises(ValueError, match=message):
        _expected_question_map(compiled)


def test_metric_passed_fails_closed_for_errors_status_codes_and_missing_scores() -> None:
    compiled = compiled_eval_of()

    assert not _metric_passed(compiled, "answer_correctness", 0.9, {"status": 200}, "failed")
    assert not _metric_passed(compiled, "answer_correctness", 0.9, {"status": 500}, None)
    assert not _metric_passed(compiled, "answer_correctness", 0.9, {"status": "bad"}, None)
    assert not _metric_passed(compiled, "answer_correctness", None, None, None)


def test_metric_passed_applies_bounded_system_thresholds() -> None:
    compiled = compiled_eval_of()
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
def test_nonnegative_int_normalizes_supported_values(value: object, expected: int) -> None:
    assert _nonnegative_int(value) == expected


@pytest.mark.parametrize("value", (True, object(), "not-an-int", -1))
def test_nonnegative_int_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValueError, match="evaluation cost value"):
        _nonnegative_int(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    ((None, None), ("1.25", 1.25)),
)
def test_optional_float_normalizes_supported_values(value: object, expected: float | None) -> None:
    assert _optional_float(value) == expected


@pytest.mark.parametrize("value", (True, object(), "not-a-number"))
def test_optional_float_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValueError, match="evaluation score"):
        _optional_float(value)


def test_status_details_helper_normalizes_and_rejects_values() -> None:
    assert _status_details(None) == ()
    assert _status_details('["one", 2]') == ("one", "2")
    assert _status_details("Invocation failed") == ("Invocation failed",)
    assert _status_details('"Invocation failed"') == ("Invocation failed",)
    assert _status_details("  ") == ()
    with pytest.raises(ValueError, match="must be an array"):
        _status_details({})


def test_compact_timestamp_uses_utc_compact_format() -> None:
    value = _compact_timestamp()

    assert len(value) == 16
    assert value.endswith("Z")


def test_eval_publication_preflight_refuses_manifest_mismatch() -> None:
    compiled = compiled_eval_of()
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
        EvalLifecycleHandler(FakeSnowflake()),
    )

    assert diagnostics[0].code == "SST-APL012"
    assert compiled.artifact_key in diagnostics[0].message


def test_eval_publication_preflight_allows_unrelated_rows_from_older_manifests() -> None:
    compiled = compiled_eval_of()
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
    port = FakeSnowflake()
    port.existing = {name.sql for _, name in artifact.physical_resources}
    seed_dataset_version(artifact, port)
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
    compiled = compiled_eval_of()
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
    port = FakeSnowflake()
    port.existing = {name.sql for _, name in artifact.physical_resources}
    seed_dataset_version(artifact, port)
    port.existing.add("DB.S.EVAL_CONFIGS")
    port.stage_formats["DB.S.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    config_path = EvalLifecycleHandler(port).config_path(artifact)
    port.stage_file(config_path, content)

    diagnostics = validate_eval_publication(
        (compiled,),
        manifest,
        state,
        EvalLifecycleHandler(port),
    )

    assert not diagnostics


def test_eval_publication_preflight_reports_plan_diagnostics() -> None:
    compiled = compiled_eval_of()
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
    port = FakeSnowflake()
    port.existing = {name.sql for _, name in artifact.physical_resources}
    seed_dataset_version(artifact, port)
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
    compiled = compiled_eval_of()
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
        EvalLifecycleHandler(FakeSnowflake()),
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

    # The JSON envelope is untyped by design; read it as the document it is.
    payload: Any = eval_suite_json(suite)

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
    first = compiled_eval_of()
    run = first.resolved.config.run
    assert run is not None and run.concurrency is None
    requesting = replace(
        first,
        resolved=replace(first.resolved, config=replace(first.resolved.config, run=replace(run, concurrency=3))),
    )
    without_run = replace(first, resolved=replace(first.resolved, config=replace(first.resolved.config, run=None)))

    assert suite_concurrency((first, requesting), EvalDefaults(concurrency=5)) == 5
    assert suite_concurrency((first, requesting), EvalDefaults(concurrency=-2)) == 1
    assert suite_concurrency((first, requesting, without_run), EvalDefaults()) == 3
    assert suite_concurrency((first, without_run), EvalDefaults()) == 1


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
def test_ensure_config_rejects_a_readback_that_is_missing_short_or_different(
    readback: bytes | None, message: str
) -> None:
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
    assert (
        _result_question_key({"INPUT": "Question", "GROUND_TRUTH": json.dumps(ground_truth)}, expected)
        == (expected["Question"][0])
    )


def test_eval_runner_rejects_a_record_answering_two_questions_and_an_unanswered_question() -> None:
    compiled = compiled_eval_of()
    dataset = json.loads(compiled.rendered.dataset_payload)
    dataset.append(
        {"input_query": "Other", "ground_truth": {"ground_truth_invocations": [], "ground_truth_output": "Answer"}}
    )
    compiled = replace(compiled, rendered=replace(compiled.rendered, dataset_payload=json.dumps(dataset)))
    first = result_row(input_query="Question", record_id="record-1").rows[0]
    second = result_row(input_query="Other", record_id="record-1").rows[0]

    assert "maps to multiple questions" in retrieval_error((first, second), compiled)
    assert "question set differs from the authored dataset" in retrieval_error(result_rows().rows, compiled)


def test_each_run_reports_its_retention_class_for_whoever_reaps_decision_runs() -> None:
    compiled = compiled_eval_of()
    run = compiled.resolved.config.run
    assert run is not None
    retention = EvalRetention(("ci",), ("sweep",), 30)
    assert retention_class(replace(run, retention=retention), None) == "audit"
    assert retention_class(replace(run, variant="sweep", retention=retention), None) == "decision"
    assert retention_class(replace(run, variant="adhoc", retention=retention), "audit") == "audit"
    assert retention_class(run, None) is None

    sweep = replace(compiled.resolved.config, run=replace(run, variant="sweep", retention=retention))
    port = EvalSnowflake([status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_sweep_20260928T010203Z"), result_rows()])
    result = RunEvalSuite(port, FixedClock()).run(
        (replace(compiled, resolved=replace(compiled.resolved, config=sweep)),),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
    )
    payload = eval_suite_json(result)["evals"]
    assert isinstance(payload, list)
    assert payload[0]["retention"] == {"class": "decision", "decision_window_days": 30}


def test_a_config_without_a_run_block_takes_the_project_retention_class() -> None:
    compiled = compiled_eval_of()
    bare = replace(compiled, resolved=replace(compiled.resolved, config=replace(compiled.resolved.config, run=None)))
    result = RunEvalSuite(EvalSnowflake([]), FixedClock()).run(
        (bare,), defaults=EvalDefaults(retention="audit"), options=EvalRunOptions("abcdef0")
    )
    assert (result.evals[0].retention, result.evals[0].decision_window_days) == ("audit", None)


def test_flattened_ground_truth_matches_as_sent_or_parsed_and_an_absent_one_only_when_none_is_expected() -> None:
    numeric = {"ground_truth_output": "42"}
    expected = {"Question": (_question_identity("Question", numeric), numeric)}
    key = expected["Question"][0]
    # The output text parses as JSON, so it is matched both as sent and as parsed.
    assert _result_question_key({"INPUT": "Question", "GROUND_TRUTH": "42"}, expected) == key
    assert _result_question_key({"INPUT": "Question", "GROUND_TRUTH": '"42"'}, expected) == key
    with pytest.raises(ValueError, match="returned no ground truth"):
        _result_question_key({"INPUT": "Question", "GROUND_TRUTH": None}, expected)

    empty: dict[str, object] = {}
    reference_free = {"Question": (_question_identity("Question", empty), empty)}
    for absent in (None, ""):
        assert (
            _result_question_key({"INPUT": "Question", "GROUND_TRUTH": absent}, reference_free)
            == (reference_free["Question"][0])
        )
    with pytest.raises(ValueError, match="unrecognized flattened ground truth"):
        _result_question_key({"INPUT": "Question", "GROUND_TRUTH": "Answer"}, reference_free)
