from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate, persist_gate
from snowflake_semantic_tools.domain.model.eval import EvalMetricResult, EvalResultRow, EvalRunAttempt, ThresholdRange
from tests.helpers.eval_builders import compiled_eval_of, run_result
from tests.helpers.eval_state_store import InMemoryEvalStateStore


def attempt(name: str, values: tuple[tuple[str, str, bool], ...]) -> EvalRunAttempt:
    rows: dict[str, list[EvalMetricResult]] = {}
    for question, metric, passed in values:
        rows.setdefault(question, []).append(EvalMetricResult(question, metric, 1.0 if passed else 0.0, passed))
    return EvalRunAttempt(
        name,
        1,
        "COMPLETED",
        tuple(EvalResultRow(question, "", tuple(metrics)) for question, metrics in sorted(rows.items())),
        agent_version="VERSION$1",
    )


def compiled_eval() -> CompiledEval:
    compiled = compiled_eval_of()
    run = compiled.resolved.config.run
    assert run is not None
    return replace(
        compiled,
        resolved=replace(
            compiled.resolved,
            config=replace(
                compiled.resolved.config,
                system_metrics=(replace(compiled.resolved.config.system_metrics[0], gate=True),),
                run=replace(run, baseline_runs=2, tier="blocking"),
            ),
        ),
    )


def test_capture_baseline_requires_reason_and_configured_attempts() -> None:
    compiled = compiled_eval()
    first = attempt("run-1", (("q", "answer_correctness", True), ("q", "grounding", True)))
    with pytest.raises(ValueError, match="reason"):
        capture_baseline(compiled, run_result(first), reason="", captured_at="2026-09-01T00:00:00Z")
    with pytest.raises(ValueError, match="2 completed attempts"):
        capture_baseline(compiled, run_result(first), reason="initial", captured_at="2026-09-01T00:00:00Z")
    partial = replace(first, terminal_status="PARTIALLY_COMPLETED")
    # A partial attempt is never captured, so a completed one alone is still one too few.
    with pytest.raises(ValueError, match="requires 2 completed attempts, found 1"):
        capture_baseline(
            compiled,
            run_result(partial, first),
            reason="initial",
            captured_at="2026-09-01T00:00:00Z",
        )


def test_capture_baseline_stores_metadata_only_vectors() -> None:
    compiled = compiled_eval()
    attempts = (
        attempt("run-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("run-2", (("q", "answer_correctness", True), ("q", "grounding", False))),
    )
    baseline = capture_baseline(
        compiled,
        run_result(*attempts),
        reason="initial calibration",
        captured_at="2026-09-01T00:00:00Z",
    )

    assert baseline.run_names == ("run-1", "run-2")
    assert baseline.expires_at == "2026-10-01T00:00:00Z"
    assert {(item.question_key, item.metric_name): item.passed_attempts for item in baseline.metrics} == {
        ("q", "answer_correctness"): (True, True),
        ("q", "grounding"): (True, False),
    }
    assert "Question" not in repr(baseline)


def test_gate_counts_only_all_pass_baseline_to_any_fail_current() -> None:
    compiled = compiled_eval()
    baseline = capture_baseline(
        compiled,
        run_result(
            attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
            attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", False))),
        ),
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    current = run_result(
        attempt("current-1", (("q", "answer_correctness", False), ("q", "grounding", False))),
        attempt("current-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )

    verdict, diagnostics = evaluate_gate(compiled, current, baseline, now="2026-09-10T00:00:00Z")

    assert verdict.regression_count == 1
    assert verdict.regressions[0].metric_name == "answer_correctness"
    assert not verdict.passed
    # The eval is blocking, so the regression is an error that fails the run.
    [regressed] = diagnostics
    assert regressed.code == "SST-VAL763"
    assert regressed.message.endswith("regressed on 1 question/metric pair(s): answer_correctness")


def test_ungated_metric_failure_is_not_a_regression() -> None:
    compiled = compiled_eval()
    baseline = capture_baseline(
        compiled,
        run_result(
            attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
            attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
        ),
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    current = run_result(
        attempt("current-1", (("q", "answer_correctness", True), ("q", "grounding", False))),
        attempt("current-2", (("q", "answer_correctness", True), ("q", "grounding", False))),
    )

    verdict, _ = evaluate_gate(compiled, current, baseline, now="2026-09-10T00:00:00Z")

    assert verdict.passed
    assert verdict.regression_count == 0


def test_gate_absent_incompatible_expired_and_near_expiry() -> None:
    compiled = compiled_eval()
    current = run_result(
        attempt("run-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("run-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )
    absent, absent_diagnostics = evaluate_gate(compiled, current, None, now="2026-09-01T00:00:00Z")
    assert absent.reason == "baseline_absent"
    assert absent_diagnostics[0].code == "SST-VAL758"

    baseline = capture_baseline(compiled, current, reason="initial", captured_at="2026-09-01T00:00:00Z")
    incompatible, incompatible_diagnostics = evaluate_gate(
        replace(compiled, rendered=replace(compiled.rendered, config_fingerprint="f" * 64)),
        current,
        baseline,
        now="2026-09-01T00:00:00Z",
    )
    assert incompatible.reason == "baseline_incompatible"
    assert incompatible_diagnostics[0].code == "SST-VAL759"

    _, warning = evaluate_gate(compiled, current, baseline, now="2026-09-25T00:00:00Z")
    assert warning[0].code == "SST-VAL760"
    expired, expired_diagnostics = evaluate_gate(compiled, current, baseline, now="2026-10-01T00:00:00Z")
    assert expired.reason == "baseline_expired"
    assert expired_diagnostics[0].code == "SST-VAL761"


def test_a_fresh_baseline_warns_only_in_its_last_week() -> None:
    compiled = compiled_eval()
    current = run_result(
        attempt("run-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("run-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )
    # Captured 2026-09-01, so it expires 2026-10-01.
    baseline = capture_baseline(compiled, current, reason="initial", captured_at="2026-09-01T00:00:00Z")

    for now in ("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z", "2026-09-23T23:59:59Z"):
        verdict, diagnostics = evaluate_gate(compiled, current, baseline, now=now)
        assert verdict.passed and diagnostics == (), now
    for now in ("2026-09-24T00:00:00Z", "2026-09-30T23:59:59Z"):
        _, diagnostics = evaluate_gate(compiled, current, baseline, now=now)
        assert [item.code for item in diagnostics] == ["SST-VAL760"], now


def test_report_tier_does_not_block_and_gate_state_is_retrospective() -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )
    baseline = capture_baseline(
        compiled,
        baseline_result,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    current = run_result(
        attempt("current-1", (("q", "answer_correctness", False), ("q", "grounding", True))),
        attempt("current-2", (("q", "answer_correctness", False), ("q", "grounding", True))),
    )
    run = compiled.resolved.config.run
    assert run is not None
    report_compiled = replace(
        compiled,
        resolved=replace(
            compiled.resolved,
            config=replace(compiled.resolved.config, run=replace(run, tier="report")),
        ),
    )
    report_baseline = capture_baseline(
        report_compiled,
        baseline_result,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    verdict, _ = evaluate_gate(report_compiled, current, report_baseline, now="2026-09-10T00:00:00Z")
    assert verdict.passed and verdict.regression_count == 1

    blocking, _ = evaluate_gate(compiled, current, baseline, now="2026-09-10T00:00:00Z")
    store = InMemoryEvalStateStore()
    state = persist_gate(store, "dev", compiled, current, blocking, evaluated_at="2026-09-10T00:00:00Z")
    assert state.unresolved
    assert store.gates[("dev", compiled.artifact_key)] == state


def test_default_tier_and_default_baseline_runs_are_honored() -> None:
    compiled = compiled_eval()
    run = compiled.resolved.config.run
    assert run is not None
    inherited = replace(
        compiled,
        resolved=replace(
            compiled.resolved, config=replace(compiled.resolved.config, run=replace(run, tier=None, baseline_runs=None))
        ),
    )
    attempts = run_result(
        attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )
    baseline = capture_baseline(
        inherited,
        attempts,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
        default_tier="blocking",
        required_attempts=2,
    )
    assert baseline.tier == "blocking"
    verdict, _ = evaluate_gate(
        inherited,
        attempts,
        baseline,
        now="2026-09-10T00:00:00Z",
        default_tier="blocking",
    )
    assert verdict.tier == "blocking"


def test_local_baseline_run_count_overrides_global_default() -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )

    baseline = capture_baseline(
        compiled,
        baseline_result,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
        required_attempts=(
            compiled.resolved.config.run.baseline_runs
            if compiled.resolved.config.run is not None and compiled.resolved.config.run.baseline_runs is not None
            else 5
        ),
    )

    assert len(baseline.run_names) == 2


@pytest.mark.parametrize("status", ("FAILED", "PARTIALLY_COMPLETED"))
def test_a_completed_retry_is_judged_and_the_attempt_it_replaced_scores_nothing(status: str) -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )
    baseline = capture_baseline(
        compiled,
        baseline_result,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    # The attempt that did not pass carries failing scores; were it judged, they would regress.
    failed = replace(
        attempt("current-1", (("q", "answer_correctness", False), ("q", "grounding", False))),
        terminal_status=status,
    )
    retry = replace(attempt("current-1_R2", (("q", "answer_correctness", True), ("q", "grounding", True))), attempt=2)

    verdict, diagnostics = evaluate_gate(compiled, run_result(failed, retry), baseline, now="2026-09-10T00:00:00Z")

    assert verdict.passed and verdict.reason is None and verdict.regression_count == 0
    assert diagnostics == ()


def test_a_regression_in_the_completed_retry_still_fails_a_blocking_gate() -> None:
    compiled = compiled_eval()
    values = (("q", "answer_correctness", True), ("q", "grounding", True))
    baseline = capture_baseline(
        compiled,
        run_result(attempt("base-1", values), attempt("base-2", values)),
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    failed = replace(attempt("current-1", values), terminal_status="FAILED")
    retry = attempt("current-1_R2", (("q", "answer_correctness", False), ("q", "grounding", True)))

    verdict, diagnostics = evaluate_gate(compiled, run_result(failed, retry), baseline, now="2026-09-10T00:00:00Z")

    assert not verdict.passed and verdict.reason is None and verdict.regression_count == 1
    assert [item.code for item in diagnostics] == ["SST-VAL763"]


def test_a_baseline_is_captured_from_the_completed_attempts_around_a_failed_one() -> None:
    compiled = compiled_eval_of()
    values = (("q", "answer_correctness", True),)
    attempts = (
        attempt("run-1", values),
        replace(attempt("run-2", values), terminal_status="FAILED"),
        *(attempt(f"run-{number}", values) for number in range(3, 7)),
    )

    baseline = capture_baseline(
        compiled,
        run_result(*attempts),
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
        required_attempts=5,
    )

    assert baseline.run_names == ("run-1", "run-3", "run-4", "run-5", "run-6")


def test_a_baseline_with_too_few_completed_attempts_is_refused() -> None:
    values = (("q", "answer_correctness", True),)
    attempts = (
        *(attempt(f"run-{number}", values) for number in range(1, 5)),
        replace(attempt("run-5", values), terminal_status="FAILED"),
        replace(attempt("run-6", values), terminal_status="COMPLETED", retrieval_error="unreadable"),
    )

    with pytest.raises(ValueError, match="requires 5 completed attempts, found 4"):
        capture_baseline(
            compiled_eval_of(),
            run_result(*attempts),
            reason="initial",
            captured_at="2026-09-01T00:00:00Z",
            required_attempts=5,
        )


def test_threshold_or_gate_policy_change_invalidates_baseline() -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )
    baseline = capture_baseline(
        compiled,
        baseline_result,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    changed_metric = replace(
        compiled.resolved.config.system_metrics[0],
        gate=False,
        threshold=ThresholdRange(min=0.5),
    )
    changed = replace(
        compiled,
        resolved=replace(
            compiled.resolved,
            config=replace(compiled.resolved.config, system_metrics=(changed_metric,)),
        ),
    )

    verdict, diagnostics = evaluate_gate(changed, baseline_result, baseline, now="2026-09-10T00:00:00Z")

    assert verdict.reason == "baseline_incompatible"
    assert diagnostics[0].code == "SST-VAL759"


def test_changed_resolved_agent_version_invalidates_baseline() -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True), ("q", "grounding", True))),
        attempt("base-2", (("q", "answer_correctness", True), ("q", "grounding", True))),
    )
    baseline = capture_baseline(
        compiled,
        baseline_result,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    current = run_result(
        replace(baseline_result.attempts[0], agent_version="VERSION$2"),
        replace(baseline_result.attempts[1], agent_version="VERSION$2"),
    )

    verdict, diagnostics = evaluate_gate(compiled, current, baseline, now="2026-09-10T00:00:00Z")

    assert verdict.reason == "baseline_incompatible"
    assert diagnostics[0].code == "SST-VAL759"


def test_multi_question_baseline_policy_is_compared_once_per_metric() -> None:
    compiled = compiled_eval()
    values = (
        ("q1", "answer_correctness", True),
        ("q1", "grounding", True),
        ("q2", "answer_correctness", True),
        ("q2", "grounding", True),
    )
    baseline_result = run_result(attempt("base-1", values), attempt("base-2", values))
    baseline = capture_baseline(
        compiled,
        baseline_result,
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )

    verdict, diagnostics = evaluate_gate(compiled, baseline_result, baseline, now="2026-09-10T00:00:00Z")

    assert verdict.passed
    assert not diagnostics.has_errors


@pytest.mark.parametrize(
    ("attempts", "message"),
    (
        (
            (
                attempt("base-1", (("q", "answer_correctness", True),)),
                replace(
                    attempt("base-2", (("q", "answer_correctness", True),)),
                    agent_version="VERSION$2",
                ),
            ),
            "one immutable agent version",
        ),
        (
            (replace(attempt("base-1", (("q", "answer_correctness", True),)), agent_version="committed"),),
            "is not immutable",
        ),
    ),
)
def test_capture_baseline_requires_one_concrete_agent_version(
    attempts: tuple[EvalRunAttempt, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        capture_baseline(
            compiled_eval(),
            run_result(*attempts),
            reason="initial",
            captured_at="2026-09-01T00:00:00Z",
            required_attempts=len(attempts),
        )


def test_gate_without_completed_attempt_has_no_current_signal() -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True),)),
        attempt("base-2", (("q", "answer_correctness", True),)),
    )
    baseline = capture_baseline(compiled, baseline_result, reason="initial", captured_at="2026-09-01T00:00:00Z")
    current = run_result(replace(baseline_result.attempts[0], terminal_status="CANCELLED"))

    verdict, diagnostics = evaluate_gate(compiled, current, baseline, now="2026-09-10T00:00:00Z")

    assert verdict.reason == "current_no_signal"
    assert diagnostics[0].code == "SST-SNO001"
    assert "immutable current agent version" in diagnostics[0].message


def test_gate_without_attempts_has_no_retrieval_signal_after_version_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True),)),
        attempt("base-2", (("q", "answer_correctness", True),)),
    )
    baseline = capture_baseline(compiled, baseline_result, reason="initial", captured_at="2026-09-01T00:00:00Z")
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.gate._result_agent_version", lambda _: "VERSION$1")
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.gate._incompatibility", lambda *args: None)

    verdict, diagnostics = evaluate_gate(compiled, run_result(), baseline, now="2026-09-10T00:00:00Z")

    assert verdict.reason == "retrieval_no_signal"
    assert diagnostics[-1].code == "SST-SNO001"
    assert "no retrievable completed attempt" in diagnostics[-1].message


def test_gate_rejects_changed_question_metric_vector(monkeypatch: pytest.MonkeyPatch) -> None:
    compiled = compiled_eval()
    baseline_result = run_result(
        attempt("base-1", (("q", "answer_correctness", True),)),
        attempt("base-2", (("q", "answer_correctness", True),)),
    )
    baseline = capture_baseline(compiled, baseline_result, reason="initial", captured_at="2026-09-01T00:00:00Z")
    baseline = replace(
        baseline,
        metrics=tuple(replace(item, question_key="different") for item in baseline.metrics),
    )
    current = run_result(
        attempt("current-1", (("q", "answer_correctness", True),)),
        attempt("current-2", (("q", "answer_correctness", True),)),
    )
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.gate._incompatibility", lambda *args: None)

    verdict, diagnostics = evaluate_gate(compiled, current, baseline, now="2026-09-01T00:00:00Z")

    assert verdict.reason == "baseline_incompatible"
    assert diagnostics[-1].code == "SST-VAL759"
    assert "question/metric vector differs" in diagnostics[-1].message


def test_capture_baseline_rejects_duplicate_incomplete_and_empty_metric_vectors() -> None:
    compiled = compiled_eval()
    metric = EvalMetricResult("q", "answer_correctness", 1.0, True)
    duplicate = EvalRunAttempt(
        "duplicate",
        1,
        "COMPLETED",
        (EvalResultRow("q", "", (metric, metric)),),
        agent_version="VERSION$1",
    )
    with pytest.raises(ValueError, match="duplicates"):
        capture_baseline(
            compiled,
            run_result(duplicate),
            reason="initial",
            captured_at="2026-09-01T00:00:00Z",
            required_attempts=1,
        )

    complete = attempt(
        "complete",
        (("q", "answer_correctness", True), ("q", "grounding", True)),
    )
    incomplete = attempt("incomplete", (("q", "answer_correctness", True),))
    with pytest.raises(ValueError, match="incomplete question/metric vector"):
        capture_baseline(
            compiled,
            run_result(complete, incomplete),
            reason="initial",
            captured_at="2026-09-01T00:00:00Z",
        )

    empty = EvalRunAttempt("empty", 1, "COMPLETED", agent_version="VERSION$1")
    with pytest.raises(ValueError, match="no metric vectors"):
        capture_baseline(
            compiled,
            run_result(empty),
            reason="initial",
            captured_at="2026-09-01T00:00:00Z",
            required_attempts=1,
        )


def test_invalid_tier_is_rejected() -> None:
    compiled = compiled_eval()
    run = compiled.resolved.config.run
    assert run is not None
    invalid = replace(
        compiled,
        resolved=replace(
            compiled.resolved,
            config=replace(compiled.resolved.config, run=replace(run, tier="advisory")),
        ),
    )

    with pytest.raises(ValueError, match="invalid eval tier 'advisory'"):
        capture_baseline(
            invalid,
            run_result(
                attempt("base-1", (("q", "answer_correctness", True),)),
                attempt("base-2", (("q", "answer_correctness", True),)),
            ),
            reason="initial",
            captured_at="2026-09-01T00:00:00Z",
        )


def test_naive_capture_timestamp_is_normalized_to_utc() -> None:
    compiled = compiled_eval()
    baseline = capture_baseline(
        compiled,
        run_result(
            attempt("base-1", (("q", "answer_correctness", True),)),
            attempt("base-2", (("q", "answer_correctness", True),)),
        ),
        reason="initial",
        captured_at="2026-09-01T00:00:00",
    )

    assert baseline.captured_at == "2026-09-01T00:00:00Z"
    assert baseline.expires_at == "2026-10-01T00:00:00Z"
