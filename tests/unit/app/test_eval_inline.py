"""Unpublished evals: each question asked of the compiled agent, scored exactly or by a judge."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.ask import stale_dependencies
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.evals.inline import INLINE_RUN_NAME, RunInlineEvals
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDefaults,
    EvalGroundTruth,
    EvalInvocation,
    EvalQuestion,
    EvalSystemMetric,
    ResolvedEval,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolUse
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.apply_sessions import ListPool
from tests.helpers.ask_builders import compiled_agent, inline_setup
from tests.helpers.eval_builders import resolved_eval
from tests.helpers.eval_inputs import settled
from tests.helpers.snowflake_fake import FakeSnowflake

ORIGIN = Origin("dataset.yml", 1, 1)
SALES = ToolUse("SALES", "cortex_analyst_text_to_sql")


def _eval(*system: EvalSystemMetric, questions: tuple[EvalQuestion, ...] | None = None) -> ResolvedEval:
    """`resolved_eval()` with these system metrics, and these questions when given."""
    base = resolved_eval()
    dataset = base.dataset if questions is None else replace(base.dataset, questions=questions)
    return replace(
        base, dataset=dataset, config=replace(base.config, system_metrics=system or base.config.system_metrics)
    )


def _question(text: str, *tools: str, output: str | None = "Answer", invocations: bool = True) -> EvalQuestion:
    called = tuple(EvalInvocation(ORIGIN, tool) for tool in tools) if invocations else None
    return EvalQuestion(ORIGIN, text, EvalGroundTruth(ORIGIN, called, output))


def _tool_selection(minimum: float | None = 0.8) -> EvalSystemMetric:
    threshold = ThresholdRange(minimum) if minimum is not None else None
    return EvalSystemMetric(ORIGIN, "tool_selection_accuracy", "v3", gate=minimum is not None, threshold=threshold)


def _codes(found: object) -> list[str]:
    return [item.code for item in found]  # type: ignore[attr-defined]


def test_every_question_runs_inline_and_each_metric_is_scored() -> None:
    port = FakeSnowflake()
    port.agent_replies = {"Orders?": AgentReply("12.", (SALES, ToolUse("chart", "data_to_chart")), duration_ms=40)}
    questions = (_question("Orders?", "SALES"), _question("Decline?"))
    setup = inline_setup(
        _eval(_tool_selection(), EvalSystemMetric(ORIGIN, "answer_correctness", "v3"), questions=questions)
    )
    outcome = RunInlineEvals(port, timeout=45).run((setup,))
    assert outcome.passed and _codes(outcome.diagnostics) == []
    assert [(run[3], run[4]) for run in port.inline_runs] == [("Orders?", 45), ("Decline?", 45)]
    assert {run[2] for run in port.inline_runs} == {setup.agent.payload}
    [result] = outcome.suite.evals
    [attempt] = result.attempts
    assert (attempt.run_name, attempt.terminal_status, attempt.cost.duration_ms) == (INLINE_RUN_NAME, "COMPLETED", 40)
    scores = {
        (row.input_query, metric.metric_name): (metric.score, metric.passed)
        for row in attempt.rows
        for metric in row.metrics
    }
    assert scores == {
        ("Orders?", "answer_correctness"): (1.0, True),
        ("Orders?", "grounding"): (1.0, True),
        ("Orders?", "tool_selection_accuracy"): (1.0, True),
        ("Decline?", "answer_correctness"): (1.0, True),
        ("Decline?", "grounding"): (1.0, True),
        ("Decline?", "tool_selection_accuracy"): (1.0, True),
    }
    # answer_correctness is judged by the agent's orchestration model, the custom metric by its own.
    assert {model for model, _ in port.judged} == {"claude-sonnet-4-6"}
    custom = next(prompt for _, prompt in port.judged if prompt.startswith("Score from 0 to 5"))
    assert custom.endswith('"score", a number from 0 to 5, and "reason", one sentence saying why.')
    assert outcome.data["mode"] == "unpublished" and outcome.data["gate_verdict"] == "passed"
    assert outcome.data["approximated_metrics"] == ["answer_correctness", "grounding"]
    assert outcome.data["unscored_metrics"] == []


def test_an_exact_metric_outside_its_threshold_fails_the_run_and_a_judged_one_never_does() -> None:
    port = FakeSnowflake()
    port.agent_replies = {"Orders?": AgentReply("none", ())}
    port.judge_answer = {"score": 0, "reason": "wrong"}
    questions = (_question("Orders?", "SALES"), _question("Free?", invocations=False))
    outcome = RunInlineEvals(port).run((inline_setup(_eval(_tool_selection(), questions=questions)),))
    assert not outcome.passed
    [failed] = outcome.diagnostics
    assert (failed.code, failed.severity) == ("SST-ASK007", Severity.ERROR)
    assert failed.message == (
        "eval 'sales_agent': 'tool_selection_accuracy' fell outside its threshold 0.8..inf "
        "on 1 question(s): questions 1"
    )
    assert outcome.data["gate_verdict"] == "failed"


def test_an_ungated_exact_metric_reports_its_score_without_failing() -> None:
    port = FakeSnowflake()
    outcome = RunInlineEvals(port).run(
        (inline_setup(_eval(_tool_selection(None), questions=(_question("Q", "SALES"),))),)
    )
    assert outcome.passed
    [row] = outcome.suite.evals[0].attempts[0].rows
    assert [(metric.metric_name, metric.score) for metric in row.metrics if metric.metric_name.startswith("tool")] == [
        ("tool_selection_accuracy", 0.0)
    ]


@pytest.mark.parametrize("failure", [AgentReply(error="model not allowed"), SnowflakePortError("statement timed out")])
def test_a_question_the_agent_cannot_answer_fails_and_is_scored_by_nothing(failure: object) -> None:
    port = FakeSnowflake()
    port.agent_replies = {"Bad?": failure}  # type: ignore[dict-item]
    questions = (_question("Good?"), _question("Bad?"))
    outcome = RunInlineEvals(port).run((inline_setup(_eval(_tool_selection(), questions=questions)),))
    assert not outcome.passed and _codes(outcome.diagnostics) == ["SST-ASK002"]
    assert "question 2:" in outcome.diagnostics[0].message
    attempt = outcome.suite.evals[0].attempts[0]
    assert attempt.terminal_status == "PARTIALLY_COMPLETED"
    bad = next(row for row in attempt.rows if row.input_query == "Bad?")
    assert all(metric.score is None and not metric.passed for metric in bad.metrics)
    # Only the answered question is judged, by the custom metric alone.
    assert len(port.judged) == 1


def test_an_eval_none_of_whose_questions_answer_failed() -> None:
    port = FakeSnowflake()
    port.agent_replies = {"Question": AgentReply(error="no")}
    outcome = RunInlineEvals(port).run((inline_setup(),))
    assert outcome.suite.evals[0].attempts[0].terminal_status == "FAILED"
    assert not outcome.suite.evals[0].success


@pytest.mark.parametrize(
    ("answer", "detail"),
    [
        (SnowflakePortError("model unavailable"), "model unavailable"),
        ({"score": 9}, "it answered no number from 0 to 1"),
    ],
)
def test_a_judge_that_gives_no_score_leaves_its_metric_unscored(answer: object, detail: str) -> None:
    port = FakeSnowflake()
    port.judge_answer = answer
    outcome = RunInlineEvals(port).run((inline_setup(_eval(EvalSystemMetric(ORIGIN, "answer_correctness", "v3"))),))
    assert outcome.passed
    warned = [item for item in outcome.diagnostics if item.code == "SST-ASK006"]
    assert warned and warned[0].severity is Severity.WARNING
    assert any(item.message.endswith(f"gave no score for question 1: {detail}") for item in warned)


def test_a_question_without_a_ground_truth_output_is_not_judged_for_correctness() -> None:
    port = FakeSnowflake()
    resolved = replace(
        _eval(EvalSystemMetric(ORIGIN, "answer_correctness", "v3"), questions=(_question("Q", output=None),)),
        custom_metrics=(),
    )
    outcome = RunInlineEvals(port).run((inline_setup(resolved),))
    assert outcome.passed and port.judged == []
    assert outcome.suite.evals[0].attempts[0].rows[0].metrics[0].score is None


def test_metrics_only_the_evaluation_service_scores_are_reported_unscored() -> None:
    system = (
        EvalSystemMetric(ORIGIN, "tool_execution_accuracy", "v3"),
        EvalSystemMetric(ORIGIN, "logical_consistency", "v3"),
    )
    outcome = RunInlineEvals(FakeSnowflake()).run((inline_setup(replace(_eval(*system), custom_metrics=())),))
    assert outcome.passed
    assert [(item.code, item.severity) for item in outcome.diagnostics] == [("SST-ASK005", Severity.INFO)] * 2
    assert outcome.data["unscored_metrics"] == ["logical_consistency", "tool_execution_accuracy"]


def test_correctness_without_any_judge_model_is_not_scored() -> None:
    resolved = replace(_eval(EvalSystemMetric(ORIGIN, "answer_correctness", "v3")), custom_metrics=())
    outcome = RunInlineEvals(FakeSnowflake()).run((inline_setup(resolved, orchestration_model="auto"),))
    [warned] = outcome.diagnostics
    assert warned.code == "SST-ASK004" and "no judge model" in warned.message


def test_the_evals_judge_model_judges_correctness_before_the_agents_model() -> None:
    port = FakeSnowflake()
    resolved = settled(
        replace(_eval(EvalSystemMetric(ORIGIN, "answer_correctness", "v3")), custom_metrics=()),
        judge_model="llama4-maverick",
    )
    RunInlineEvals(port).run((inline_setup(resolved),), defaults=EvalDefaults(judge_model="ignored"))
    assert [model for model, _ in port.judged] == ["llama4-maverick"]


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (
            {"prompt": "Score {{ span_name }} from 0 to 5."},
            "its prompt uses span_name, which only a published run's trace fills",
        ),
        ({"prompt": None}, "it has no prompt or no score bands"),
        ({"model": "auto"}, "it names no judge model"),
    ],
)
def test_a_custom_metric_a_reply_cannot_judge_is_not_scored(change: dict[str, object], reason: str) -> None:
    base = resolved_eval()
    resolved = replace(base, custom_metrics=(replace(base.custom_metrics[0], **change),))  # type: ignore[arg-type]
    outcome = RunInlineEvals(FakeSnowflake()).run((inline_setup(resolved),))
    warned = [item for item in outcome.diagnostics if item.code == "SST-ASK004"]
    assert [item.message for item in warned] == [
        f"eval 'sales_agent': metric 'grounding' is not scored without publishing: {reason}"
    ]
    assert outcome.data["unscored_metrics"] == ["grounding"]


def test_a_disabled_custom_metric_and_an_unnamed_system_metric_are_left_out() -> None:
    base = resolved_eval()
    resolved = replace(
        _eval(EvalSystemMetric(ORIGIN, None)), custom_metrics=(replace(base.custom_metrics[0], enabled=False),)
    )
    outcome = RunInlineEvals(FakeSnowflake()).run((inline_setup(resolved),))
    assert outcome.passed and outcome.suite.evals[0].attempts[0].rows[0].metrics == ()


def test_fail_fast_stops_after_the_first_eval_that_fails() -> None:
    port = FakeSnowflake()
    port.agent_replies = {"Question": AgentReply(error="no")}
    first = inline_setup()
    resolved = first.compiled.resolved
    renamed = replace(resolved, agent=replace(resolved.agent, name="ops_agent"))
    second = replace(first, compiled=replace(first.compiled, resolved=renamed))
    outcome = RunInlineEvals(port).run((first, second), fail_fast=True)
    assert [result.eval_key for result in outcome.suite.evals] == ["eval:sales_agent"]
    everything = RunInlineEvals(FakeSnowflake()).run((first, second), fail_fast=True)
    assert len(everything.suite.evals) == 2


def test_questions_run_at_once_on_leased_sessions_and_an_interrupt_halts_them() -> None:
    workers = [FakeSnowflake(), FakeSnowflake()]
    pool = ListPool(workers)
    questions = (_question("A"), _question("B"), _question("C"))
    setup = inline_setup(_eval(questions=questions))
    outcome = RunInlineEvals(FakeSnowflake(), pool).run((setup,), threads=2, notes=())
    assert outcome.passed and pool.leases == 3
    assert sorted(run[3] for worker in workers for run in worker.inline_runs) == ["A", "B", "C"]
    interrupted = [FakeSnowflake(), FakeSnowflake()]
    for worker in interrupted:
        worker.fail("run_inline", KeyboardInterrupt())
    halted = ListPool(interrupted)
    with pytest.raises(KeyboardInterrupt):
        RunInlineEvals(FakeSnowflake(), halted).run((setup,), threads=2)
    assert halted.halted == "the unpublished eval run was interrupted"
    alone = FakeSnowflake()
    alone.fail("run_inline", KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        RunInlineEvals(alone).run((setup,))


def test_the_callers_notes_come_first_and_progress_hears_each_question() -> None:
    heard: list[str] = []
    note = inline_setup().agent
    notes = stale_dependencies(compiled_agent("x", note.artifact_key), CompileResult((note,)), None)
    outcome = RunInlineEvals(FakeSnowflake(), progress=heard.append).run((inline_setup(),), notes=notes)
    assert _codes(outcome.diagnostics)[0] == "SST-ASK001"
    assert heard == ["eval:sales_agent: question 1 answered"]
    assert json.dumps(outcome.data)  # the projection is plain JSON
