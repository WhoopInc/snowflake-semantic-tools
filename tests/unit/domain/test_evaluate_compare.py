"""The model comparison report, over hand-built runs: summaries, ranks, verdicts, and changed questions."""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.domain.evaluate.compare import (
    CANDIDATE,
    KNOWN,
    PENDING,
    PENDING_NOTE,
    PINNED,
    UNAVAILABLE,
    Amount,
    Dataset,
    MetricSpec,
    Subject,
    compare_eval,
    cost_states,
    decide,
    finalize,
    headline_metric,
    model_lifecycle,
    nearest_run,
    question_reports,
)
from snowflake_semantic_tools.domain.model.eval import (
    EvalMetricResult,
    EvalRunAttempt,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel
from tests.helpers.compare_reports import CORRECT, LOGIC, METRICS, compared, credits, model, run


def test_a_candidate_outside_the_pinned_spread_wins_at_its_relative_price() -> None:
    result = compared(
        model("pinned", PINNED, run("p1", 1.0, 0.0), run("p2", 0.5, 0.5)),
        model("opus", CANDIDATE, run("o1", 1.0, 1.0), run("o2", 1.0, 0.8)),
        costs={"pinned": credits(0.4), "opus": credits(0.8)},
    )
    assert [(item.model, item.rank) for item in result.models] == [("pinned", 2), ("opus", 1)]
    assert result.verdict.kind == "winner" and result.verdict.model == "opus"
    assert result.verdict.summary == (
        "opus beats the pinned pinned on answer_correctness (0.95 vs 0.50) outside the spread, "
        "at 2.0x the credits per question."
    )
    assert result.verdict.pin_suggestion == "# agent.yml of agent 'sales'\nconfig:\n  orchestration_model: opus"
    opus = result.models[1]
    summary = opus.metric(CORRECT.name)
    assert summary is not None and (summary.mean, summary.min, summary.max) == (0.95, 0.9, 1.0)
    # The pass count is of the run nearest the mean: o2, whose mean 0.9 is as near as o1's 1.0.
    assert (summary.passed, summary.total) == (2, 2)
    assert opus.cost.credits_per_question == Amount(KNOWN, 0.2)
    assert (opus.latency_mean, opus.latency_p95, opus.agent_input, opus.judge) == (150, 200, 200, 20)
    assert opus.request_ids == ("o1-r1", "o1-r2", "o2-r1", "o2-r2") and opus.status == "completed"
    assert result.notes == ("Held fixed.",)


def test_a_profiles_winner_is_pinned_under_cowork_profiles_and_its_price_may_be_unknown() -> None:
    result = compared(
        model("pinned", PINNED, run("p1", 0.0, 0.0), run("p2", 0.0, 0.0)),
        model("opus", CANDIDATE, run("o1", 1.0, 1.0), run("o2", 1.0, 1.0)),
        kind="cowork_profile",
        costs={"opus": credits(None, PENDING)},
    )
    assert result.verdict.pin_suggestion == (
        "# cowork_profile.yml of CoWork profile 'sales'\nconfig:\n  orchestration_model: opus"
    )
    assert result.verdict.summary.endswith("its credits per question pending.")
    assert result.notes == ("Held fixed.", PENDING_NOTE)
    assert cost_states(result) == {PENDING: ("opus",), UNAVAILABLE: ("pinned",)}
    unread = compared(
        model("pinned", PINNED, run("p1", 0.0), run("p2", 0.0)),
        model("opus", CANDIDATE, run("o1", 1.0), run("o2", 1.0)),
    )
    assert unread.verdict.summary.endswith("its credits per question unavailable.")


def test_a_lead_inside_the_pinned_spread_is_no_clear_winner() -> None:
    result = compared(
        model("pinned", PINNED, run("p1", 1.0, 0.5), run("p2", 0.5, 0.5)),
        model("opus", CANDIDATE, run("o1", 1.0, 0.6), run("o2", 1.0, 0.5)),
    )
    assert result.verdict.kind == "no_clear_winner" and result.verdict.model is None
    assert "their runs overlap" in result.verdict.summary and result.verdict.pin_suggestion is None


def test_no_candidate_beating_the_pinned_model_is_pinned_holds() -> None:
    result = compared(model("pinned", PINNED, run("p1", 1.0)), model("opus", CANDIDATE, run("o1", 0.5)))
    assert result.verdict.kind == "pinned_holds"
    assert result.verdict.summary == "No candidate beats the pinned pinned on answer_correctness (1.00)."


def test_a_cheaper_candidate_that_ties_the_pinned_model_is_named_but_never_pinned() -> None:
    result = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("haiku", CANDIDATE, run("h1", 1.0)),
        costs={"pinned": credits(0.1), "haiku": credits(0.04)},
    )
    assert result.verdict.kind == "pinned_holds" and result.verdict.pin_suggestion is None
    assert result.verdict.summary == (
        "No candidate beats the pinned pinned on answer_correctness (1.00)."
        " haiku ties it at 0.4x the credits per question and ranks above it on what follows."
    )


def test_a_candidate_that_ties_the_pinned_model_but_ranks_below_it_is_not_named() -> None:
    result = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("haiku", CANDIDATE, run("h1", 1.0)),
        costs={"pinned": credits(0.04), "haiku": credits(0.1)},
    )
    assert result.verdict.summary == "No candidate beats the pinned pinned on answer_correctness (1.00)."


def test_a_lead_on_one_run_or_a_partial_run_is_no_clear_winner() -> None:
    once = compared(model("pinned", PINNED, run("p1", 0.0)), model("opus", CANDIDATE, run("o1", 1.0)))
    assert once.verdict.kind == "no_clear_winner" and once.verdict.pin_suggestion is None
    assert once.verdict.summary == (
        "opus leads the pinned pinned on answer_correctness (1.00 vs 0.00) on 1 completed run each at most,"
        " too few to tell a lead from run-to-run noise; compare with --compare-runs 2 or more."
    )
    partial = compared(
        model("pinned", PINNED, run("p1", 0.0), run("p2", 0.0)),
        model("opus", CANDIDATE, run("o1", 1.0), run("o2", 1.0), runs=3),
    )
    assert partial.verdict.kind == "no_clear_winner" and "2 completed runs each" in partial.verdict.summary


def test_a_preview_candidate_never_wins_and_is_listed_as_excluded() -> None:
    result = compared(
        model("pinned", PINNED, run("p1", 0.0), run("p2", 0.0)),
        model("preview", CANDIDATE, run("x1", 1.0), run("x2", 1.0), lifecycle="preview"),
        model("haiku", CANDIDATE, run("h1", 0.5), run("h2", 0.5)),
    )
    assert result.verdict.kind == "winner" and result.verdict.model == "haiku"
    assert result.verdict.excluded == (("preview", "preview"),)
    only = compared(
        model("pinned", PINNED, run("p1", 0.0)), model("preview", CANDIDATE, run("x1", 1.0), lifecycle="internal")
    )
    assert only.verdict.kind == "pinned_holds"


@pytest.mark.parametrize(
    ("pinned", "candidate", "summary"),
    [
        ((), (run("o1", 1.0),), "pinned scored no run, so nothing compares."),
        ((run("p1", 1.0),), (), "every candidate scored no run, so nothing compares."),
    ],
)
def test_a_comparison_without_a_scored_pinned_model_or_candidate_is_incomplete(
    pinned: tuple[EvalRunAttempt, ...], candidate: tuple[EvalRunAttempt, ...], summary: str
) -> None:
    result = compared(model("pinned", PINNED, *pinned), model("opus", CANDIDATE, *candidate, error="commit failed"))
    assert (result.verdict.kind, result.verdict.summary) == ("incomplete", summary)
    assert {item.model: item.status for item in result.models}["opus"] == ("partial" if candidate else "failed")


def test_a_model_that_scored_some_runs_is_partial_and_a_candidate_with_no_score_loses_to_any() -> None:
    result = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 0.0), runs=2),
        model("dead", CANDIDATE, runs=2, error="ADD VERSION failed"),
    )
    assert [(item.model, item.status, item.rank) for item in result.models] == [
        ("pinned", "partial", 1),
        ("opus", "partial", 2),
        ("dead", "failed", 3),
    ]
    assert result.models[2].latency_mean is None and result.models[2].cost.credits_per_question.state == UNAVAILABLE


def test_ties_share_a_rank_and_keep_their_order_and_cost_then_latency_break_them() -> None:
    tied = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("a", CANDIDATE, run("a1", 1.0)),
        model("b", CANDIDATE, run("b1", 1.0)),
    )
    assert [(item.model, item.rank) for item in tied.models] == [("pinned", 1), ("a", 1), ("b", 1)]
    cheaper = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("a", CANDIDATE, run("a1", 1.0)),
        model("b", CANDIDATE, run("b1", 1.0)),
        costs={"a": credits(2.0), "b": credits(1.0)},
    )
    assert [item.model for item in cheaper.models] == ["pinned", "b", "a"]
    faster = compared(
        model("pinned", PINNED, run("p1", 1.0, duration=300)),
        model("a", CANDIDATE, run("a1", 1.0, duration=200)),
    )
    assert [item.rank for item in faster.models] == [2, 1]


def test_each_question_reports_each_models_result_and_what_each_candidate_changed() -> None:
    runs = (
        model("pinned", PINNED, run("p1", 1.0, 0.0, route=("DB.A.X",))),
        model("opus", CANDIDATE, run("o1", 0.0, 1.0, route=())),
        model("gpt", CANDIDATE, run("g1", 1.0)),
    )
    [first, second] = question_reports(runs, METRICS)
    assert (first.id, first.text) == ("q1", "Question 1?")
    assert dict(first.results)["pinned"].route == ("DB.A.X",) and dict(first.results)["opus"].reason == "why 1"
    assert first.changes == (("opus", "breaks"),)
    assert second.changes == (("opus", "fixes"),) and "gpt" not in dict(second.results)


def test_questions_without_a_pinned_result_or_index_change_nothing() -> None:
    unindexed = replace(run("o1", 1.0), rows=(replace(run("o1", 1.0).rows[0], index=None),))
    [question] = question_reports((model("pinned", PINNED), model("opus", CANDIDATE, unindexed)), METRICS)
    assert (question.id, question.changes) == ("q1", ())
    assert question_reports((model("pinned", PINNED, run("p1", 1.0)),), ()) == ()


def test_the_headline_metric_is_the_first_gated_one_else_the_first() -> None:
    assert headline_metric(METRICS) == CORRECT
    assert headline_metric((LOGIC,)) == LOGIC
    assert headline_metric(()) is None


def test_the_nearest_run_skips_a_run_that_scored_none_of_the_metric() -> None:
    blank = replace(run("b1", 1.0), rows=())
    assert nearest_run((blank, run("r2", 1.0)), CORRECT).run_name == "r2"
    assert nearest_run((blank,), None).run_name == "b1"


def test_a_metric_without_a_threshold_passes_only_a_full_score_and_an_unscored_question_fails() -> None:
    lenient = MetricSpec(CORRECT.name, True)
    unscored = replace(
        run("p1", 1.0),
        rows=(replace(run("p1", 1.0).rows[0], metrics=(EvalMetricResult("key1", CORRECT.name, None, False),)),),
    )
    result = compare_eval(
        eval_name="sales",
        kind="agent",
        subject=Subject("sales", "DB.S.SALES", None, "pinned"),
        dataset=Dataset("EVAL", None, 1),
        runs_per_model=1,
        metrics=(lenient, MetricSpec("other", False, ThresholdRange(max=0.2))),
        runs=(model("pinned", PINNED, run("p1", 0.9)), model("opus", CANDIDATE, unscored)),
        costs={},
    )
    assert (result.models[0].metric(CORRECT.name) or result.models[0]).passed == 0  # type: ignore[union-attr]
    [question] = result.questions
    assert dict(question.results)["opus"] == dict(question.results)["opus"].__class__(False, None, None, None)
    assert result.metrics[1].bar == 0.2 and lenient.bar is None


def test_a_comparison_whose_eval_scores_no_metric_is_incomplete() -> None:
    result = compare_eval(
        eval_name="sales",
        kind="agent",
        subject=Subject("sales", "DB.S.SALES", None, "pinned"),
        dataset=Dataset("EVAL", None, 1),
        runs_per_model=1,
        metrics=(),
        runs=(model("pinned", PINNED, run("p1", 1.0)), model("opus", CANDIDATE, run("o1", 1.0))),
        costs={},
    )
    assert result.verdict.kind == "incomplete"


def test_finalizing_again_keeps_one_cost_note_and_drops_it_once_nothing_is_pending() -> None:
    pending = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("o", CANDIDATE, run("o1", 1.0)),
        costs={"o": credits(None, PENDING)},
    )
    again = finalize(pending)
    assert again.notes.count(PENDING_NOTE) == 1
    settled = finalize(replace(again, models=tuple(replace(item, cost=credits(1.0)) for item in again.models)))
    assert PENDING_NOTE not in settled.notes
    assert decide("agent", settled.models, METRICS, subject="sales") == settled.verdict


def test_a_models_lifecycle_reads_its_show_status() -> None:
    assert [
        model_lifecycle(CortexBaseModel("m", status))
        for status in ("GA", "pupr", "PRPR", "INTERNAL", "LEGACY", "EOL", "X")
    ] == [
        "ga",
        "preview",
        "preview",
        "internal",
        "deprecated",
        "deprecated",
        "unknown",
    ]
    assert model_lifecycle(None) == "unknown" and model_lifecycle(CortexBaseModel("m")) == "unknown"
