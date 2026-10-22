"""Hand-built runs of a model comparison, and the comparison `compare_eval` builds from them.

`run` is one scored run of a two-question eval scored on `answer_correctness` (gated, passing at
0.5) and `logical_consistency`; `model` gathers a model's runs, `credits` prices them, and
`compared` builds the `sales` eval's comparison, noting what was held fixed. `jaffle_outcome` is
a whole comparison of the reference project's agent, as the command line reports it.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.compare import CompareOutcome
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.evaluate.compare import (
    KNOWN,
    PINNED,
    Amount,
    Dataset,
    EvalComparison,
    MetricSpec,
    ModelCost,
    ModelRuns,
    Subject,
    compare_eval,
)
from snowflake_semantic_tools.domain.evaluate.compare_json import CompareReport
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from snowflake_semantic_tools.domain.model.eval import (
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.lifecycle import eval_model_alias

CORRECT = MetricSpec("answer_correctness", True, ThresholdRange(min=0.5))
LOGIC = MetricSpec("logical_consistency", False)
METRICS = (CORRECT, LOGIC)


def run(
    name: str, *scores: float, logic: float = 1.0, duration: int = 100, route: tuple[str, ...] | None = None
) -> EvalRunAttempt:
    """One scored run: question n scores `scores[n]` on correctness, `logic` on consistency."""
    rows = tuple(
        EvalResultRow(
            f"key{index}",
            f"Question {index}?",
            (
                EvalMetricResult(f"key{index}", CORRECT.name, score, score >= 0.5, explanation=f"why {index}"),
                EvalMetricResult(f"key{index}", LOGIC.name, logic, True),
            ),
            index=index,
            cost=EvalCostSummary(duration_ms=duration * index),
            request_ids=(f"{name}-r{index}",),
            routed_subagents=route,
        )
        for index, score in enumerate(scores, start=1)
    )
    cost = EvalCostSummary(total_tokens=10, total_input_tokens=100, total_output_tokens=20, llm_call_count=3)
    return EvalRunAttempt(name, 1, "COMPLETED", rows, cost)


def model(
    name: str, role: str, *scored: EvalRunAttempt, runs: int = 2, lifecycle: str = "ga", error: str | None = None
) -> ModelRuns:
    alias = None if role == PINNED else eval_model_alias(name)
    return ModelRuns(
        name, role, lifecycle, "VERSION$4", alias, tuple(item.run_name for item in scored), scored, runs, error
    )


def credits(value: float | None, state: str = KNOWN) -> ModelCost:
    return ModelCost(Amount(state, value), Amount(KNOWN, 0.0), Amount(KNOWN, 7.0))


def compared(*runs: ModelRuns, kind: str = "agent", costs: dict[str, ModelCost] | None = None) -> EvalComparison:
    return compare_eval(
        eval_name="sales",
        kind=kind,
        subject=Subject("sales", "DB.S.SALES", "VERSION$4", "pinned"),
        dataset=Dataset("EVAL_SALES", "SST_ABC", 2),
        runs_per_model=2,
        metrics=METRICS,
        runs=runs,
        costs=costs or {},
        notes=("Held fixed.",),
    )


def compared_report(*runs: ModelRuns) -> CompareReport:
    """A whole report of the `sales` comparison, its pinned model and `opus` by default."""
    evaluation = compared(
        *(runs or (model("pinned", PINNED, run("p1", 1.0)), model("opus", "candidate", run("o1", 1.0))))
    )
    return CompareReport("2026-10-22T14:03:11Z", None, "dev", "c7f2a19e", "703f69a", (evaluation,))


def jaffle_outcome() -> CompareOutcome:
    """A comparison of the reference project's agent eval: opus wins, a preview model is excluded.

    The pinned model's runs spread 0.50 to 0.75, opus's 1.00, the preview's 1.00; opus's cost is
    pending, so the report notes it and the verdict says so.
    """

    def named(name: str, *scores: float) -> EvalRunAttempt:
        attempt = run(name, *scores)
        rows = tuple(replace(row, input_query=f"Which jaffles sold most in week {row.index}?") for row in attempt.rows)
        return replace(attempt, rows=rows)

    pinned = replace(
        model("claude-sonnet-4-6", PINNED, named("P1", 1.0, 0.0), named("P2", 1.0, 0.5)),
        version="VERSION$4",
    )
    opus = replace(
        model("claude-opus-4-8", "candidate", named("O1", 1.0, 1.0), named("O2", 1.0, 1.0)), version="VERSION$6"
    )
    preview = replace(
        model("openai-gpt-5", "candidate", named("G1", 1.0, 1.0), lifecycle="preview"), version="VERSION$7", runs=2
    )
    failed = replace(
        model("claude-haiku-4-5", "candidate", error="ADD VERSION failed: Insufficient privileges"),
        version=None,
        alias=None,
    )
    evaluation = compare_eval(
        eval_name="jaffle_analytics_agent",
        kind="agent",
        subject=Subject(
            "jaffle_analytics_agent", "JAFFLE.AGENTS.JAFFLE_ANALYTICS_AGENT", "VERSION$4", "claude-sonnet-4-6"
        ),
        dataset=Dataset("EVAL_JAFFLE_ANALYTICS_AGENT_1A2B3C4", "SST_1A2B3C4D5E6F", 2),
        runs_per_model=2,
        metrics=METRICS,
        runs=(pinned, opus, preview, failed),
        costs={
            "claude-sonnet-4-6": ModelCost(Amount(KNOWN, 0.4), Amount(KNOWN, 0.02), Amount(KNOWN, 4100.0)),
            "claude-opus-4-8": ModelCost(Amount("pending"), Amount("pending"), Amount(KNOWN, 4300.0)),
            "openai-gpt-5": ModelCost(Amount(KNOWN, 0.3), Amount(KNOWN, 0.0), Amount(KNOWN, 3900.0)),
        },
        notes=(
            "Held fixed: dataset EVAL_JAFFLE_ANALYTICS_AGENT_1A2B3C4 SST_1A2B3C4D5E6F, metric versions, judge "
            "claude-sonnet-4-6, everything in the specification but models.orchestration.",
            "Committed VERSION$5 (hold), VERSION$6 (EVAL_CLAUDE_OPUS_4_8), VERSION$7 (EVAL_OPENAI_GPT_5), "
            "VERSION$8 (restore); the default and the release's alias stayed on VERSION$4.",
        ),
    )
    report = CompareReport("2026-10-22T14:03:11Z", "2026-10-22T14:03:11Z", "dev", "c7f2a19e", "703f69a", (evaluation,))
    notices = comparison_notices(evaluation, path="sst_target/compare/run/compare.json")
    return CompareOutcome(DiagnosticBag(notices), report, True)
