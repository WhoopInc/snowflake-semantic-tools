"""Hand-built runs of a model comparison, and the comparison `compare_eval` builds from them.

`run` is one scored run of a two-question eval scored on `answer_correctness` (gated, passing at
0.5) and `logical_consistency`; `model` gathers a model's runs, `credits` prices them, and
`compared` builds the `sales` eval's comparison, noting what was held fixed.
"""

from __future__ import annotations

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
from snowflake_semantic_tools.domain.model.eval import (
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    ThresholdRange,
)

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
    alias = None if role == PINNED else f"EVAL_{name.upper()}"
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
