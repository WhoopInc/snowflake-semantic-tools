"""Project an eval suite's results as the JSON `sst test --output json` reports.

Each attempt keeps its run, status and cost, each metric's pass count and average, and every
question: what the agent answered and called, each metric's score, threshold and explanation,
and its own cost. The projection judges nothing; the gate fills in its verdict.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.retrieve import sum_costs
from snowflake_semantic_tools.app.evals.run import EvalSuiteResult
from snowflake_semantic_tools.domain.model.eval import EvalCostSummary, EvalMetricResult, EvalResultRow


def eval_suite_json(result: EvalSuiteResult, lenient: frozenset[str] = frozenset()) -> dict[str, object]:
    """Project a suite result for the CLI: each attempt's summary, cost and questions, and totals.

    A question passed when it answered and every metric that can fail it passed; `lenient`
    names the metrics that cannot, such as those an unpublished run approximates or cannot
    score. The regression count and gate verdict are placeholders the gate fills in when it runs.
    """
    attempts = tuple(attempt for eval_result in result.evals for attempt in eval_result.attempts)
    total_cost = sum_costs(tuple(attempt.cost for attempt in attempts))
    return {
        "evals": [
            {
                "eval_key": eval_result.eval_key,
                "retention": {"class": eval_result.retention},
                "attempts": [
                    {
                        "run_name": attempt.run_name,
                        "attempt": attempt.attempt,
                        "terminal_status": attempt.terminal_status,
                        "status_details": list(attempt.status_details),
                        "retrieval_error": attempt.retrieval_error,
                        "metric_summaries": _metric_summaries(attempt.rows),
                        "cost": _cost_json(attempt.cost),
                        "wall_ms": attempt.wall_ms,
                        "questions": [_question_json(row, lenient) for row in attempt.rows],
                    }
                    for attempt in eval_result.attempts
                ],
            }
            for eval_result in result.evals
        ],
        "attempt_count": len(attempts),
        "cost_totals": _cost_json(total_cost),
        "regression_count": 0,
        "gate_verdict": "not_evaluated",
    }


def empty_eval_suite_json() -> dict[str, object]:
    """Project a suite that ran nothing, in the shape `eval_suite_json` gives a suite that ran."""
    return {
        "evals": [],
        "attempt_count": 0,
        "cost_totals": _cost_json(EvalCostSummary()),
        "regression_count": 0,
        "gate_verdict": "not_evaluated",
    }


def _metric_summaries(rows: tuple[EvalResultRow, ...]) -> list[dict[str, object]]:
    """Summarize each metric across an attempt's questions, by name: records, passes, mean score.

    The mean counts only the questions with a score; None when no question has one.
    """
    values: dict[str, list[EvalMetricResult]] = {}
    for row in rows:
        for metric in row.metrics:
            values.setdefault(metric.metric_name, []).append(metric)
    return [
        {
            "metric_name": name,
            "record_count": len(metrics),
            "passed_count": sum(metric.passed for metric in metrics),
            "average_score": (
                sum(score for metric in metrics if (score := metric.score) is not None)
                / sum(metric.score is not None for metric in metrics)
                if any(metric.score is not None for metric in metrics)
                else None
            ),
        }
        for name, metrics in sorted(values.items())
    ]


def _question_json(row: EvalResultRow, lenient: frozenset[str]) -> dict[str, object]:
    """Project one question: what the agent said and called, each metric's result, and its cost."""
    passed = row.error is None and all(metric.passed for metric in row.metrics if metric.metric_name not in lenient)
    return {
        "index": row.index,
        "question_key": row.question_key,
        "input_query": row.input_query,
        "passed": passed,
        "answer": row.answer,
        "error": row.error,
        "expected_tools": None if row.expected_tools is None else list(row.expected_tools),
        "called_tools": None if row.called_tools is None else list(row.called_tools),
        "metrics": [
            {
                "metric_name": metric.metric_name,
                "score": metric.score,
                "passed": metric.passed,
                "threshold": None
                if metric.threshold is None
                else {"min": metric.threshold.min, "max": metric.threshold.max},
                "explanation": metric.explanation,
            }
            for metric in row.metrics
        ],
        "cost": None if row.cost is None else _cost_json(row.cost),
    }


def _cost_json(value: EvalCostSummary) -> dict[str, object]:
    return {
        "duration_ms": value.duration_ms,
        "prompt_tokens": value.prompt_tokens,
        "completion_tokens": value.completion_tokens,
        "total_tokens": value.total_tokens,
        "total_input_tokens": value.total_input_tokens,
        "total_output_tokens": value.total_output_tokens,
        "llm_call_count": value.llm_call_count,
        "unmeasured": list(value.unmeasured),
        "credits": None,
        "credit_attribution": "not_requested",
    }
