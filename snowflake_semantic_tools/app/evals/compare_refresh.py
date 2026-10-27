"""Refresh a comparison report's cost: read its models' usage again, and rank and decide again.

`--refresh-cost` reads back a `compare.json` this engine wrote, reads the usage of every agent
request and run it names, and finalizes each eval again, so a cost that was pending when the
comparison ended fills in, and the ranking and verdict follow it. It starts no run and commits
nothing; the scores stand as they were read.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from snowflake_semantic_tools.app.evals.compare_cost import CostQuery, read_costs
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.evaluate.compare import ModelCost, finalize
from snowflake_semantic_tools.domain.evaluate.compare_json import CompareReport, report_from_json
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from snowflake_semantic_tools.domain.ports.snowflake.usage import UsagePort

# How far before the report was written its runs may have started: their deadline, with retries.
RUNS_LOOKBACK = timedelta(hours=6)


def refresh_cost(
    port: UsagePort, document: object, *, path: str, now: str
) -> tuple[CompareReport, tuple[Diagnostic, ...]] | Diagnostic:
    """Read a report's usage again and rebuild what follows from its cost.

    Args:
        document: The report as its JSON parsed.
        path: The report's file, which the notices and a refusal name.
        now: When the cost is read, which the report records as `cost_read_at`.

    Returns:
        The refreshed report and what it notices; else why the document cannot be refreshed.

    Diagnostics:
        SST-MAN032: the document is not an `sst.compare/1` report as SST writes it.
        Those of `comparison_notices`, for each eval.
    """
    try:
        report = report_from_json(document)
    except ValueError as exc:
        return D("SST-MAN032", path=path, detail=str(exc))
    queries = tuple(
        CostQuery(evaluation.eval, model.model, model.request_ids, model.run_names)
        for evaluation in report.evals
        for model in evaluation.models
    )
    costs = read_costs(port, queries, started_at=report.generated_at, now=now, lookback=RUNS_LOOKBACK)
    evaluations = tuple(
        finalize(
            replace(
                evaluation,
                models=tuple(
                    replace(model, cost=costs.get((evaluation.eval, model.model), ModelCost()))
                    for model in evaluation.models
                ),
            )
        )
        for evaluation in report.evals
    )
    notices = tuple(notice for item in evaluations for notice in comparison_notices(item, path=path))
    return replace(report, cost_read_at=now, evals=evaluations), notices
