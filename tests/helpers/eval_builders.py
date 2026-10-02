"""A compiled eval and the Snowflake double its runs poll, for the eval compile, run, gate, and lifecycle tests.

`resolved_eval` is one agent with one question, one system metric, and one custom metric;
`compile_eval` compiles it against `DB.S.SALES_AGENT`. `EvalSnowflake` answers queries from a
queue of results, in order, and fails a test on any query it was not given an answer for.
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Sequence

from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval, CompileEvals
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalConfig,
    EvalDataset,
    EvalDatasetConfig,
    EvalGroundTruth,
    EvalQuestion,
    EvalRunConfig,
    EvalScoreRanges,
    EvalSystemMetric,
    ResolvedEval,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.app_ports import InMemorySnowflake

ORIGIN = Origin("agent.yml", 1, 1)


def resolved_eval() -> ResolvedEval:
    agent = AgentModel("sales_agent", ORIGIN, ("agent.yml",))
    dataset = EvalDataset(
        ORIGIN,
        "agents/sales/evals/dataset.yml",
        "sales_agent",
        None,
        (EvalQuestion(ORIGIN, "Question", EvalGroundTruth(ORIGIN, (), "Answer")),),
    )
    config = EvalConfig(
        ORIGIN,
        "agents/sales/evals/config.yml",
        "sales_agent",
        "committed",
        EvalDatasetConfig(
            "auto",
            "EVAL_{{ agent | upper }}_{{ sha7 }}",
            "EVAL_SRC_{{ agent | upper }}_{{ sha7 }}",
        ),
        (EvalSystemMetric(ORIGIN, "answer_correctness", "v3"),),
        ("grounding",),
        EvalRunConfig(label="ci"),
    )
    metric = CustomEvalMetric(
        ORIGIN,
        "eval_metrics/grounding.yml",
        "grounding",
        None,
        "claude-sonnet-4-6",
        EvalScoreRanges((0, 1), (2, 3), (4, 5)),
        "Score from 0 to 5. If tied, choose the lower score.",
    )
    return ResolvedEval(agent, dataset, config, (metric,))


def compile_eval(resolved: ResolvedEval | None = None) -> CompileResult:
    value = resolved or resolved_eval()
    catalog = EvalCatalog((value,), value.custom_metrics, diagnostics=DiagnosticBag())
    return CompileEvals(
        catalog,
        agent_targets={"sales_agent": QualifiedName.parse("DB.S.SALES_AGENT")},
    ).run_result()


def compiled_eval_of(resolved: ResolvedEval | None = None) -> CompiledEval:
    """Compile one eval, `resolved_eval()` unless given, and return it as a `CompiledEval`."""
    compiled = compile_eval(resolved).compiled[0]
    assert isinstance(compiled, CompiledEval)
    return compiled


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

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        self.scripts.append(tuple(str(statement) for statement in statements))
        return self.start_results.popleft() if self.start_results else ExecResult(True)

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        self.queries.append((str(sql), params))
        if not self.results:
            raise AssertionError(f"unexpected query: {sql}")
        result = self.results.popleft()
        if isinstance(result, Exception):
            raise result
        return result

    def query_in_context(self, scope: SchemaScope, sql: Sql, params: object = None) -> QueryResult:
        del scope
        return self.query(sql, params)


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
