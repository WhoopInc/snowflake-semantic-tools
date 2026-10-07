"""A compiled eval and the Snowflake double its runs poll, for the eval compile, run, gate, and lifecycle tests.

`resolved_eval` is one agent with one question, one system metric, and one custom metric;
`compile_eval` compiles it against `DB.S.SALES_AGENT`. `EvalSnowflake` is the Snowflake fake
with every query scripted: it fails a test on any query it was not given an answer for.
`gated_eval`, `attempt` and `run_result` build what the gate judges: a blocking eval with two
baseline runs, one completed attempt's pass flags, and a run of attempts.
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval, CompileEvals
from snowflake_semantic_tools.app.evals.run import EvalRunResult
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalConfig,
    EvalDataset,
    EvalDatasetConfig,
    EvalGroundTruth,
    EvalMetricResult,
    EvalQuestion,
    EvalResultRow,
    EvalRunAttempt,
    EvalRunConfig,
    EvalScoreRanges,
    EvalSystemMetric,
    ResolvedEval,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult, RenderedArtifact
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.snowflake_fake import FakeSnowflake, Sent

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


# The commit plan stamps on an eval for publication, which its dataset version records.
GIT_SHA = "abc1234"


def compile_eval(resolved: ResolvedEval | None = None, *, git_sha: str = GIT_SHA) -> CompileResult:
    """Compile one eval, stamped with the commit plan would publish it from."""
    value = resolved or resolved_eval()
    catalog = EvalCatalog((value,), value.custom_metrics, diagnostics=DiagnosticBag())
    result = CompileEvals(
        catalog,
        agent_targets={"sales_agent": QualifiedName.parse("DB.S.SALES_AGENT")},
    ).run_result()
    stamped = tuple(
        replace(item, git_sha=git_sha) if isinstance(item, CompiledEval) else item for item in result.compiled
    )
    return replace(result, compiled=stamped)


def seed_dataset_version(artifact: RenderedArtifact, port: FakeSnowflake) -> None:
    """Record SST's version on the eval's dataset, as a publish that finished leaves it."""
    version = dict(artifact.component_fingerprints)["dataset_version"]
    dataset = next(name for kind, name in artifact.physical_resources if kind == "DATASET")
    port.dataset_version_names[dataset.sql] = [version]


def compiled_eval_of(resolved: ResolvedEval | None = None) -> CompiledEval:
    """Compile one eval, `resolved_eval()` unless given, and return it as a `CompiledEval`."""
    compiled = compile_eval(resolved).compiled[0]
    assert isinstance(compiled, CompiledEval)
    return compiled


def gated_eval(resolved: ResolvedEval | None = None) -> CompiledEval:
    """Compile an eval whose system metric gates, as a blocking eval with two baseline runs."""
    compiled = compiled_eval_of(resolved)
    config = compiled.resolved.config
    run = config.run or EvalRunConfig()
    return replace(
        compiled,
        resolved=replace(
            compiled.resolved,
            config=replace(
                config,
                system_metrics=(replace(config.system_metrics[0], gate=True),),
                run=replace(run, baseline_runs=2, tier="blocking"),
            ),
        ),
    )


def attempt(name: str, *values: tuple[str, str, bool], agent_version: str = "VERSION$1") -> EvalRunAttempt:
    """One completed attempt, from `(question, metric, passed)` flags."""
    rows: dict[str, list[EvalMetricResult]] = {}
    for question, metric, passed in values:
        rows.setdefault(question, []).append(EvalMetricResult(question, metric, 1.0 if passed else 0.0, passed))
    return EvalRunAttempt(
        name,
        1,
        "COMPLETED",
        tuple(EvalResultRow(question, "", tuple(metrics)) for question, metrics in sorted(rows.items())),
        agent_version=agent_version,
    )


def run_result(*attempts: EvalRunAttempt) -> EvalRunResult:
    return EvalRunResult("eval:sales_agent", attempts, DiagnosticBag(), True)


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


class EvalSnowflake(FakeSnowflake):
    """The fake with every query scripted, which runs START on the eval's scoped session.

    Each query takes the next of `results` (an exception raises), and a query with none left
    fails the test. A START is the one scoped call that writes: it takes the next of
    `execute_results`, is logged as a script `("IN <scope>", <statement>)`, and raises when
    that result failed, as `query_in_context` raises for a refused statement.
    """

    def __init__(self, results: list[QueryResult | Exception], **world: Any) -> None:
        super().__init__(query_results=tuple(results), **world)
        self.answer_unscripted = False
        # The agent `compile_eval` targets has one committed version.
        self.agent_versions.setdefault(("DB.S.SALES_AGENT", "committed"), "VERSION$1")

    def query_in_context(
        self, scope: SchemaScope, sql: Sql, params: object = None, *, timeout_seconds: int | None = None
    ) -> QueryResult:
        if "EXECUTE_AI_EVALUATION('START'" not in str(sql):
            return super().query_in_context(scope, sql, params, timeout_seconds=timeout_seconds)
        self.log.append(Sent("script", (f"IN {scope.sql}", str(sql))))
        result = self.execute_results.pop(0) if self.execute_results else ExecResult(True)
        if not result.ok:
            raise SnowflakePortError(result.error.message if result.error else "start failed")
        return QueryResult()


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
