"""A compiled eval and the Snowflake double its runs poll, for the eval compile, run, gate, and lifecycle tests.

`resolved_eval` is one agent with one question, one system metric, and one custom metric;
`compile_eval` compiles it against `DB.S.SALES_AGENT`. `EvalSnowflake` is the Snowflake fake
with every query scripted: it fails a test on any query it was not given an answer for.
`gated_eval`, `attempt` and `run_result` build what the gate judges: a blocking eval with two
baseline runs, one completed attempt's pass flags, and a run of attempts. `profile_eval_of`,
`profile_status`, `profile_results` and `spans` do the same for a run of the CoWork profile
`concierge` (a Snowflake private preview), whose router spans `spans` scripts.
"""

from __future__ import annotations

import json
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval, CompileEvals
from snowflake_semantic_tools.app.evals.retrieve import compiled_questions
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
    SubjectTarget,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult, RenderedArtifact
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.eval_inputs import concierge_eval, settings_of, settled
from tests.helpers.snowflake_fake import FakeSnowflake, Sent

ORIGIN = Origin("agent.yml", 1, 1)


def eval_agent(resolved: ResolvedEval) -> AgentModel:
    """The agent an agent's eval evaluates; fails the test for a CoWork profile's eval."""
    agent = resolved.agent
    assert agent is not None, f"{resolved.key} evaluates no agent"
    return agent


def resolved_eval() -> ResolvedEval:
    agent = AgentModel("sales_agent", ORIGIN, ("agent.yml",))
    dataset = EvalDataset(
        ORIGIN,
        "agents/sales/evals/dataset.yml",
        None,
        (EvalQuestion(ORIGIN, "Question", EvalGroundTruth(ORIGIN, (), "Answer")),),
    )
    config = EvalConfig(
        ORIGIN,
        "agents/sales/evals/config.yml",
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
    return ResolvedEval(agent, dataset, config, (metric,), settings_of({"agent_version": "committed"}))


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


# Where `compile_profile_eval` publishes the CoWork profile `concierge` (a Snowflake private
# preview) by default: a rehearsal schema, with its two subagents' qualified names there.
REHEARSAL = QualifiedName.parse("DB.PROFILES.CONCIERGE")
CONCIERGE_SUBAGENTS = MappingProxyType(
    {
        "agent:sales": QualifiedName.parse("DB.AGENTS.SALES"),
        "tool:partner_agent": QualifiedName.parse("PARTNER.AGENTS.DELIVERY"),
    }
)


def compile_profile_eval(resolved: ResolvedEval, target: QualifiedName = REHEARSAL) -> CompileResult:
    """Compile one CoWork profile's eval, its profile published at `target`, stamped as `compile_eval`."""
    catalog = EvalCatalog((resolved,), resolved.custom_metrics, diagnostics=DiagnosticBag())
    result = CompileEvals(
        catalog,
        agent_targets={},
        profile_targets={resolved.name.casefold(): SubjectTarget(target, CONCIERGE_SUBAGENTS)},
    ).run_result()
    stamped = tuple(
        replace(item, git_sha=GIT_SHA) if isinstance(item, CompiledEval) else item for item in result.compiled
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
    gated = replace(config, system_metrics=(replace(config.system_metrics[0], gate=True),))
    return replace(
        compiled,
        resolved=settled(replace(compiled.resolved, config=gated), baseline_runs=2, eval_tier="blocking"),
    )


def attempt(
    name: str,
    *values: tuple[str, str, bool],
    agent_version: str = "VERSION$1",
    agent_spec_fingerprint: str | None = None,
) -> EvalRunAttempt:
    """One completed attempt, from `(question, metric, passed)` flags, on `agent_version`."""
    rows: dict[str, list[EvalMetricResult]] = {}
    for question, metric, passed in values:
        rows.setdefault(question, []).append(EvalMetricResult(question, metric, 1.0 if passed else 0.0, passed))
    return EvalRunAttempt(
        name,
        1,
        "COMPLETED",
        tuple(EvalResultRow(question, "", tuple(metrics)) for question, metrics in sorted(rows.items())),
        agent_version=agent_version,
        agent_spec_fingerprint=agent_spec_fingerprint,
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
    """The fake with every query scripted, which runs START and CANCEL on the eval's scoped session.

    Each query takes the next of `results` (an exception raises), and a query with none left
    fails the test. A START is a scoped call that writes: it takes the next of
    `execute_results`, is logged as a script `("IN <scope>", <statement>)`, and raises when
    that result failed, as `query_in_context` raises for a refused statement. A CANCEL is
    logged the same way and takes the next of `cancel_results`, succeeding when none is left.
    """

    def __init__(self, results: list[QueryResult | Exception], **world: Any) -> None:
        super().__init__(query_results=tuple(results), **world)
        self.answer_unscripted = False
        self.cancel_results: list[Exception | None] = []
        # The agent `compile_eval` targets has one committed version, as has the CoWork profile
        # `compile_profile_eval` targets by default.
        self.agent_versions.setdefault(("DB.S.SALES_AGENT", "committed"), "VERSION$1")
        self.agent_versions.setdefault((REHEARSAL.sql, "committed"), "VERSION$1")

    def query_in_context(
        self, scope: SchemaScope, sql: Sql, params: object = None, *, timeout_seconds: int | None = None
    ) -> QueryResult:
        if "EXECUTE_AI_EVALUATION('CANCEL'" in str(sql):
            self.log.append(Sent("script", (f"IN {scope.sql}", str(sql))))
            failure = self.cancel_results.pop(0) if self.cancel_results else None
            if failure is not None:
                raise failure
            return QueryResult()
        if "EXECUTE_AI_EVALUATION('START'" not in str(sql):
            return super().query_in_context(scope, sql, params, timeout_seconds=timeout_seconds)
        self.log.append(Sent("script", (f"IN {scope.sql}", str(sql))))
        result = self.execute_results.pop(0) if self.execute_results else ExecResult(True)
        if not result.ok:
            raise SnowflakePortError(result.error.message if result.error else "start failed")
        return QueryResult()


def status_result(
    status: str, run_name: str = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z", agent: str = "SALES_AGENT"
) -> QueryResult:
    return QueryResult(STATUS_COLUMNS, ((run_name, agent, "CORTEX AGENT", status, []),))


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


# A run of the CoWork profile `concierge` (a Snowflake private preview), as `profile_eval_of`
# compiles it and `EvalRunOptions("abcdef0", timestamp="20260928T010203Z")` names its first
# attempt; `profile_status`, `profile_results` and `spans` answer its reads.
PROFILE_RUN = "EVAL_CONCIERGE_abcdef0_ci_20260928T010203Z"
ROUTE_COLUMNS = ("RECORD_ID", "SPAN_TYPE", "SUBAGENT")


def profile_eval_of(resolved: ResolvedEval | None = None) -> CompiledEval:
    """Compile one CoWork profile's eval, `concierge_eval()` unless given, at `REHEARSAL`."""
    compiled = compile_profile_eval(resolved or concierge_eval()).compiled[0]
    assert isinstance(compiled, CompiledEval)
    return compiled


def profile_status(status: str, run_name: str = PROFILE_RUN) -> QueryResult:
    return status_result(status, run_name, "CONCIERGE")


def profile_results(compiled: CompiledEval, score: float = 1.0) -> QueryResult:
    """One `answer_correctness` result per question of the compiled profile eval, record `r<n>`."""
    rows = tuple(
        result_row(
            score=score,
            record_id=f"r{index}",
            request_id=f"q{index}",
            input_query=query,
            ground_truth=dict(truth),
        ).rows[0]
        for index, (_, query, truth) in enumerate(compiled_questions(compiled), start=1)
    )
    return QueryResult(RESULT_COLUMNS, rows)


def spans(*records: tuple[str, ...]) -> QueryResult:
    """The router span rows of a run: per record `(record_id, *subagents)`, its root, then each route."""
    rows: list[tuple[object, ...]] = []
    for record_id, *routed in records:
        rows.append((record_id, "record_root", None))
        rows.extend((record_id, None, subagent) for subagent in routed)
    return QueryResult(ROUTE_COLUMNS, tuple(rows))
