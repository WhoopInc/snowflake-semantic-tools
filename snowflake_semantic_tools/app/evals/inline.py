"""Run evals against agents as they compile now, through temporary agents, with no evaluation run.

`RunInlineEvals` is `sst test --suite evals --unpublished`: each question of each eval's
compiled dataset is asked of its agent's compiled specification, as `sst ask` asks it, as
many at once as the suite's concurrency allows. Then each enabled metric is scored:

- `tool_selection_accuracy` exactly, by the rule `domain.evaluate.tool_selection` documents;
- `answer_correctness`, by SST's own judge prompt, and each custom metric, by its prompt with
  the reply's placeholders filled, each judged by a completion at temperature 0 and marked
  approximate. `answer_correctness` is judged by the eval's `judge_model`, else the agent's
  orchestration model; a custom metric by its own `model`;
- `tool_execution_accuracy` and `logical_consistency` not at all: only Snowflake's evaluation
  service scores them.

Only exact metrics gate, on their thresholds alone. Nothing is published, staged, read from or
written to the eval state table, and no baseline is read, captured or compared.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.app.ask import DEFAULT_RUN_TIMEOUT_SECONDS
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.retrieve import compiled_questions, sum_costs
from snowflake_semantic_tools.app.evals.run import EvalRunResult, EvalSuiteResult, eval_suite_json, suite_concurrency
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.evaluate.judge import (
    ANSWER_CORRECTNESS_SCALE,
    JUDGE_SCHEMA,
    JudgedReply,
    answer_correctness_prompt,
    custom_judge_prompt,
    judged_reason,
    judged_score,
    unfillable_placeholders,
)
from snowflake_semantic_tools.domain.evaluate.scores import score_text, threshold_phrase
from snowflake_semantic_tools.domain.evaluate.tool_selection import (
    counted_calls,
    declared_tools,
    expected_tools,
    tool_selection_score,
)
from snowflake_semantic_tools.domain.model.eval import (
    EVAL_COMPLETED,
    CustomEvalMetric,
    EvalCostSummary,
    EvalDefaults,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.enrich import CortexPort
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, AgentRunPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool

EXACT_METRICS = frozenset(("tool_selection_accuracy",))
UNAVAILABLE_METRICS = frozenset(("tool_execution_accuracy", "logical_consistency"))
# The name every inline attempt reports in place of an evaluation run's.
INLINE_RUN_NAME = "unpublished"
_EXACT, _JUDGED = "exact", "judged"
# What an inline run never counts: the judge's tokens, which AI_COMPLETE's answer omits, and
# the agent's model calls, which its usage omits (measured on a live account).
_NEVER_COUNTED = ("completion_tokens", "llm_call_count", "prompt_tokens", "total_tokens")
_AGENT_TOKENS = ("total_input_tokens", "total_output_tokens")


class InlineEvalPort(AgentRunPort, CortexPort, Protocol):
    """The Snowflake roles an unpublished eval run uses: running an agent inline, and asking a judge."""


@dataclass(frozen=True, slots=True)
class InlineEvalSetup:
    """One eval to run unpublished, with the compiled agent it runs and where its temporary agents go."""

    compiled: CompiledEval
    agent: CompiledAgent
    scope: SchemaScope


@dataclass(frozen=True, slots=True)
class _Metric:
    """One metric an unpublished run scores, and how.

    Attributes:
        kind: `exact` or `judged`; a metric the run cannot score has no `_Metric`.
        model: The judge model; empty for an exact metric.
        custom: The custom metric judged; None for a system metric.
    """

    name: str
    kind: str
    threshold: ThresholdRange | None
    model: str = ""
    custom: CustomEvalMetric | None = None


@dataclass(frozen=True, slots=True)
class _Plan:
    """What every question of one eval is scored with: its metrics, and the names left unscored."""

    setup: InlineEvalSetup
    metrics: tuple[_Metric, ...]
    unscored: tuple[str, ...]
    declared: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class _Question:
    plan: _Plan
    index: int
    key: str
    input_query: str
    ground_truth: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class InlineEvalOutcome:
    """What an unpublished eval run found, for the CLI to report.

    Attributes:
        suite: Each eval as one attempt, named `unpublished`, holding a row per question.
        passed: Whether nothing reported an error: every question was answered, and every
            exact metric stayed within its threshold.
        data: The run as JSON: the suite's projection, its mode, and which metrics were
            approximated or left unscored.
    """

    diagnostics: DiagnosticBag
    suite: EvalSuiteResult
    passed: bool
    data: Mapping[str, object]


class RunInlineEvals:
    """Run each eval's questions against its agent's compiled specification, and score the replies.

    Args:
        sessions: Where questions asked at once each lease a session, opened from `port`; None
            asks them one at a time on `port`.
        progress: Hears one line per question answered.
        timeout: Seconds each question may run.
    """

    def __init__(
        self,
        port: InlineEvalPort,
        sessions: SessionPool[InlineEvalPort] | None = None,
        *,
        progress: Callable[[str], None] = lambda line: None,
        timeout: int = DEFAULT_RUN_TIMEOUT_SECONDS,
    ) -> None:
        self._port = port
        self._sessions = sessions
        self._progress = progress
        self._timeout = timeout

    def run(
        self,
        setups: Sequence[InlineEvalSetup],
        *,
        defaults: EvalDefaults = EvalDefaults(),
        threads: int = 1,
        fail_fast: bool = False,
        notes: tuple[Diagnostic, ...] = (),
    ) -> InlineEvalOutcome:
        """Ask every eval's questions, score them, and gate each eval on its exact metrics.

        With `fail_fast` the evals run one after another, and the run stops after the first
        one that fails; otherwise every question of every eval shares one fan-out.

        Args:
            notes: What the caller found first, such as each agent's stale dependencies;
                reported ahead of the run's own.

        Raises:
            ValueError: an eval's compiled dataset payload is malformed.

        Diagnostics:
            SST-ASK004, SST-ASK005: once per eval and metric the run cannot score.
            SST-ASK002, SST-ASK006: as `_scored` reports them, per question.
            SST-ASK007: once per eval and exact metric that fell outside its threshold.
        """
        workers = suite_concurrency(tuple(setup.compiled for setup in setups), defaults, threads)
        fanout = Fanout(self._port, self._sessions, workers)
        planned = [_plan(setup, defaults) for setup in setups]
        results: list[tuple[EvalRunResult, _Plan]] = []
        try:
            for batch in [[plan] for plan, _ in planned] if fail_fast else [[plan for plan, _ in planned]]:
                questions = [question for plan in batch for question in _questions(plan)]
                started = time.monotonic()
                answered = fanout.map(self._scored, questions)
                wall_ms = int((time.monotonic() - started) * 1000)
                for plan in batch:
                    result = _eval_result(plan, questions, answered, planned, wall_ms)
                    results.append((result, plan))
                last = results[-1][0]
                if fail_fast and (not last.success or last.diagnostics.has_errors):
                    break
        except KeyboardInterrupt:
            if self._sessions is not None:
                self._sessions.halt("the unpublished eval run was interrupted")
            raise
        suite = EvalSuiteResult(
            tuple(result for result, _ in results),
            DiagnosticBag(tuple(item for result, _ in results for item in result.diagnostics)),
        )
        diagnostics = DiagnosticBag((*notes, *suite.diagnostics))
        return InlineEvalOutcome(diagnostics, suite, not diagnostics.has_errors, _data(suite, results, diagnostics))

    def _scored(self, port: InlineEvalPort, question: _Question) -> tuple[EvalResultRow, tuple[Diagnostic, ...]]:
        """Ask one question and score each metric on the reply; unscored metrics have no score.

        The row keeps what the reply said and did: its answer or error, the tools it called
        against those expected, each judge's reason, and the question's time and tokens.

        A run that fails, or answers with an error, scores nothing. A judge that fails or
        answers no number on its scale leaves its metric unscored on this question.

        Diagnostics:
            SST-ASK002: the agent's run failed, or it answered with an error.
            SST-ASK006: a judge gave no score.
        """
        setup = question.plan.setup
        try:
            reply = port.run_inline(
                setup.scope, setup.agent.name, setup.agent.payload, question.input_query, self._timeout
            )
        except SnowflakePortError as exc:
            reply = AgentReply(error=str(exc))
        self._progress(
            f"{setup.compiled.artifact_key}: question {question.index} {'failed' if reply.error else 'answered'}"
        )
        found: list[Diagnostic] = []
        if reply.error is not None:
            detail = f"question {question.index}: {reply.error}"
            found.append(
                D("SST-ASK002", subject=setup.compiled.artifact_key, artifact=setup.agent.artifact_key, detail=detail)
            )
        judged = _judged_reply(question, reply)
        metrics = []
        for metric in question.plan.metrics:
            score, reason = (
                (None, None) if reply.error is not None else self._score(port, metric, question, reply, judged, found)
            )
            passed = _within(metric.threshold, score)
            metrics.append(EvalMetricResult(question.key, metric.name, score, passed, reason, metric.threshold))
        metrics.extend(EvalMetricResult(question.key, name, None, False) for name in question.plan.unscored)
        row = EvalResultRow(
            question.key,
            question.input_query,
            tuple(sorted(metrics, key=lambda item: item.metric_name)),
            index=question.index,
            answer=None if reply.error is not None else reply.answer,
            error=reply.error,
            expected_tools=expected_tools(question.ground_truth),
            called_tools=None
            if reply.error is not None
            else counted_calls((use.name for use in reply.tool_uses), question.plan.declared),
            cost=_reply_cost(reply),
        )
        return row, tuple(found)

    def _score(
        self,
        port: InlineEvalPort,
        metric: _Metric,
        question: _Question,
        reply: AgentReply,
        judged: JudgedReply,
        found: list[Diagnostic],
    ) -> tuple[float | None, str | None]:
        """Score one metric on an answered question, with the judge's reason.

        A judge's failure is added to `found`. An exact metric gives no reason: the row's tools
        say why.
        """
        if metric.kind == _EXACT:
            called = (use.name for use in reply.tool_uses)
            return tool_selection_score(called, question.plan.declared, expected_tools(question.ground_truth)), None
        custom = metric.custom
        if custom is None:
            low, high = ANSWER_CORRECTNESS_SCALE
            prompt = answer_correctness_prompt(judged)
        else:
            assert custom.score_ranges is not None and custom.prompt is not None  # `_plan` judges no other
            low, high = custom.score_ranges.min_score[0], custom.score_ranges.max_score[1]
            prompt = custom_judge_prompt(custom.prompt, judged, low, high)
        if prompt is None:
            return None, None
        try:
            answer = port.complete_json(metric.model, prompt, JUDGE_SCHEMA)
        except SnowflakePortError as exc:
            found.append(_no_score(question, metric, str(exc)))
            return None, None
        score = judged_score(answer, low, high)
        if score is None:
            found.append(_no_score(question, metric, f"it answered no number from {low:g} to {high:g}"))
        return score, judged_reason(answer)


def _plan(setup: InlineEvalSetup, defaults: EvalDefaults) -> tuple[_Plan, tuple[Diagnostic, ...]]:
    """Decide how each metric the eval enables is scored, and report each one that cannot be.

    Diagnostics:
        SST-ASK004: a judged metric that cannot be judged here, naming why.
        SST-ASK005: a system metric only the evaluation service scores.
    """
    compiled = setup.compiled
    key, name_of = compiled.artifact_key, compiled.name
    metrics: list[_Metric] = []
    unscored: list[str] = []
    found: list[Diagnostic] = []
    judge = (
        compiled.resolved.options.judge_model or defaults.judge_model or setup.agent.resolved.model.orchestration_model
    )
    for system in compiled.resolved.config.system_metrics:
        name = system.name
        if name is None:
            continue
        if name in EXACT_METRICS:
            metrics.append(_Metric(name, _EXACT, system.threshold))
        elif name in UNAVAILABLE_METRICS:
            unscored.append(name)
            found.append(D("SST-ASK005", subject=key, artifact=name_of, name=name))
        elif judge and judge != "auto":
            metrics.append(_Metric(name, _JUDGED, system.threshold, judge))
        else:
            unscored.append(name)
            reason = "no judge model: set the eval's judge_model, or give the agent an orchestration model"
            found.append(D("SST-ASK004", subject=key, artifact=name_of, name=name, reason=reason))
    for custom in compiled.resolved.custom_metrics:
        if not custom.enabled:
            continue
        refused = _unjudgeable(custom)
        if refused is None:
            metrics.append(_Metric(custom.name, _JUDGED, custom.threshold_default, str(custom.model), custom))
        else:
            unscored.append(custom.name)
            found.append(D("SST-ASK004", subject=key, artifact=name_of, name=custom.name, reason=refused))
    declared = declared_tools(json.loads(setup.agent.payload))
    return _Plan(setup, tuple(metrics), tuple(unscored), declared), tuple(found)


def _unjudgeable(custom: CustomEvalMetric) -> str | None:
    """Say why a custom metric cannot be judged from one reply; None when it can."""
    if custom.prompt is None or custom.score_ranges is None:
        return "it has no prompt or no score bands"
    if not custom.model or custom.model == "auto":
        return "it names no judge model"
    missing = unfillable_placeholders(custom.prompt)
    if missing:
        return f"its prompt uses {', '.join(missing)}, which only a published run's trace fills"
    return None


def _questions(plan: _Plan) -> list[_Question]:
    """Return the eval's questions, numbered from 1 in dataset order."""
    return [
        _Question(plan, index, key, input_query, ground_truth)
        for index, (key, input_query, ground_truth) in enumerate(compiled_questions(plan.setup.compiled), start=1)
    ]


def _eval_result(
    plan: _Plan,
    questions: list[_Question],
    answered: tuple[tuple[EvalResultRow, tuple[Diagnostic, ...]], ...],
    planned: list[tuple[_Plan, tuple[Diagnostic, ...]]],
    wall_ms: int,
) -> EvalRunResult:
    """Collect one eval's questions into one attempt, then gate it on its exact metrics.

    The attempt is COMPLETED when every question was answered, PARTIALLY_COMPLETED when some
    were, and FAILED when none was. The eval is accepted when the attempt completed. Its cost
    adds its questions'; `wall_ms` is how long the batch it ran in took.

    Diagnostics:
        SST-ASK007: an exact metric scored some question outside its threshold.
    """
    mine = [outcome for question, outcome in zip(questions, answered, strict=True) if question.plan is plan]
    setup_notes = next(notes for candidate, notes in planned if candidate is plan)
    rows = tuple(row for row, _ in mine)
    failed = sum(any(item.code == "SST-ASK002" for item in notes) for _, notes in mine)
    status = EVAL_COMPLETED if not failed else "FAILED" if failed == len(mine) else "PARTIALLY_COMPLETED"
    cost = sum_costs(tuple(row.cost for row in rows if row.cost is not None))
    attempt = EvalRunAttempt(INLINE_RUN_NAME, 1, status, rows, cost, wall_ms=wall_ms)
    gated = tuple(_gate(plan, rows))
    diagnostics = DiagnosticBag((*setup_notes, *(item for _, notes in mine for item in notes), *gated))
    return EvalRunResult(plan.setup.compiled.artifact_key, (attempt,), diagnostics, status == EVAL_COMPLETED)


def _gate(plan: _Plan, rows: tuple[EvalResultRow, ...]) -> list[Diagnostic]:
    """Report each exact metric with a threshold that some question scored outside it.

    A question the metric left unscored, having no expectation, does not count.

    Diagnostics:
        SST-ASK007: once per such metric, naming each question by number with its score and
            the tools it was expected to call and called.
    """
    found = []
    compiled = plan.setup.compiled
    for metric in plan.metrics:
        phrase = threshold_phrase(metric.threshold)
        if metric.kind != _EXACT or phrase is None:
            continue
        outside = [
            (row, result)
            for row in rows
            for result in row.metrics
            if result.metric_name == metric.name and result.score is not None and not result.passed
        ]
        if outside:
            found.append(
                D(
                    "SST-ASK007",
                    subject=compiled.artifact_key,
                    artifact=compiled.name,
                    name=metric.name,
                    expected=phrase,
                    count=len(outside),
                    total=len(rows),
                    detail="; ".join(_short(row, result.score) for row, result in outside),
                )
            )
    return found


def _short(row: EvalResultRow, score: float | None) -> str:
    """Say how one question fell short: its number, its score, and the tools behind it."""
    return (
        f"question {row.index} scored {score_text(score)} "
        f"(expected: {_tool_list(row.expected_tools)}; called: {_tool_list(row.called_tools)})"
    )


def _tool_list(tools: tuple[str, ...] | None) -> str:
    return "none" if not tools else ", ".join(tools)


def _reply_cost(reply: AgentReply) -> EvalCostSummary:
    """One question's cost: the agent's time and tokens, and which counts it never had."""
    if reply.tokens is None:
        return EvalCostSummary(
            duration_ms=reply.duration_ms, unmeasured=tuple(sorted((*_NEVER_COUNTED, *_AGENT_TOKENS)))
        )
    tokens_in, tokens_out = reply.tokens
    return EvalCostSummary(
        duration_ms=reply.duration_ms,
        total_input_tokens=tokens_in,
        total_output_tokens=tokens_out,
        unmeasured=_NEVER_COUNTED,
    )


def _judged_reply(question: _Question, reply: AgentReply) -> JudgedReply:
    tool_info = json.dumps(
        {
            "tool_uses": [{"name": use.name, "type": use.type, "input": dict(use.input)} for use in reply.tool_uses],
            "tool_results": [{"name": result.name, "status": result.status} for result in reply.tool_results],
        },
        sort_keys=True,
        default=str,
    )
    return JudgedReply(
        question.input_query, reply.answer, question.ground_truth, tool_info, reply.duration_ms, reply.error
    )


def _no_score(question: _Question, metric: _Metric, detail: str) -> Diagnostic:
    compiled = question.plan.setup.compiled
    return D(
        "SST-ASK006",
        subject=compiled.artifact_key,
        artifact=compiled.name,
        name=metric.name,
        index=question.index,
        detail=detail,
    )


def _within(threshold: ThresholdRange | None, score: float | None) -> bool:
    """Report whether a score passed: scored, and inside the threshold when the metric has one."""
    if score is None:
        return False
    if threshold is None:
        return True
    return (threshold.min is None or score >= threshold.min) and (threshold.max is None or score <= threshold.max)


def _data(
    suite: EvalSuiteResult, results: list[tuple[EvalRunResult, _Plan]], diagnostics: DiagnosticBag
) -> dict[str, object]:
    """Project the run for the CLI: the suite's projection, with this mode's verdict and metric kinds."""
    approximated = sorted({metric.name for _, plan in results for metric in plan.metrics if metric.kind == _JUDGED})
    unscored = sorted({name for _, plan in results for name in plan.unscored})
    return {
        "suite": "evals",
        **eval_suite_json(suite, frozenset((*approximated, *unscored))),
        "mode": "unpublished",
        "approximated_metrics": approximated,
        "unscored_metrics": unscored,
        "gate_verdict": "failed" if diagnostics.has_errors else "passed",
    }
