"""Eval run results, captured baselines, and gate outcomes.

A run makes one or more attempts; each completed attempt yields a score per question and
metric. A baseline records which attempts passed, per question and metric, together with
the fingerprints a later run must match to be compared with it. The gate compares a run
with its baseline and reports the regressions; `EvalGateState` is what persists of that.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.eval.model import ThresholdRange


@dataclass(frozen=True, slots=True)
class EvalMetricResult:
    """One metric's result on one question in one attempt.

    Attributes:
        score: None when none was reported.
        explanation: Why the scorer gave that score, as it said it; None when it gave no reason.
        threshold: The band the score had to fall in to pass; None when the metric has none.
    """

    question_key: str
    metric_name: str
    score: float | None
    passed: bool
    explanation: str | None = None
    threshold: ThresholdRange | None = None


@dataclass(frozen=True, slots=True)
class EvalCostSummary:
    """Time and token totals for a record, an attempt or a suite; an unreported value counts as 0.

    Attributes:
        duration_ms: The agent's time answering; an attempt's or suite's adds its questions' times,
            which ran concurrently, so it exceeds the time the run took.
        prompt_tokens: The judge's prompt tokens, over every metric call.
        completion_tokens: The judge's completion tokens, over every metric call.
        total_tokens: The judge's tokens, over every metric call.
        total_input_tokens: The agent's input tokens.
        total_output_tokens: The agent's output tokens.
        llm_call_count: The agent's model calls.
        unmeasured: The names of the fields above that were not counted, sorted; each is 0
            because nothing counted it, not because nothing was used.
    """

    duration_ms: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    llm_call_count: int = 0
    unmeasured: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EvalResultRow:
    """One question's results in one attempt, one entry per metric.

    Attributes:
        index: The question's 1-based position in the eval's dataset; None when unknown.
        answer: The agent's reply text; None when not read.
        error: Why the agent answered with an error instead of a reply; None when it replied.
        expected_tools: The tools the question's ground truth expects, in its order; `()` expects
            none, and None states no expectation.
        called_tools: The calls of the agent's own tools the reply made, in call order, by name;
            None when not known.
        cost: The question's own time and tokens; None when not read.
    """

    question_key: str
    input_query: str
    metrics: tuple[EvalMetricResult, ...]
    index: int | None = None
    answer: str | None = None
    error: str | None = None
    expected_tools: tuple[str, ...] | None = None
    called_tools: tuple[str, ...] | None = None
    cost: EvalCostSummary | None = None


@dataclass(frozen=True, slots=True)
class EvalRunAttempt:
    """One attempt of an eval run.

    Attributes:
        attempt: 1 for the first attempt; a retry's run name carries its number as `_R<n>`.
        terminal_status: The run's final status, or `START_FAILED` or `STATUS_FAILED` when
            it could not be started or polled.
        rows: The results, read only from a completed attempt; `()` otherwise.
        retrieval_error: Why starting, polling or reading the attempt failed; None when nothing did.
        agent_version: The concrete version the run resolved the agent to; None when unknown.
        wall_ms: How long the attempt took from its first question to its last answer; None
            when not measured.
    """

    run_name: str
    attempt: int
    terminal_status: str
    rows: tuple[EvalResultRow, ...] = ()
    cost: EvalCostSummary = EvalCostSummary()
    retrieval_error: str | None = None
    status_details: tuple[str, ...] = ()
    agent_version: str | None = None
    wall_ms: int | None = None


@dataclass(frozen=True, slots=True)
class EvalBaselineMetric:
    """Whether one metric passed on one question, in each captured attempt.

    Attributes:
        passed_attempts: One flag per captured attempt, in attempt order.
        score_range: The metric's threshold as `(min, max)` at capture; None without one.
    """

    question_key: str
    metric_name: str
    passed_attempts: tuple[bool, ...]
    score_range: tuple[float | None, float | None] | None = None


@dataclass(frozen=True, slots=True)
class EvalBaselineRecord:
    """A captured baseline, and the identity a later run must share to be compared with it.

    Attributes:
        agent_version: The one immutable `VERSION$<n>` every captured attempt ran against.
        metric_versions: `(metric, version)` pairs, sorted; a custom metric's version is
            `custom:<judge model>`.
        run_names: The captured attempts' run names, in attempt order.
        expires_at: From this timestamp on, the baseline no longer gates a run.
        reason: Why it was captured; never empty.
        gate_policy: `(metric, gated)` pairs, sorted.
    """

    eval_key: str
    dataset_fingerprint: str
    config_fingerprint: str
    agent_version: str
    metric_versions: tuple[tuple[str, str], ...]
    metrics: tuple[EvalBaselineMetric, ...]
    run_names: tuple[str, ...]
    captured_at: str
    expires_at: str
    reason: str
    tier: str = "report"
    gate_policy: tuple[tuple[str, bool], ...] = ()


@dataclass(frozen=True, slots=True)
class EvalGateState:
    """The latest gate outcome for one eval, as persisted.

    Attributes:
        unresolved: True when a blocking eval failed its gate or produced no comparison.
        run_names: The run name of every attempt the gated run made, in attempt order.
    """

    eval_key: str
    tier: str
    regression_count: int
    regressions: tuple[EvalRegression, ...]
    unresolved: bool
    evaluated_at: str
    run_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvalRegression:
    """A gated metric that passed a question in every baseline attempt and failed it in a current one."""

    question_key: str
    metric_name: str


@dataclass(frozen=True, slots=True)
class EvalGateVerdict:
    """The gate's decision on one run.

    Attributes:
        passed: False when no comparison was made, whatever the tier, and for a blocking eval
            with a regression.
        reason: Why no comparison was made, such as `baseline_absent`; None when one was.
    """

    tier: str
    regressions: tuple[EvalRegression, ...]
    passed: bool
    reason: str | None = None

    @property
    def regression_count(self) -> int:
        """Count the regressions."""
        return len(self.regressions)
