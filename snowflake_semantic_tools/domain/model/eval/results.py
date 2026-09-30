"""Eval run results, captured baselines, and gate outcomes.

A run makes one or more attempts; each completed attempt yields a score per question and
metric. A baseline records which attempts passed, per question and metric, together with
the fingerprints a later run must match to be compared with it. The gate compares a run
with its baseline and reports the regressions; `EvalGateState` is what persists of that.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EvalMetricResult:
    """One metric's result on one question in one attempt; `score` is None when none was reported."""

    question_key: str
    metric_name: str
    score: float | None
    passed: bool


@dataclass(frozen=True, slots=True)
class EvalResultRow:
    """One question's results in one attempt, one entry per metric."""

    question_key: str
    input_query: str
    metrics: tuple[EvalMetricResult, ...]


@dataclass(frozen=True, slots=True)
class EvalCostSummary:
    """Time and token totals for a record, an attempt or a suite; an unreported value counts as 0."""

    duration_ms: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    llm_call_count: int = 0


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
    """

    run_name: str
    attempt: int
    terminal_status: str
    rows: tuple[EvalResultRow, ...] = ()
    cost: EvalCostSummary = EvalCostSummary()
    retrieval_error: str | None = None
    status_details: tuple[str, ...] = ()
    agent_version: str | None = None


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
