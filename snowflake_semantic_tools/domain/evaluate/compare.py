"""The model comparison report: what each model scored, what it cost, how they rank, and the verdict.

Everything here is pure. The app gathers each model's scored runs and its cost, and
`compare_eval` turns them into one eval's comparison, the `sst.compare/1` shape that
`domain.evaluate.compare_json` writes and reads (docs/design/compare-report-schema.md).
`finalize` ranks the models and decides the verdict; refreshing a report's cost calls it again.

A metric's mean is over every scored question of every run, its spread the lowest and highest
per-run mean, and its pass count from the run nearest the mean. Models rank by their gated metric
means in metric order, then their ungated ones, higher first, then credits per question and p95
latency, lower first; a value that is not known sorts last, and models whose keys are equal share
a rank. A candidate wins only when it beats the pinned model on the first gated metric (else the
first metric) with its lowest run above the pinned model's highest, each of the two completed
at least `MIN_WINNER_RUNS` runs -- one run has no spread, so a lead on one cannot be told from
run-to-run noise -- and it is generally available: a preview, internal or deprecated model is
excluded from the verdict.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from snowflake_semantic_tools.domain.model.eval import EvalResultRow, EvalRunAttempt, ThresholdRange
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel

PINNED = "pinned"
CANDIDATE = "candidate"
KNOWN = "known"
PENDING = "pending"
UNAVAILABLE = "unavailable"
# The fewest runs the pinned model and a candidate must each complete for the candidate to win.
MIN_WINNER_RUNS = 2
# The lifecycles a verdict never names a winner from.
EXCLUDED_LIFECYCLES = frozenset({"preview", "internal", "deprecated"})
_LIFECYCLE = {"GA": "ga", "PUPR": "preview", "PRPR": "preview", "INTERNAL": "internal"}
_LIFECYCLE.update({"LEGACY": "deprecated", "EOL": "deprecated"})
# The note a report carries while some usage has not landed; a refresh drops it once none is pending.
PENDING_NOTE = "Usage views lag up to an hour; values marked pending fill in with --refresh-cost."


@dataclass(frozen=True, slots=True)
class Amount:
    """One cost value and whether it is known.

    Attributes:
        state: `known`, `pending` (the usage has not landed yet), or `unavailable`.
        value: Set only when `known`.
        reason: Why it is unavailable, for the notice; never written to the report.
    """

    state: str
    value: float | None = None
    reason: str | None = None


NOT_READ = Amount(UNAVAILABLE, reason="no usage was read")


@dataclass(frozen=True, slots=True)
class ModelCost:
    """What one model's runs cost: the agent's credits, its SQL's, and the judges' tokens.

    Attributes:
        credits_per_question: The agent's credits, with its SQL's once they are known, per agent
            request the runs made; `finalize` computes it.
    """

    agent_credits: Amount = NOT_READ
    sql_credits: Amount = NOT_READ
    judge_tokens: Amount = NOT_READ
    credits_per_question: Amount = NOT_READ


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """One metric an eval scores, in the order its config lists it.

    Attributes:
        threshold: The pass band; None when the metric has none, and a question passes on 1.0.
    """

    name: str
    gated: bool
    threshold: ThresholdRange | None = None

    @property
    def bar(self) -> float | None:
        """The threshold the report prints: its lower bound, else its upper; None for none."""
        if self.threshold is None:
            return None
        return self.threshold.min if self.threshold.min is not None else self.threshold.max


@dataclass(frozen=True, slots=True)
class ModelRuns:
    """One model's runs of one eval, as the app gathered them.

    Attributes:
        version: The committed version the runs evaluated; None when none was committed.
        alias: The `EVAL_` alias a candidate's version carries; None for the pinned model.
        scored: The attempt of each run that completed and was read, in run order.
        runs: The runs asked for; fewer `scored` makes the model partial, none failed.
        error: Why a run, or the candidate's commit, failed; None when nothing did.
    """

    model: str
    role: str
    lifecycle: str
    version: str | None
    alias: str | None
    run_names: tuple[str, ...]
    scored: tuple[EvalRunAttempt, ...]
    runs: int
    error: str | None = None


@dataclass(frozen=True, slots=True)
class MetricSummary:
    """One metric across one model's runs: mean, spread, and the pass count of the run nearest the mean."""

    mean: float | None
    min: float | None
    max: float | None
    passed: int
    total: int


@dataclass(frozen=True, slots=True)
class ModelReport:
    """One model's line of the comparison, as the report writes it.

    Attributes:
        rank: 1 for the best; models whose ranking keys are equal share one. 0 until ranked.
        metrics: `(metric, summary)` in metric order.
        latency_mean, latency_p95: Over every scored question's duration, in milliseconds.
        request_ids: Every agent request the runs made, one per question answered, sorted.
    """

    model: str
    role: str
    version: str | None
    alias: str | None
    lifecycle: str
    status: str
    error: str | None
    rank: int
    run_names: tuple[str, ...]
    metrics: tuple[tuple[str, MetricSummary], ...]
    latency_mean: int | None
    latency_p95: int | None
    agent_input: int
    agent_output: int
    llm_calls: int
    judge: int
    cost: ModelCost
    request_ids: tuple[str, ...]

    def metric(self, name: str) -> MetricSummary | None:
        """Return the summary of one metric; None when the model scored none of it."""
        return next((summary for metric, summary in self.metrics if metric == name), None)


@dataclass(frozen=True, slots=True)
class QuestionResult:
    """One model's answer to one question, scored on the first gated metric, else the first.

    Attributes:
        route: For a CoWork profile, the subagents its router chose, `()` for a decline; None for an agent.
    """

    passed: bool
    score: float | None
    reason: str | None
    route: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class QuestionReport:
    """One question, each model's result on it, and how each candidate changed it against the pinned model.

    Attributes:
        changes: `(candidate, fixes|breaks)` for each candidate whose result differs; none unchanged.
    """

    id: str
    text: str
    results: tuple[tuple[str, QuestionResult], ...]
    changes: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class Verdict:
    """Which model the comparison found best, and the config line to review when one won.

    Attributes:
        kind: `winner`, `pinned_holds`, `no_clear_winner`, or `incomplete`.
        excluded: `(model, lifecycle)` of each candidate no verdict names, in model order.
    """

    kind: str
    model: str | None
    summary: str
    pin_suggestion: str | None
    excluded: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class Subject:
    """The published agent or CoWork profile an eval compares models on."""

    name: str
    qualified_name: str
    released_version: str | None
    pinned_model: str


@dataclass(frozen=True, slots=True)
class Dataset:
    """The eval's published dataset: its name, the version SST added, and its question count.

    Attributes:
        version: None for a dataset SST does not mint (`dataset.mint: never`).
    """

    name: str
    version: str | None
    questions: int


@dataclass(frozen=True, slots=True)
class EvalComparison:
    """One eval's comparison, every field the report writes for it.

    Attributes:
        kind: `agent` or `cowork_profile`.
        models: The pinned model first, then the candidates in rank order, once `finalize` ran.
        notes: What was held fixed and what to know reading it, in display order.
    """

    eval: str
    kind: str
    subject: Subject
    dataset: Dataset
    runs_per_model: int
    metrics: tuple[MetricSpec, ...]
    models: tuple[ModelReport, ...]
    questions: tuple[QuestionReport, ...]
    verdict: Verdict
    notes: tuple[str, ...]


def compare_eval(
    *,
    eval_name: str,
    kind: str,
    subject: Subject,
    dataset: Dataset,
    runs_per_model: int,
    metrics: tuple[MetricSpec, ...],
    runs: Sequence[ModelRuns],
    costs: Mapping[str, ModelCost],
    notes: tuple[str, ...] = (),
) -> EvalComparison:
    """Build one eval's comparison from each model's runs and cost, ranked and decided.

    Args:
        runs: The pinned model first, then each candidate in the order asked.
        costs: Each model's cost by model; a model absent from it is unread.
        notes: What the app knows to note, such as the versions it committed.
    """
    models = tuple(model_report(item, metrics, costs.get(item.model, ModelCost())) for item in runs)
    unranked = EvalComparison(
        eval_name,
        kind,
        subject,
        dataset,
        runs_per_model,
        metrics,
        models,
        question_reports(runs, metrics),
        Verdict("incomplete", None, "", None, ()),
        (),
    )
    return finalize(unranked, extra_notes=notes)


def model_lifecycle(row: CortexBaseModel | None) -> str:
    """Name a model's lifecycle from its SHOW CORTEX BASE MODELS row: `ga` to `unknown`.

    A model SHOW does not list, or lists with a status SST does not know, is `unknown`.
    """
    if row is None:
        return "unknown"
    return _LIFECYCLE.get((row.lifecycle_status or "").upper(), "unknown")


def model_report(runs: ModelRuns, metrics: Sequence[MetricSpec], cost: ModelCost) -> ModelReport:
    """Summarize one model's runs, unranked: its metrics, latency, tokens, cost and requests."""
    rows = [row for attempt in runs.scored for row in attempt.rows]
    durations = sorted(row.cost.duration_ms for row in rows if row.cost is not None)
    status = "failed" if not runs.scored else "partial" if len(runs.scored) < runs.runs else "completed"
    return ModelReport(
        model=runs.model,
        role=runs.role,
        version=runs.version,
        alias=runs.alias,
        lifecycle=runs.lifecycle,
        status=status,
        error=runs.error,
        rank=0,
        run_names=runs.run_names,
        metrics=tuple((metric.name, _summary(runs.scored, metric)) for metric in metrics),
        latency_mean=round(sum(durations) / len(durations)) if durations else None,
        latency_p95=durations[max(0, math.ceil(0.95 * len(durations)) - 1)] if durations else None,
        agent_input=sum(attempt.cost.total_input_tokens for attempt in runs.scored),
        agent_output=sum(attempt.cost.total_output_tokens for attempt in runs.scored),
        llm_calls=sum(attempt.cost.llm_call_count for attempt in runs.scored),
        judge=sum(attempt.cost.total_tokens for attempt in runs.scored),
        cost=cost,
        request_ids=tuple(sorted({request for row in rows for request in row.request_ids})),
    )


def question_reports(runs: Sequence[ModelRuns], metrics: Sequence[MetricSpec]) -> tuple[QuestionReport, ...]:
    """Report each question in dataset order: each model's result, and what each candidate changed.

    Each model's result is from its run nearest the mean of the first gated metric, else the
    first metric. A question is keyed `q<n>` by its position in the dataset.
    """
    metric = headline_metric(metrics)
    chosen = {
        item.model: {row.question_key: row for row in nearest_run(item.scored, metric).rows}
        for item in runs
        if item.scored and metric is not None
    }
    order: dict[str, EvalResultRow] = {}
    for rows in chosen.values():
        for key, row in rows.items():
            order.setdefault(key, row)
    pinned = next((item.model for item in runs if item.role == PINNED), None)
    reports: list[QuestionReport] = []
    for key, first in sorted(order.items(), key=lambda pair: (pair[1].index or 0, pair[0])):
        results = tuple(
            (model, _result(rows[key], metric)) for model, rows in chosen.items() if key in rows and metric is not None
        )
        reports.append(
            QuestionReport(f"q{first.index or len(reports) + 1}", first.input_query, results, _changes(results, pinned))
        )
    return tuple(reports)


def headline_metric(metrics: Sequence[MetricSpec]) -> MetricSpec | None:
    """Return the metric a verdict and a question's results read: the first gated one, else the first."""
    return next((metric for metric in metrics if metric.gated), metrics[0] if metrics else None)


def nearest_run(scored: Sequence[EvalRunAttempt], metric: MetricSpec | None) -> EvalRunAttempt:
    """Return the scored run whose mean of `metric` is nearest its mean over all runs; the first on a tie.

    A run that scored none of the metric is never nearer than one that did.
    """
    if metric is None:
        return scored[0]
    overall = _mean([score for attempt in scored for score in _scores(attempt.rows, metric.name)])
    means = [_mean(_scores(attempt.rows, metric.name)) for attempt in scored]
    distance = [math.inf if mean is None or overall is None else abs(mean - overall) for mean in means]
    return scored[distance.index(min(distance))]


def finalize(evaluation: EvalComparison, *, extra_notes: tuple[str, ...] = ()) -> EvalComparison:
    """Price each model per question, rank the models, decide the verdict, and settle the cost note.

    Idempotent: a refreshed report is finalized again from its new cost. The pinned model stays
    first; candidates follow in rank order, ties in their given order.

    Args:
        extra_notes: Notes to add once, after the report's own, as a new report's are.
    """
    priced = [replace(model, cost=_per_question(model)) for model in evaluation.models]
    ranks = _ranks(priced, evaluation.metrics)
    ranked = [replace(model, rank=rank) for model, rank in zip(priced, ranks, strict=True)]
    pinned = [model for model in ranked if model.role == PINNED]
    candidates = sorted((model for model in ranked if model.role != PINNED), key=lambda model: model.rank)
    models = (*pinned, *candidates)
    notes = tuple(note for note in (*evaluation.notes, *extra_notes) if note != PENDING_NOTE)
    if any(amount.state == PENDING for model in models for amount in _amounts(model.cost)):
        notes = (*notes, PENDING_NOTE)
    verdict = decide(evaluation.kind, models, evaluation.metrics, subject=evaluation.subject.name)
    return replace(evaluation, models=models, verdict=verdict, notes=notes)


def decide(kind: str, models: Sequence[ModelReport], metrics: Sequence[MetricSpec], *, subject: str) -> Verdict:
    """Decide the verdict as the module docstring says, from ranked models, the pinned one among them.

    A winner's pin suggestion is the `config:` block of the subject's own file, never a project-wide
    key: the comparison measured this subject alone, and `agents.+orchestration_model` would move
    every agent with it.
    """
    excluded = tuple(
        (model.model, model.lifecycle)
        for model in models
        if model.role == CANDIDATE and model.lifecycle in EXCLUDED_LIFECYCLES
    )
    pinned = next(model for model in models if model.role == PINNED)
    candidates = [model for model in models if model.role == CANDIDATE and model.status != "failed"]
    metric = headline_metric(metrics)
    if pinned.status == "failed" or not candidates or metric is None:
        failed = pinned.model if pinned.status == "failed" else "every candidate"
        return Verdict("incomplete", None, f"{failed} scored no run, so nothing compares.", None, excluded)
    eligible = sorted(
        (model for model in candidates if model.lifecycle not in EXCLUDED_LIFECYCLES), key=lambda model: model.rank
    )
    held = pinned.metric(metric.name) or MetricSummary(None, None, None, 0, 0)
    best = eligible[0] if eligible else None
    leading = best.metric(metric.name) if best is not None else None
    if best is None or leading is None or not _above(leading.mean, held.mean):
        return Verdict("pinned_holds", None, _held(pinned, best, leading, held, metric), None, excluded)
    unclear = _unclear(pinned, best, leading, held, metric)
    if unclear is not None:
        return Verdict("no_clear_winner", None, unclear, None, excluded)
    versus = _versus(metric, leading, held)
    summary = f"{best.model} beats the pinned {pinned.model} {versus} outside the spread, {_price(best, pinned)}."
    return Verdict("winner", best.model, summary, pin_suggestion(kind, subject, best.model), excluded)


def _held(
    pinned: ModelReport,
    best: ModelReport | None,
    leading: MetricSummary | None,
    held: MetricSummary,
    metric: MetricSpec,
) -> str:
    """Say no candidate beats the pinned model, naming a candidate that ties it and still ranks above it.

    Such a candidate is ahead on what ranks after the headline -- another metric, its price, or
    its latency -- which a reviewer weighing a cheaper model needs, though it is no winner to pin.
    """
    summary = f"No candidate beats the pinned {pinned.model} on {metric.name} ({_score(held.mean)})."
    if best is not None and leading is not None and leading.mean == held.mean and best.rank < pinned.rank:
        summary += f" {best.model} ties it {_price(best, pinned)} and ranks above it on what follows."
    return summary


def _unclear(
    pinned: ModelReport, best: ModelReport, leading: MetricSummary, held: MetricSummary, metric: MetricSpec
) -> str | None:
    """Say why a candidate's lead names no winner, too few runs or overlapping ones; None if clear."""
    versus = _versus(metric, leading, held)
    if not _repeated(pinned) or not _repeated(best):
        runs = _count(min(len(pinned.run_names), len(best.run_names)), "completed run")
        return (
            f"{best.model} leads the pinned {pinned.model} {versus} on {runs} each at most, too few to tell "
            f"a lead from run-to-run noise; compare with --compare-runs {MIN_WINNER_RUNS} or more."
        )
    if not _above(leading.min, held.max):
        return (
            f"{best.model} leads the pinned {pinned.model} {versus}, but their runs overlap; "
            "compare more runs with --compare-runs."
        )
    return None


def _versus(metric: MetricSpec, leading: MetricSummary, held: MetricSummary) -> str:
    return f"on {metric.name} ({_score(leading.mean)} vs {_score(held.mean)})"


def pin_suggestion(kind: str, subject: str, model: str) -> str:
    """Return the YAML that pins `model` for one subject, under a comment naming the file it goes in."""
    where = (
        f"agent.yml of agent '{subject}'" if kind == "agent" else f"cowork_profile.yml of CoWork profile '{subject}'"
    )
    return f"# {where}\nconfig:\n  orchestration_model: {model}"


def _summary(scored: Sequence[EvalRunAttempt], metric: MetricSpec) -> MetricSummary:
    """Summarize one metric over a model's runs: its mean over every score, each run's mean, and passes.

    The pass count is of the run nearest the mean, as `nearest_run` picks it, by the metric's band.
    """
    run_means = [mean for attempt in scored if (mean := _mean(_scores(attempt.rows, metric.name))) is not None]
    overall = _mean([score for attempt in scored for score in _scores(attempt.rows, metric.name)])
    nearest = list(nearest_run(scored, metric).rows) if scored else []
    results = [result for row in nearest for result in row.metrics if result.metric_name == metric.name]
    passed = sum(_passes(result.score, metric.threshold) for result in results)
    return MetricSummary(overall, min(run_means, default=None), max(run_means, default=None), passed, len(results))


def _scores(rows: Sequence[EvalResultRow], name: str) -> list[float]:
    return [
        result.score
        for row in rows
        for result in row.metrics
        if result.metric_name == name and result.score is not None
    ]


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _passes(score: float | None, threshold: ThresholdRange | None) -> bool:
    """Report whether a score passes its band, or, with none, is a full 1.0."""
    if score is None:
        return False
    if threshold is None:
        return score >= 1.0
    return (threshold.min is None or score >= threshold.min) and (threshold.max is None or score <= threshold.max)


def _result(row: EvalResultRow, metric: MetricSpec) -> QuestionResult:
    found = next((result for result in row.metrics if result.metric_name == metric.name), None)
    score = found.score if found is not None else None
    reason = found.explanation if found is not None else None
    return QuestionResult(_passes(score, metric.threshold), score, reason, row.routed_subagents)


def _changes(results: tuple[tuple[str, QuestionResult], ...], pinned: str | None) -> tuple[tuple[str, str], ...]:
    held = dict(results).get(pinned or "")
    if held is None:
        return ()
    return tuple(
        (model, "fixes" if result.passed else "breaks")
        for model, result in results
        if model != pinned and result.passed != held.passed
    )


def _per_question(model: ModelReport) -> ModelCost:
    """Price a model's runs per agent request: its agent credits, with its SQL's once they are known."""
    agent, sql = model.cost.agent_credits, model.cost.sql_credits
    if agent.state != KNOWN or not model.request_ids:
        state = agent.state if agent.state != KNOWN else UNAVAILABLE
        return replace(model.cost, credits_per_question=Amount(state, reason=agent.reason or "no request was answered"))
    total = (agent.value or 0.0) + (sql.value or 0.0 if sql.state == KNOWN else 0.0)
    return replace(model.cost, credits_per_question=Amount(KNOWN, round(total / len(model.request_ids), 6)))


def _ranks(models: Sequence[ModelReport], metrics: Sequence[MetricSpec]) -> list[int]:
    """Rank each model, 1 the best; equal keys share the rank of the first of them."""
    ordered = [*(metric for metric in metrics if metric.gated), *(metric for metric in metrics if not metric.gated)]
    keys = [_key(model, ordered) for model in models]
    return [1 + sum(other < key for other in keys) for key in keys]


def _key(model: ModelReport, metrics: Sequence[MetricSpec]) -> tuple[tuple[int, float], ...]:
    """The model's ranking key: each value as `(unknown, value)`, oriented so that smaller is better."""
    means = [summary.mean if (summary := model.metric(metric.name)) is not None else None for metric in metrics]
    price = model.cost.credits_per_question.value if model.cost.credits_per_question.state == KNOWN else None
    latency = float(model.latency_p95) if model.latency_p95 is not None else None
    return (
        *((0, -mean) if mean is not None else (1, 0.0) for mean in means),
        (0, price) if price is not None else (1, 0.0),
        (0, latency) if latency is not None else (1, 0.0),
    )


def _repeated(model: ModelReport) -> bool:
    """Report whether a model completed enough runs for its spread to mean anything."""
    return model.status == "completed" and len(model.run_names) >= MIN_WINNER_RUNS


def _count(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _above(value: float | None, other: float | None) -> bool:
    return value is not None and (other is None or value > other)


def _score(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "no score"


def _price(best: ModelReport, pinned: ModelReport) -> str:
    """Say what the winner costs per question against the pinned model, or why that is not known yet."""
    mine, theirs = best.cost.credits_per_question, pinned.cost.credits_per_question
    if mine.state == KNOWN and theirs.state == KNOWN and theirs.value:
        return f"at {(mine.value or 0.0) / theirs.value:.1f}x the credits per question"
    state = PENDING if PENDING in (mine.state, theirs.state) else UNAVAILABLE
    return f"its credits per question {state}"


def _amounts(cost: ModelCost) -> tuple[Amount, ...]:
    return (cost.agent_credits, cost.sql_credits, cost.judge_tokens)


def unavailable_reason(evaluation: EvalComparison) -> str:
    """Say why the first unavailable amount of the comparison is unavailable, as its read said."""
    return next(
        (
            amount.reason
            for model in evaluation.models
            for amount in _amounts(model.cost)
            if amount.state == UNAVAILABLE and amount.reason
        ),
        "nothing attributes the usage",
    )


def cost_states(evaluation: EvalComparison) -> Mapping[str, tuple[str, ...]]:
    """Return the models whose cost is pending, and those whose cost is unavailable, by state.

    A model counts once per state, whichever of its amounts are in it.
    """
    found: dict[str, list[str]] = {PENDING: [], UNAVAILABLE: []}
    for model in evaluation.models:
        for state in (PENDING, UNAVAILABLE):
            if any(amount.state == state for amount in _amounts(model.cost)):
                found[state].append(model.model)
    return {state: tuple(models) for state, models in found.items()}
