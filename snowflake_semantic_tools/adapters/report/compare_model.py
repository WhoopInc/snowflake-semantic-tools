"""The `sst.compare/1` report read into the rows its HTML renders, every value checked as it is read.

`docs/design/compare-report-schema.md` is the contract. Each enumeration is folded onto the values
it names, so a renderer maps them through fixed tables and never puts report text in a class
name: an unknown `lifecycle` reads as ``unknown``, an unknown `status` or `verdict.kind` as
``unknown``, a cost whose `state` is not one of the three, or is ``known`` without a number, as
``unavailable``, and a `changes` value other than ``fixes`` or ``breaks`` as unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.adapters.report.values import mapping, number, records, strings, text

SCHEMA = "sst.compare/1"
COST_KEYS = ("agent_credits", "sql_credits", "judge_tokens", "credits_per_question")
LIFECYCLES = frozenset(("ga", "preview", "internal", "deprecated", "unknown"))
STATUSES = frozenset(("completed", "partial", "failed"))
VERDICTS = frozenset(("winner", "pinned_holds", "no_clear_winner", "incomplete"))
CHANGES = frozenset(("fixes", "breaks"))


@dataclass(frozen=True)
class Metric:
    """A metric the eval scores, in the report's order (gated ones are not moved first here)."""

    name: str
    gated: bool
    threshold: float | None


@dataclass(frozen=True)
class Score:
    """One model's result on one metric.

    Attributes:
        low: The lowest per-run mean (`min`); `high` the highest. Either may be None alone.
        passed: Questions passing in the run nearest the mean, out of `total`.
    """

    mean: float | None
    low: float | None
    high: float | None
    passed: float | None
    total: float | None


@dataclass(frozen=True)
class Cost:
    """One cost figure: `value` is set exactly when `state` is ``known``."""

    state: str
    value: float | None


@dataclass(frozen=True)
class ModelRow:
    """One orchestration model's runs over the eval set.

    Attributes:
        name: The model, as `questions[].results` and `changes` key it; ``unnamed model`` when absent.
        lifecycle: One of `LIFECYCLES`.
        status: One of `STATUSES`, or ``unknown``.
        rank: The report's rank, or None when it gave none.
        tokens: `agent_input`, `agent_output`, `llm_calls`, and `judge`, each None when not a number.
        costs: Every key of `COST_KEYS`, always present.
    """

    name: str
    pinned: bool
    version: str | None
    alias: str | None
    lifecycle: str
    status: str
    error: str | None
    rank: int | None
    run_names: tuple[str, ...]
    scores: Mapping[str, Score]
    latency_mean: float | None
    latency_p95: float | None
    tokens: Mapping[str, float | None]
    costs: Mapping[str, Cost]


@dataclass(frozen=True)
class Result:
    """One model's answer to one question, scored on the headline metric.

    Attributes:
        passed: None when the report did not say.
        route: For a CoWork profile, the subagents the router chose, ``()`` for a decline; None
            when the report carries no route, as for an agent.
    """

    passed: bool | None
    score: float | None
    reason: str | None
    route: tuple[str, ...] | None


@dataclass(frozen=True)
class Question:
    """One question of the dataset, with each model's result and each candidate's change.

    Attributes:
        changes: ``fixes`` or ``breaks`` by candidate model; a model with no entry is unchanged.
    """

    id: str | None
    text: str | None
    results: Mapping[str, Result]
    changes: Mapping[str, str]


@dataclass(frozen=True)
class Verdict:
    """What the comparison concluded.

    Attributes:
        kind: One of `VERDICTS`, or ``unknown`` when the report holds none it names.
        excluded: (model, reason) for each model left out of the verdict, either part None.
    """

    kind: str
    model: str | None
    summary: str | None
    pin_suggestion: str | None
    excluded: tuple[tuple[str | None, str | None], ...]


@dataclass(frozen=True)
class EvalView:
    """One eval's comparison, its models ordered pinned first and then by rank.

    Attributes:
        kind: ``agent``, ``cowork_profile``, or ``unknown``.
        questions_count: The dataset's stated question count, which may differ from `questions`.
    """

    eval: str | None
    kind: str
    subject: Mapping[str, str | None]
    dataset_name: str | None
    dataset_version: str | None
    questions_count: float | None
    runs_per_model: float | None
    metrics: tuple[Metric, ...]
    models: tuple[ModelRow, ...]
    questions: tuple[Question, ...]
    verdict: Verdict
    notes: tuple[str, ...]

    @property
    def headline(self) -> Metric | None:
        """Return the metric a scorecard leads with: the first gated one, else the first, else None."""
        return next((metric for metric in self.metrics if metric.gated), self.metrics[0] if self.metrics else None)

    @property
    def gated_first(self) -> tuple[Metric, ...]:
        """Return the metrics with the gated ones first, each group in the report's order."""
        return tuple(sorted(self.metrics, key=lambda metric: not metric.gated))


def check_schema(report: Mapping[str, object]) -> None:
    """Refuse a report that does not carry the schema tag this renderer reads.

    Raises:
        ValueError: `schema` is not ``sst.compare/1``.
    """
    found = report.get("schema")
    if found != SCHEMA:
        raise ValueError(f"not a {SCHEMA} report: its schema is {found!r}")


def read_evals(report: Mapping[str, object]) -> list[EvalView]:
    """Read every eval of a report, in its order."""
    return [read_eval(raw) for raw in records(report.get("evals"))]


def read_eval(raw: Mapping[str, object]) -> EvalView:
    """Read one entry of `evals`, every absent or ill-typed value read as unknown."""
    subject = mapping(raw.get("subject"))
    dataset = mapping(raw.get("dataset"))
    kind = text(raw.get("kind"))
    models = [_model(item) for item in records(raw.get("models"))]
    return EvalView(
        eval=text(raw.get("eval")),
        kind=kind if kind in ("agent", "cowork_profile") else "unknown",
        subject={key: text(subject.get(key)) for key in ("name", "qualified_name", "released_version", "pinned_model")},
        dataset_name=text(dataset.get("name")),
        dataset_version=text(dataset.get("version")),
        questions_count=number(dataset.get("questions")),
        runs_per_model=number(raw.get("runs_per_model")),
        metrics=tuple(_metric(item) for item in records(raw.get("metrics"))),
        models=tuple(sorted(models, key=_order)),
        questions=tuple(_question(item) for item in records(raw.get("questions"))),
        verdict=_verdict(mapping(raw.get("verdict"))),
        notes=tuple(strings(raw.get("notes"))),
    )


def _order(model: ModelRow) -> tuple[bool, bool, int]:
    # sorted() is stable, so models of equal rank keep the report's order.
    return (not model.pinned, model.rank is None, model.rank or 0)


def _metric(raw: Mapping[str, object]) -> Metric:
    return Metric(text(raw.get("name")) or "unnamed metric", raw.get("gated") is True, number(raw.get("threshold")))


def _model(raw: Mapping[str, object]) -> ModelRow:
    lifecycle = text(raw.get("lifecycle"))
    status = text(raw.get("status"))
    rank = number(raw.get("rank"))
    latency = mapping(raw.get("latency_ms"))
    tokens = mapping(raw.get("tokens"))
    cost = mapping(raw.get("cost"))
    return ModelRow(
        name=text(raw.get("model")) or "unnamed model",
        pinned=raw.get("role") == "pinned",
        version=text(raw.get("version")),
        alias=text(raw.get("alias")),
        lifecycle=lifecycle if lifecycle in LIFECYCLES else "unknown",
        status=status if status in STATUSES else "unknown",
        error=text(raw.get("error")),
        rank=None if rank is None else int(rank),
        run_names=tuple(strings(raw.get("run_names"))),
        scores={name: _score(mapping(value)) for name, value in mapping(raw.get("metrics")).items()},
        latency_mean=number(latency.get("mean")),
        latency_p95=number(latency.get("p95")),
        tokens={key: number(tokens.get(key)) for key in ("agent_input", "agent_output", "llm_calls", "judge")},
        costs={key: _cost(mapping(cost.get(key))) for key in COST_KEYS},
    )


def _score(raw: Mapping[str, object]) -> Score:
    return Score(
        number(raw.get("mean")),
        number(raw.get("min")),
        number(raw.get("max")),
        number(raw.get("passed")),
        number(raw.get("total")),
    )


def _cost(raw: Mapping[str, object]) -> Cost:
    state = raw.get("state")
    value = number(raw.get("value"))
    if state == "known" and value is not None:
        return Cost("known", value)
    return Cost("pending" if state == "pending" else "unavailable", None)


def _question(raw: Mapping[str, object]) -> Question:
    changes = mapping(raw.get("changes"))
    return Question(
        id=text(raw.get("id")),
        text=text(raw.get("text")),
        results={name: _result(mapping(value)) for name, value in mapping(raw.get("results")).items()},
        changes={name: str(value) for name, value in changes.items() if value in CHANGES},
    )


def _result(raw: Mapping[str, object]) -> Result:
    passed = raw.get("passed")
    route = raw.get("route")
    return Result(
        passed=passed if isinstance(passed, bool) else None,
        score=number(raw.get("score")),
        reason=text(raw.get("reason")),
        route=tuple(item for item in route if isinstance(item, str)) if isinstance(route, list) else None,
    )


def _verdict(raw: Mapping[str, object]) -> Verdict:
    kind = text(raw.get("kind"))
    excluded = records(raw.get("excluded"))
    return Verdict(
        kind=kind if kind in VERDICTS else "unknown",
        model=text(raw.get("model")),
        summary=text(raw.get("summary")),
        pin_suggestion=text(raw.get("pin_suggestion")),
        excluded=tuple((text(item.get("model")), text(item.get("reason"))) for item in excluded),
    )
