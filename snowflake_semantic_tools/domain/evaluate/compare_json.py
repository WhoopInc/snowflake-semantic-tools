"""Write a model comparison as `sst.compare/1`, and read one back to refresh its cost.

The shape is docs/design/compare-report-schema.md: every key always present, a value not known
`null`, numbers as JSON numbers, lists in display order. `report_from_json` reads exactly that
shape back and refuses anything else, so `--refresh-cost` never rewrites a file it did not write.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeVar

from snowflake_semantic_tools.domain.evaluate.compare import (
    Amount,
    Dataset,
    EvalComparison,
    MetricSpec,
    MetricSummary,
    ModelCost,
    ModelReport,
    QuestionReport,
    QuestionResult,
    Subject,
    Verdict,
)
from snowflake_semantic_tools.domain.model.eval import ThresholdRange

SCHEMA = "sst.compare/1"
_COSTS = ("agent_credits", "sql_credits", "judge_tokens", "credits_per_question")
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class CompareReport:
    """A whole comparison run: when it ran and read cost, on which target, and each eval's comparison.

    Attributes:
        cost_read_at: When the cost was last read; None when it never was.
        git_sha: The commit the comparison ran from, as the project reported it.
    """

    generated_at: str
    cost_read_at: str | None
    target: str
    run_id: str
    git_sha: str
    evals: tuple[EvalComparison, ...]


def report_json(report: CompareReport) -> dict[str, object]:
    """Write a comparison as the `sst.compare/1` object, keys in schema order."""
    return {
        "schema": SCHEMA,
        "generated_at": report.generated_at,
        "cost_read_at": report.cost_read_at,
        "target": report.target,
        "run_id": report.run_id,
        "git_sha": report.git_sha,
        "evals": [_eval_json(item) for item in report.evals],
    }


def _eval_json(item: EvalComparison) -> dict[str, object]:
    """Write one eval's comparison: subject, dataset, metrics, models, questions, verdict, notes."""
    return {
        "eval": item.eval,
        "kind": item.kind,
        "subject": {
            "name": item.subject.name,
            "qualified_name": item.subject.qualified_name,
            "released_version": item.subject.released_version,
            "pinned_model": item.subject.pinned_model,
        },
        "dataset": {"name": item.dataset.name, "version": item.dataset.version, "questions": item.dataset.questions},
        "runs_per_model": item.runs_per_model,
        "metrics": [{"name": metric.name, "gated": metric.gated, "threshold": metric.bar} for metric in item.metrics],
        "models": [_model_json(model) for model in item.models],
        "questions": [
            {
                "id": question.id,
                "text": question.text,
                "results": {
                    model: {
                        "passed": result.passed,
                        "score": result.score,
                        "reason": result.reason,
                        "route": None if result.route is None else list(result.route),
                    }
                    for model, result in question.results
                },
                "changes": dict(question.changes),
            }
            for question in item.questions
        ],
        "verdict": {
            "kind": item.verdict.kind,
            "model": item.verdict.model,
            "summary": item.verdict.summary,
            "pin_suggestion": item.verdict.pin_suggestion,
            "excluded": [{"model": model, "reason": reason} for model, reason in item.verdict.excluded],
        },
        "notes": list(item.notes),
    }


def _model_json(model: ModelReport) -> dict[str, object]:
    """Write one model's line: its version, status and rank, each metric, its latency, tokens and cost."""
    return {
        "model": model.model,
        "role": model.role,
        "version": model.version,
        "alias": model.alias,
        "lifecycle": model.lifecycle,
        "status": model.status,
        "error": model.error,
        "rank": model.rank,
        "run_names": list(model.run_names),
        "metrics": {
            name: {"mean": item.mean, "min": item.min, "max": item.max, "passed": item.passed, "total": item.total}
            for name, item in model.metrics
        },
        "latency_ms": {"mean": model.latency_mean, "p95": model.latency_p95},
        "tokens": {
            "agent_input": model.agent_input,
            "agent_output": model.agent_output,
            "llm_calls": model.llm_calls,
            "judge": model.judge,
        },
        "cost": {
            name: {"state": amount.state, "value": amount.value}
            for name, amount in zip(_COSTS, _cost_amounts(model.cost), strict=True)
        },
        "request_ids": list(model.request_ids),
    }


def _cost_amounts(cost: ModelCost) -> tuple[Amount, ...]:
    return (cost.agent_credits, cost.sql_credits, cost.judge_tokens, cost.credits_per_question)


def report_from_json(document: object) -> CompareReport:
    """Read back a report `report_json` wrote.

    Raises:
        ValueError: the document is not an `sst.compare/1` object, or a key is missing or holds
            a value of another type; the message names the key.
    """
    root = _Object(document, "the report")
    if root.text("schema") != SCHEMA:
        raise ValueError(f"the report's schema is {root.raw('schema')!r}, not {SCHEMA!r}")
    return CompareReport(
        root.text("generated_at"),
        root.optional_text("cost_read_at"),
        root.text("target"),
        root.text("run_id"),
        root.text("git_sha"),
        tuple(_eval(item) for item in root.objects("evals")),
    )


def _eval(item: _Object) -> EvalComparison:
    """Read one eval's comparison back, as `_eval_json` wrote it.

    Raises:
        ValueError: a key is missing or holds a value of another type.
    """
    subject, dataset, verdict = item.object("subject"), item.object("dataset"), item.object("verdict")
    return EvalComparison(
        item.text("eval"),
        item.text("kind"),
        Subject(
            subject.text("name"),
            subject.text("qualified_name"),
            subject.optional_text("released_version"),
            subject.text("pinned_model"),
        ),
        Dataset(dataset.text("name"), dataset.optional_text("version"), dataset.integer("questions")),
        item.integer("runs_per_model"),
        tuple(_metric(metric) for metric in item.objects("metrics")),
        tuple(_model(model) for model in item.objects("models")),
        tuple(_question(question) for question in item.objects("questions")),
        Verdict(
            verdict.text("kind"),
            verdict.optional_text("model"),
            verdict.text("summary"),
            verdict.optional_text("pin_suggestion"),
            tuple((entry.text("model"), entry.text("reason")) for entry in verdict.objects("excluded")),
        ),
        tuple(item.texts("notes")),
    )


def _metric(item: _Object) -> MetricSpec:
    bar = item.optional_number("threshold")
    return MetricSpec(item.text("name"), item.boolean("gated"), ThresholdRange(min=bar) if bar is not None else None)


def _model(item: _Object) -> ModelReport:
    latency, tokens, cost = item.object("latency_ms"), item.object("tokens"), item.object("cost")
    amounts = tuple(_amount(cost.object(name)) for name in _COSTS)
    return ModelReport(
        model=item.text("model"),
        role=item.text("role"),
        version=item.optional_text("version"),
        alias=item.optional_text("alias"),
        lifecycle=item.text("lifecycle"),
        status=item.text("status"),
        error=item.optional_text("error"),
        rank=item.integer("rank"),
        run_names=tuple(item.texts("run_names")),
        metrics=tuple((name, _summary(summary)) for name, summary in item.object("metrics").entries()),
        latency_mean=latency.optional_integer("mean"),
        latency_p95=latency.optional_integer("p95"),
        agent_input=tokens.integer("agent_input"),
        agent_output=tokens.integer("agent_output"),
        llm_calls=tokens.integer("llm_calls"),
        judge=tokens.integer("judge"),
        cost=ModelCost(*amounts),
        request_ids=tuple(item.texts("request_ids")),
    )


def _summary(item: _Object) -> MetricSummary:
    return MetricSummary(
        item.optional_number("mean"),
        item.optional_number("min"),
        item.optional_number("max"),
        item.integer("passed"),
        item.integer("total"),
    )


def _amount(item: _Object) -> Amount:
    state = item.text("state")
    if state not in ("known", "pending", "unavailable"):
        raise ValueError(f"{item.where}.state is {state!r}, not known, pending or unavailable")
    return Amount(state, item.optional_number("value"))


def _question(item: _Object) -> QuestionReport:
    results = tuple(
        (
            model,
            QuestionResult(
                result.boolean("passed"),
                result.optional_number("score"),
                result.optional_text("reason"),
                None if result.raw("route") is None else tuple(result.texts("route")),
            ),
        )
        for model, result in item.object("results").entries()
    )
    changes = tuple(item.object("changes").text_entries())
    return QuestionReport(item.text("id"), item.text("text"), results, changes)


class _Object:
    """One JSON object of the report, read key by key, each read naming the key it refused."""

    def __init__(self, value: object, where: str) -> None:
        if not isinstance(value, Mapping):
            raise ValueError(f"{where} is not an object")
        self._value = value
        self.where = where

    def raw(self, key: str) -> object:
        if key not in self._value:
            raise ValueError(f"{self.where} has no {key!r}")
        return self._value[key]

    def value(self, key: str, kind: type[_T]) -> _T:
        found = self.raw(key)
        if not isinstance(found, kind) or (kind is not bool and isinstance(found, bool)):
            raise ValueError(f"{self.where}.{key} is not {_KINDS.get(kind, kind.__name__)}")
        return found

    def optional(self, key: str, read: Callable[[str], _T]) -> _T | None:
        return None if self.raw(key) is None else read(key)

    def text(self, key: str) -> str:
        return self.value(key, str)

    def optional_text(self, key: str) -> str | None:
        return self.optional(key, self.text)

    def integer(self, key: str) -> int:
        return self.value(key, int)

    def optional_integer(self, key: str) -> int | None:
        return self.optional(key, self.integer)

    def optional_number(self, key: str) -> float | None:
        found = self.raw(key)
        if found is None:
            return None
        if isinstance(found, bool) or not isinstance(found, (int, float)):
            raise ValueError(f"{self.where}.{key} is not a number")
        return float(found)

    def boolean(self, key: str) -> bool:
        return self.value(key, bool)

    def object(self, key: str) -> _Object:
        return _Object(self.raw(key), f"{self.where}.{key}")

    def objects(self, key: str) -> list[_Object]:
        return [_Object(item, f"{self.where}.{key}[{index}]") for index, item in enumerate(self.value(key, list))]

    def texts(self, key: str) -> list[str]:
        items: Sequence[object] = self.value(key, list)
        if not all(isinstance(item, str) for item in items):
            raise ValueError(f"{self.where}.{key} is not a list of strings")
        return [str(item) for item in items]

    def entries(self) -> list[tuple[str, _Object]]:
        return [(str(key), _Object(item, f"{self.where}.{key}")) for key, item in self._value.items()]

    def text_entries(self) -> list[tuple[str, str]]:
        return [(str(key), self.text(str(key))) for key in self._value]


_KINDS: Mapping[type, str] = {str: "a string", int: "an integer", bool: "a boolean", list: "a list"}
