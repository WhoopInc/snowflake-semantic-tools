"""The key table of a custom eval metric file: a judge's model, rubric and score bands."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.authored_schema.field_types import Field, FileKind, Node, ValueKind

_STRING, _LIST, _MAPPING, _BOOLEAN = ValueKind.STRING, ValueKind.LIST, ValueKind.MAPPING, ValueKind.BOOLEAN


def _band(name: str, which: str) -> Field:
    return Field(
        name, _LIST, f"The inclusive {which} score band, as two integers.", required=True, items=ValueKind.INTEGER
    )


_SCORE_RANGES = Node(
    (_band("min_score", "low"), _band("median_score", "middle"), _band("max_score", "high")),
    open=True,
)
_THRESHOLD = Node(
    (
        Field("min", ValueKind.NUMBER, "The lowest passing score."),
        Field("max", ValueKind.NUMBER, "The highest passing score."),
    ),
    open=True,
)
EVAL_METRIC = FileKind(
    "eval_metric",
    "A custom eval metric: an LLM judge that scores each answer against a rubric.",
    "a YAML file in the eval metrics directory",
    Node(
        (
            Field(
                "name", _STRING, "The metric's name, which eval configs reference with `eval_metric()`.", required=True
            ),
            Field("description", _STRING, "What the judge measures."),
            Field("model", _STRING, "The judge model that scores the metric."),
            Field(
                "score_ranges",
                _MAPPING,
                "The judge's three score bands: low, middle and high.",
                required=True,
                node=_SCORE_RANGES,
            ),
            Field("prompt", _STRING, "The judge's rubric: its scale, how to report a score, and how to break ties."),
            Field("gate_default", _BOOLEAN, "Gates the metric in every eval that uses it."),
            Field("threshold_default", _MAPPING, "The default pass band of the metric's score.", node=_THRESHOLD),
            Field("enabled", _BOOLEAN, "`false` leaves the metric out of every eval config."),
            Field("meta", _MAPPING, "Free-form metadata, never rendered."),
        )
    ),
)
