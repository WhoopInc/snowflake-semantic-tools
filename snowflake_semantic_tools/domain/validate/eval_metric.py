"""Check custom LLM-judge metrics: their names, judge models, prompt contracts and score bands.

Each custom metric in the catalog is checked once, on its own, whether or not an eval
uses it: the rules read only the metric, how many catalog metrics share its name, and
the judge model allowlist.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.eval.model import (
    SUPPORTED_JUDGE_PLACEHOLDERS,
    SYSTEM_EVAL_METRICS,
    CustomEvalMetric,
    EvalScoreRanges,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.validate.shared import Emitter

_JUDGE_PLACEHOLDER = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*}}")
_SCALE = re.compile(r"(-?\d+(?:\.\d+)?)\s+(?:to|and|through|-)\s+(-?\d+(?:\.\d+)?)", re.IGNORECASE)
_SCORE_OUTPUT = re.compile(
    r"(?:return|respond|output|produce).{0,80}(?:numeric\s+)?score|score\s+alone|numeric\s+score",
    re.IGNORECASE,
)
_REASONING_OUTPUT = re.compile(r"\b(?:reasoning|rationale|explain|explanation|justify|justification)\b", re.IGNORECASE)
_TIE_BREAK = re.compile(r"\b(?:tie|if tied|when tied)\b", re.IGNORECASE)
_INSUFFICIENT = re.compile(r"\b(?:insufficient|not enough|unclear|ambiguous)\b", re.IGNORECASE)
# Checked in this order; only the first system metric whose intent a prompt reads as is reported.
_SYSTEM_INTENT_HINTS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "tool_selection_accuracy": ("tool selection", "select the correct tool", "correct tool"),
        "tool_execution_accuracy": ("tool execution", "tool input", "tool output"),
        "answer_correctness": ("answer correctness", "evaluate whether the answer is correct"),
        "logical_consistency": ("logical consistency", "internally consistent"),
    }
)


def validate_custom_metric(
    metric: CustomEvalMetric,
    name_counts: Mapping[str, int],
    allowed_models: frozenset[str],
) -> tuple[Diagnostic, ...]:
    """Check one custom judge metric.

    Args:
        name_counts: How many catalog metrics have each casefolded name.
        allowed_models: The judge models allowed, casefolded; empty allows any explicit model.

    Diagnostics:
        SST-VAL001: another catalog metric has the same name, compared casefolded.
        SST-VAL737: the name is a system metric's.
        SST-VAL733: a default threshold on a metric that does not gate by default.
        SST-VAL738: the judge model is absent or `auto`.
        SST-VAL739: the judge model is outside a non-empty allowlist.
        SST-VAL740: the prompt reads as a system metric's intent.
        SST-VAL741: the prompt states no scale with a score instruction or band anchors.
        SST-VAL742: the prompt asks for reasoning and states no output contract.
        SST-VAL743: the rubric has neither a tie-break nor an insufficient-information branch.
        SST-PRS114: a score band is inverted, or the bands leave a gap or overlap.
        SST-VAL748: an invariant check declares a scale wider than three values.
        SST-VAL747: the prompt's scale reaches outside the declared bands.
        SST-PRS115: the default threshold is not a usable bound inside the declared bands.
        SST-PRS116: the prompt uses a placeholder the judge is not given.
    """
    prompt = metric.prompt or ""
    scale = _prompt_scale(prompt)
    return (
        *_name(metric, name_counts),
        *_ungated_threshold(metric),
        *_judge_model(metric, allowed_models),
        *_system_intent(metric, prompt),
        *_output_contract(metric, prompt, scale),
        *_rubric_branches(metric, prompt),
        *_score_bands(metric, prompt, scale),
        *_unsupported_placeholders(metric, prompt),
    )


def _subject(metric: CustomEvalMetric) -> str:
    return f"eval_metric:{metric.name.casefold()}"


def _emitter(metric: CustomEvalMetric) -> Emitter:
    return Emitter(subject=_subject(metric), origin=metric.origin, artifact=metric.name)


def _name(metric: CustomEvalMetric, name_counts: Mapping[str, int]) -> tuple[Diagnostic, ...]:
    # SST-VAL001 takes the metric's type and name as context, and no artifact.
    emit = Emitter(subject=_subject(metric), origin=metric.origin)
    if name_counts[metric.name.casefold()] > 1:
        emit("SST-VAL001", type="eval_metric", name=metric.name)
    if metric.name.casefold() in SYSTEM_EVAL_METRICS:
        emit("SST-VAL737", artifact=metric.name, name=metric.name)
    return emit.diagnostics


def _ungated_threshold(metric: CustomEvalMetric) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    if metric.threshold_default is not None and not metric.gate_default:
        emit("SST-VAL733", name=metric.name, detail="a threshold on an ungated metric")
    return emit.diagnostics


def _judge_model(metric: CustomEvalMetric, allowed_models: frozenset[str]) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    if metric.model is None or metric.model.casefold() == "auto":
        emit("SST-VAL738", found=metric.model)
    elif allowed_models and metric.model.casefold() not in allowed_models:
        emit("SST-VAL739", found=metric.model)
    return emit.diagnostics


def _system_intent(metric: CustomEvalMetric, prompt: str) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    folded = prompt.casefold()
    intent = next(
        (
            system_name
            for system_name, hints in _SYSTEM_INTENT_HINTS.items()
            if system_name != metric.name.casefold() and any(hint in folded for hint in hints)
        ),
        None,
    )
    if intent is not None:
        emit("SST-VAL740", name=intent)
    return emit.diagnostics


def _output_contract(
    metric: CustomEvalMetric, prompt: str, scale: tuple[float, float] | None
) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    has_output_contract = scale is not None and (
        _SCORE_OUTPUT.search(prompt) is not None or _prompt_anchors_declared_bands(prompt, metric.score_ranges)
    )
    if not has_output_contract:
        emit("SST-VAL741")
    if _REASONING_OUTPUT.search(prompt) and not has_output_contract:
        emit("SST-VAL742")
    return emit.diagnostics


def _rubric_branches(metric: CustomEvalMetric, prompt: str) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    if not _TIE_BREAK.search(prompt) and not _INSUFFICIENT.search(prompt):
        emit("SST-VAL743", detail="tie-break or insufficient-information")
    return emit.diagnostics


def _score_bands(metric: CustomEvalMetric, prompt: str, scale: tuple[float, float] | None) -> tuple[Diagnostic, ...]:
    ranges = metric.score_ranges
    if ranges is None:
        return ()
    return (
        *_band_shape(metric, ranges),
        *_invariant_width(metric, prompt, ranges),
        *_prompt_scale_within_bands(metric, ranges, scale),
        *_threshold_within_bands(metric, ranges),
    )


def _band_shape(metric: CustomEvalMetric, ranges: EvalScoreRanges) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    values = (*ranges.min_score, *ranges.median_score, *ranges.max_score)
    if any(low > high for low, high in (ranges.min_score, ranges.median_score, ranges.max_score)):
        emit("SST-PRS114", value="an inverted band")
    if ranges.min_score[1] + 1 != ranges.median_score[0] or ranges.median_score[1] + 1 != ranges.max_score[0]:
        emit("SST-PRS114", value=f"{values}")
    return emit.diagnostics


def _invariant_width(metric: CustomEvalMetric, prompt: str, ranges: EvalScoreRanges) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    scale_width = ranges.max_score[1] - ranges.min_score[0] + 1
    if "invariant" in f"{metric.description or ''} {prompt}".casefold() and scale_width > 3:
        emit("SST-VAL748", count=scale_width)
    return emit.diagnostics


def _prompt_scale_within_bands(
    metric: CustomEvalMetric, ranges: EvalScoreRanges, scale: tuple[float, float] | None
) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    declared = (ranges.min_score[0], ranges.max_score[1])
    if scale is not None and (scale[0] < declared[0] or scale[1] > declared[1]):
        emit("SST-VAL747", found=f"{scale[0]}..{scale[1]}", expected=f"{declared[0]}..{declared[1]}")
    return emit.diagnostics


def _threshold_within_bands(metric: CustomEvalMetric, ranges: EvalScoreRanges) -> tuple[Diagnostic, ...]:
    threshold = metric.threshold_default
    if threshold is None:
        return ()
    emit = _emitter(metric)
    scale_min = ranges.min_score[0]
    scale_max = ranges.max_score[1]
    usable = (
        threshold.min is not None
        and scale_min <= threshold.min <= scale_max
        and (threshold.max is None or scale_min <= threshold.max <= scale_max)
        and (threshold.max is None or threshold.min <= threshold.max)
    )
    if not usable:
        emit("SST-PRS115", found=_threshold_text(threshold), expected=f"{scale_min}..{scale_max}")
    return emit.diagnostics


def _unsupported_placeholders(metric: CustomEvalMetric, prompt: str) -> tuple[Diagnostic, ...]:
    emit = _emitter(metric)
    for placeholder in _JUDGE_PLACEHOLDER.findall(prompt):
        if placeholder not in SUPPORTED_JUDGE_PLACEHOLDERS:
            emit("SST-PRS116", placeholder=placeholder)
    return emit.diagnostics


def _prompt_scale(prompt: str) -> tuple[float, float] | None:
    matches = tuple(_SCALE.finditer(prompt))
    if not matches:
        return None
    lower = min(float(match.group(1)) for match in matches)
    upper = max(float(match.group(2)) for match in matches)
    return (lower, upper) if lower <= upper else (upper, lower)


def _prompt_anchors_declared_bands(prompt: str, ranges: EvalScoreRanges | None) -> bool:
    if ranges is None:
        return False
    compact = prompt.casefold()
    return all(f"{low} to {high}" in compact for low, high in (ranges.min_score, ranges.median_score, ranges.max_score))


def _threshold_text(value: ThresholdRange) -> str:
    return f"min={value.min}, max={value.max}"
