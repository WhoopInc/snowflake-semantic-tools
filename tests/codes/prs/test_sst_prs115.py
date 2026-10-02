"""SST-PRS115: a custom eval metric's `threshold_default` has no usable bound within its scale."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.eval import CustomEvalMetric, EvalCatalog, ResolvedEval, ThresholdRange
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog
from tests.helpers.eval_builders import resolved_eval

VALUE = resolved_eval()


def _found(
    code: str, value: ResolvedEval = VALUE, metrics: tuple[CustomEvalMetric, ...] | None = None
) -> list[Diagnostic]:
    custom = value.custom_metrics if metrics is None else metrics
    catalog = EvalCatalog((value,), custom)
    return [item for item in validate_eval_catalog(catalog, allowed_models=("claude-sonnet-4-6",)) if item.code == code]


def test_sst_prs115_fires() -> None:
    metric = replace(VALUE.custom_metrics[0], threshold_default=ThresholdRange(min=6, max=2))
    [diagnostic] = _found("SST-PRS115", metrics=(metric,))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "grounding: threshold_default min=6, max=2 is not a usable bound within max_score 0..5"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_prs115_silent() -> None:
    metric = replace(VALUE.custom_metrics[0], threshold_default=ThresholdRange(min=3))
    assert _found("SST-PRS115", metrics=(metric,)) == []
