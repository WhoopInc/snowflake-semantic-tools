"""SST-PRS114: a custom eval metric's score ranges leave a gap or overlap."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.eval import CustomEvalMetric, EvalCatalog, EvalScoreRanges, ResolvedEval
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog
from tests.helpers.eval_builders import resolved_eval

VALUE = resolved_eval()


def _found(
    code: str, value: ResolvedEval = VALUE, metrics: tuple[CustomEvalMetric, ...] | None = None
) -> list[Diagnostic]:
    custom = value.custom_metrics if metrics is None else metrics
    catalog = EvalCatalog((value,), custom)
    return [item for item in validate_eval_catalog(catalog, allowed_models=("claude-sonnet-4-6",)) if item.code == code]


def test_sst_prs114_fires() -> None:
    metric = replace(VALUE.custom_metrics[0], score_ranges=EvalScoreRanges((0, 1), (3, 4), (5, 6)))
    [diagnostic] = _found("SST-PRS114", metrics=(metric,))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "grounding: score_ranges leave a gap or overlap at (0, 1, 3, 4, 5, 6)"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_prs114_silent() -> None:
    assert _found("SST-PRS114") == []
