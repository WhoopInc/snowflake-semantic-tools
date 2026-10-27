"""SST-PRS115: a custom eval metric's `threshold_default` has no usable bound within its scale."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import ThresholdRange
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs115_fires() -> None:
    metric = replace(VALUE.custom_metrics[0], threshold_default=ThresholdRange(min=6, max=2))
    [diagnostic] = eval_catalog_findings("SST-PRS115", metrics=(metric,))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "grounding: threshold_default min=6, max=2 is not a usable bound within max_score 0..5"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_prs115_silent() -> None:
    metric = replace(VALUE.custom_metrics[0], threshold_default=ThresholdRange(min=3))
    assert eval_catalog_findings("SST-PRS115", metrics=(metric,)) == []
