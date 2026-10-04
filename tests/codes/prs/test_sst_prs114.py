"""SST-PRS114: a custom eval metric's score ranges leave a gap or overlap."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalScoreRanges
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs114_fires() -> None:
    metric = replace(VALUE.custom_metrics[0], score_ranges=EvalScoreRanges((0, 1), (3, 4), (5, 6)))
    [diagnostic] = eval_catalog_findings("SST-PRS114", metrics=(metric,))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "grounding: score_ranges leave a gap or overlap at (0, 1, 3, 4, 5, 6)"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_prs114_silent() -> None:
    assert eval_catalog_findings("SST-PRS114") == []
