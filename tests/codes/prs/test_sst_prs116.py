"""SST-PRS116: a custom eval metric's judge prompt uses a placeholder SST does not supply."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs116_fires() -> None:
    metric = replace(VALUE.custom_metrics[0], prompt="Score from 0 to 5 using {{unknown}}.")
    [diagnostic] = eval_catalog_findings("SST-PRS116", metrics=(metric,))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "grounding: 'unknown' is not one of the 12 supported names"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_prs116_silent() -> None:
    assert eval_catalog_findings("SST-PRS116") == []
