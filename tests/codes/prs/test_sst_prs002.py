"""SST-PRS002: a semantic member omits a field it requires, here a metric's `expr`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, metric_file


def test_sst_prs002_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(SmallProject(tmp_path, files=metric_file("")).load().diagnostics, "SST-PRS002")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: required field 'expr' is missing"
    assert diagnostic.subject == "metric:total"


def test_sst_prs002_silent(tmp_path: Path) -> None:
    assert (
        coded(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n")).load().diagnostics, "SST-PRS002") == []
    )
