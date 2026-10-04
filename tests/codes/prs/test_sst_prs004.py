"""SST-PRS004: a block holds a field SST does not model, and none it does is close."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, metric_file


def test_sst_prs004_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n    owner: data-team\n")).load().diagnostics,
        "SST-PRS004",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:total: unknown field 'owner'"
    assert diagnostic.subject == "metric:total"


def test_sst_prs004_silent(tmp_path: Path) -> None:
    assert (
        coded(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n")).load().diagnostics, "SST-PRS004") == []
    )
