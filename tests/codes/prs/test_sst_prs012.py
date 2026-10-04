"""SST-PRS012: a member name holds a character outside ASCII."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, metric_file


def test_sst_prs012_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n", name="café")).load().diagnostics, "SST-PRS012"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "'café' contains non-ASCII characters"
    assert diagnostic.subject == "metric:café"


def test_sst_prs012_silent(tmp_path: Path) -> None:
    assert (
        coded(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n")).load().diagnostics, "SST-PRS012") == []
    )
