"""SST-PRS031: a member name is a Snowflake reserved word."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs031_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n", name="select")).load(), "SST-PRS031"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "'select' is a reserved word"
    assert diagnostic.subject == "metric:select"


def test_sst_prs031_silent(tmp_path: Path) -> None:
    assert (
        found(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n", name="selection")).load(), "SST-PRS031")
        == []
    )
