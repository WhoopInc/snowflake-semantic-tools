"""SST-PRS004: a block holds a field SST does not model, and none it does is close."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs004_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n    owner: data-team\n")).load(), "SST-PRS004"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:total: unknown field 'owner'"
    assert diagnostic.subject == "metric:total"


def test_sst_prs004_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n")).load(), "SST-PRS004") == []
