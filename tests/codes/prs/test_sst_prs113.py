"""SST-PRS113: a metric's `expr` is a list rather than a SQL string."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs113_fires(tmp_path: Path) -> None:
    [diagnostic] = found(SmallProject(tmp_path, files=metric_file("    expr: [COUNT(*)]\n")).load(), "SST-PRS113")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: 'expr' expects a SQL string, found list"
    assert diagnostic.subject == "metric:total"


def test_sst_prs113_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n")).load(), "SST-PRS113") == []
