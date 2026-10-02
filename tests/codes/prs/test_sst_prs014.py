"""SST-PRS014: a metric declares two fields that exclude each other."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import ORDER_BY, SmallProject, found, metric_file


def test_sst_prs014_fires(tmp_path: Path) -> None:
    files = metric_file("    expr: SUM(x)\n    using_relationships: [r]\n" + ORDER_BY)
    [diagnostic] = found(SmallProject(tmp_path, files=files).load(), "SST-PRS014")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: 'window' and 'using_relationships' are mutually exclusive"
    assert diagnostic.subject == "metric:total"


def test_sst_prs014_silent(tmp_path: Path) -> None:
    files = metric_file("    expr: SUM(x)\n" + ORDER_BY)
    assert found(SmallProject(tmp_path, files=files).load(), "SST-PRS014") == []
