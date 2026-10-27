"""SST-PRS014: a metric declares two fields that exclude each other."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import ORDER_BY, SmallProject, metric_file


def test_sst_prs014_fires(tmp_path: Path) -> None:
    files = metric_file("    expr: SUM(x)\n    using_relationships: [r]\n" + ORDER_BY)
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS014")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: 'window' and 'using_relationships' are mutually exclusive"
    assert diagnostic.subject == "metric:total"


def test_sst_prs014_silent(tmp_path: Path) -> None:
    files = metric_file("    expr: SUM(x)\n" + ORDER_BY)
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS014") == []
