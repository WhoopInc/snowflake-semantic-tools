"""SST-PRS005: a metric name is not one Snowflake identifier."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs005_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n", name="total count")).load(), "SST-PRS005"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total count: 'total count' is not a valid identifier"
    assert diagnostic.subject == "metric:total count"


def test_sst_prs005_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n")).load(), "SST-PRS005") == []
