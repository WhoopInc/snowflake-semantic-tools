"""SST-PRS124: a metric window's `frame` is not a frame clause."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import ORDER_BY, SmallProject, found, metric_file


def test_sst_prs124_fires(tmp_path: Path) -> None:
    files = metric_file("    expr: SUM(x)\n" + ORDER_BY + "      frame: ROWS 2 PRECEDING\n")
    [diagnostic] = found(SmallProject(tmp_path, files=files).load(), "SST-PRS124")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "metric:total: window frame 'ROWS 2 PRECEDING' is not ROWS or RANGE BETWEEN <bound> AND <bound>"
    )
    assert diagnostic.subject == "metric:total"


def test_sst_prs124_silent(tmp_path: Path) -> None:
    files = metric_file("    expr: SUM(x)\n" + ORDER_BY + "      frame: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW\n")
    assert found(SmallProject(tmp_path, files=files).load(), "SST-PRS124") == []
