"""SST-PRS003: a field holds a value of the wrong type, here a metric's `tables`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import METRICS, SmallProject, metric_file


def test_sst_prs003_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(
            tmp_path,
            files={
                METRICS: "snowflake_metrics:\n  - name: total\n    description: T.\n    tables: x\n    expr: COUNT(*)\n"
            },
        )
        .load()
        .diagnostics,
        "SST-PRS003",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: 'tables' expects a list, found str"
    assert diagnostic.subject == "metric:total"


def test_sst_prs003_silent(tmp_path: Path) -> None:
    assert (
        coded(SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n")).load().diagnostics, "SST-PRS003") == []
    )
