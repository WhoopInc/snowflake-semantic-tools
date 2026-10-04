"""SST-PRS102: a metric declares `tables: []`, which attaches it to nothing."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import METRICS, SmallProject, metric_file

EMPTY = "snowflake_metrics:\n  - name: total\n    description: Total.\n    tables: []\n    expr: COUNT(*)\n"


def test_sst_prs102_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(SmallProject(tmp_path, files={METRICS: EMPTY}).load().diagnostics, "SST-PRS102")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: 'tables: []' is not accepted"
    assert diagnostic.subject == "metric:total"


def test_sst_prs102_silent(tmp_path: Path) -> None:
    files = metric_file("    expr: COUNT(*)\n")
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS102") == []
