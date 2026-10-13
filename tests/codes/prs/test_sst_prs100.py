"""SST-PRS100: a member name starts with a prefix SST or Snowflake reserves."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import COUNTED, SmallProject, metric_file


def test_sst_prs100_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files=metric_file(COUNTED, name="sst_total")).load().diagnostics,
        "SST-PRS100",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "'sst_total' is in a reserved namespace"
    assert diagnostic.subject == "metric:sst_total"


def test_sst_prs100_silent(tmp_path: Path) -> None:
    assert (
        coded(
            SmallProject(tmp_path, files=metric_file(COUNTED, name="total_sst")).load().diagnostics,
            "SST-PRS100",
        )
        == []
    )
