"""SST-PRS105: a member type's folder holds a list of another member type."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import FILTERS, SmallProject


def test_sst_prs105_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files={FILTERS: "snowflake_metrics: []\n"}).load().diagnostics, "SST-PRS105"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter does not accept members of type metric"
    assert diagnostic.subject is None


def test_sst_prs105_silent(tmp_path: Path) -> None:
    assert (
        coded(
            SmallProject(tmp_path, files={"semantic_models/metrics/m.yml": "snowflake_metrics: []\n"})
            .load()
            .diagnostics,
            "SST-PRS105",
        )
        == []
    )
