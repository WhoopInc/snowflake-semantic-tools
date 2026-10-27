"""SST-PRS105: a file in a member type's folder declares a member of another type."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import FILTERS, SmallProject


@pytest.mark.parametrize(
    ("text", "key"),
    [("snowflake_metrics: []\n", "snowflake_metrics"), ("metric:\n  name: m\n  expr: COUNT(*)\n", "metric")],
    ids=["list", "single item"],
)
def test_sst_prs105_fires(tmp_path: Path, text: str, key: str) -> None:
    [diagnostic] = coded(SmallProject(tmp_path, files={FILTERS: text}).load().diagnostics, "SST-PRS105")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"semantic_models/filters/f.yml: filters/ holds filter members, not the metric under '{key}:'"
    )
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
