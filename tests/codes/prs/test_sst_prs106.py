"""SST-PRS106: one file declares a member name twice."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import FILTERS, SmallProject, filter_entry, found


def test_sst_prs106_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files={FILTERS: filter_entry("cheap") + filter_entry("cheap").split("\n", 1)[1]}).load(),
        "SST-PRS106",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_models/filters/f.yml: filter 'cheap' is declared twice"
    assert diagnostic.subject == "filter:cheap"


def test_sst_prs106_silent(tmp_path: Path) -> None:
    assert (
        found(
            SmallProject(
                tmp_path, files={FILTERS: filter_entry("cheap") + filter_entry("dear").split("\n", 1)[1]}
            ).load(),
            "SST-PRS106",
        )
        == []
    )
