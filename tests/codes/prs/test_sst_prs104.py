"""SST-PRS104: one member is declared in the folders of two owning types."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import FILTERS, SmallProject, filter_entry, found


def test_sst_prs104_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(
            tmp_path,
            files={FILTERS: filter_entry("cheap"), "semantic_models/semantic_views/more.yml": filter_entry("cheap")},
        ).load(),
        "SST-PRS104",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter:cheap is declared under both filters/ and semantic_views/"
    assert diagnostic.subject == "filter:cheap"


def test_sst_prs104_silent(tmp_path: Path) -> None:
    assert (
        found(
            SmallProject(
                tmp_path,
                files={FILTERS: filter_entry("cheap"), "semantic_models/semantic_views/more.yml": filter_entry("dear")},
            ).load(),
            "SST-PRS104",
        )
        == []
    )
