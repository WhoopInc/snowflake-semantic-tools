"""SST-PRS007: a member name declared in one file is declared again in another of the same folder."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import FILTERS, SmallProject, filter_entry


def test_sst_prs007_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(
            tmp_path, files={FILTERS: filter_entry("cheap"), "semantic_models/filters/g.yml": filter_entry("cheap")}
        )
        .load()
        .diagnostics,
        "SST-PRS007",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter 'cheap' collides with the one in semantic_models/filters/f.yml"
    assert diagnostic.subject == "filter:cheap"


def test_sst_prs007_silent(tmp_path: Path) -> None:
    assert (
        coded(
            SmallProject(
                tmp_path, files={FILTERS: filter_entry("cheap"), "semantic_models/filters/g.yml": filter_entry("dear")}
            )
            .load()
            .diagnostics,
            "SST-PRS007",
        )
        == []
    )
