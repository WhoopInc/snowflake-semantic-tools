"""SST-PRS112: a relationship declares more than one ASOF condition."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import KEY, SmallProject, relationship_file


def test_sst_prs112_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files=relationship_file(f"{KEY} >= {KEY}", f"{KEY} >= {KEY}")).load().diagnostics,
        "SST-PRS112",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "relationship:self_join: 2 asof conditions declared"
    assert diagnostic.subject == "relationship:self_join"


def test_sst_prs112_silent(tmp_path: Path) -> None:
    assert (
        coded(
            SmallProject(tmp_path, files=relationship_file(f"{KEY} = {KEY}", f"{KEY} >= {KEY}")).load().diagnostics,
            "SST-PRS112",
        )
        == []
    )
