"""SST-PRS110: a relationship condition is not one equality, ASOF comparison or range."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import KEY, SmallProject, relationship_file


def test_sst_prs110_fires(tmp_path: Path) -> None:
    files = relationship_file(f"{KEY} == {KEY}")
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS110")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == f"relationship:self_join: condition '{KEY} == {KEY}' does not parse to one left/right pair"
    )
    assert diagnostic.subject == "relationship:self_join"


def test_sst_prs110_silent(tmp_path: Path) -> None:
    files = relationship_file(f"{KEY} = {KEY}")
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS110") == []
