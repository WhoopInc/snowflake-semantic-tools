"""SST-PRS111: a range condition names one column for both its start and its end."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import KEY, SmallProject, found, relationship_file


def test_sst_prs111_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=relationship_file(f"{KEY} BETWEEN {KEY} AND {KEY}")).load(), "SST-PRS111"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "relationship:self_join: range condition uses 'products_id' for both start and end"
    assert diagnostic.subject == "relationship:self_join"


def test_sst_prs111_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=relationship_file(f"{KEY} = {KEY}")).load(), "SST-PRS111") == []
