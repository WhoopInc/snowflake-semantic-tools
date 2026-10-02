"""SST-PRS011: a relationship name needs quoting to be an identifier, and is not written quoted."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import KEY, SmallProject, found, relationship_file


def test_sst_prs011_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=relationship_file(f"{KEY} = {KEY}", name="self join")).load(), "SST-PRS011"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "'self join' needs quoting to render safely"
    assert diagnostic.subject == "relationship:self join"


def test_sst_prs011_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=relationship_file(f"{KEY} = {KEY}")).load(), "SST-PRS011") == []
