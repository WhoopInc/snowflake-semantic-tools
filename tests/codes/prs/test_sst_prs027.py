"""SST-PRS027: a view's `tags` is not a list of name and value entries."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, view_file


def test_sst_prs027_fires(tmp_path: Path) -> None:
    [diagnostic] = found(SmallProject(tmp_path, files=view_file("    tags: {tier: gold}\n")).load(), "SST-PRS027")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:catalog: tags must be a list of name and value entries, found dict"
    assert diagnostic.subject == "semantic_view:catalog"


def test_sst_prs027_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=view_file("")).load(), "SST-PRS027") == []
