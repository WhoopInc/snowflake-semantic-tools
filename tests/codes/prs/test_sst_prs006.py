"""SST-PRS006: a view lists one model as a table twice."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, view_file


def test_sst_prs006_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=view_file("      - \"{{ ref('products') }}\"\n")).load(), "SST-PRS006"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "table 'products' is declared more than once"
    assert diagnostic.subject == "semantic_view:catalog"


def test_sst_prs006_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=view_file("")).load(), "SST-PRS006") == []
