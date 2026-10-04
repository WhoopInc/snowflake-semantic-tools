"""SST-PRS028: a table's `distinct_range` constraint lacks a start or an end column."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, view_file

CONFIG = "    table_config:\n      products:\n        {entry}\n"


def test_sst_prs028_fires(tmp_path: Path) -> None:
    files = view_file(CONFIG.format(entry="distinct_range: {start: products_id}"))
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS028")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:catalog: constraints block is invalid: "
        "table_config.products.distinct_range needs a start and an end column"
    )
    assert diagnostic.subject == "semantic_view:catalog"


def test_sst_prs028_silent(tmp_path: Path) -> None:
    files = view_file(CONFIG.format(entry="synonyms: [items]"))
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS028") == []
