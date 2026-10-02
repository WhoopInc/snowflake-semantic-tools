"""SST-PRS109: a view's `table_config` names a table its `tables` does not list."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, view_file


def test_sst_prs109_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=view_file("    table_config:\n      orders:\n        synonyms: [o]\n")).load(),
        "SST-PRS109",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:catalog: table_config key 'orders' is not in tables:"
    assert diagnostic.subject == "semantic_view:catalog"


def test_sst_prs109_silent(tmp_path: Path) -> None:
    assert (
        found(
            SmallProject(
                tmp_path, files=view_file("    table_config:\n      products:\n        synonyms: [items]\n")
            ).load(),
            "SST-PRS109",
        )
        == []
    )
