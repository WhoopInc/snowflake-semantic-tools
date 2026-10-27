"""SST-PRS107: a member entry has no name."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import FILTERS, SmallProject, filter_entry


def test_sst_prs107_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files={FILTERS: "snowflake_filters:\n  - expr: 'TRUE'\n"}).load().diagnostics,
        "SST-PRS107",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_models/filters/f.yml: filter entry 0 has no name"
    assert diagnostic.subject == "filter:0"


def test_sst_prs107_fires_without_a_list_index_in_a_single_item_file(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files={FILTERS: "filter:\n  expr: 'TRUE'\n"}).load().diagnostics,
        "SST-PRS107",
    )
    assert diagnostic.message == "semantic_models/filters/f.yml: the filter has no name"
    assert diagnostic.origin is not None and (diagnostic.origin.line, diagnostic.origin.col) == (2, 3)


def test_sst_prs107_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files={FILTERS: filter_entry("cheap")}).load().diagnostics, "SST-PRS107") == []
