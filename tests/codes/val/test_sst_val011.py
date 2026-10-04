"""SST-VAL011: an authored value the loader reads would be dropped or altered by the renderer."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import FILTERS, QUERIES, edited, project_copy, reported


def test_sst_val011_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, FILTERS, "    labels:\n      - filter\n", "    labels:\n      - filter\n      - kpi\n")
    [diagnostic] = reported(project, "SST-VAL011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter 'is_completed_order': 'labels: kpi' would be dropped by the renderer"
    assert diagnostic.subject == "filter:is_completed_order"


def test_sst_val011_fires_for_a_verified_by_the_loader_trims(tmp_path: Path) -> None:
    project = edited(tmp_path, QUERIES, 'verified_by: "daa-platform"', 'verified_by: " daa-platform "')
    [diagnostic] = reported(project, "SST-VAL011")
    assert diagnostic.message.endswith(": 'verified_by' would be trimmed by the renderer")
    assert (diagnostic.subject or "").startswith("verified_query:")


def test_sst_val011_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL011") == []
