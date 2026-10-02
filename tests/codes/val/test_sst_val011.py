"""SST-VAL011: a declared value would not reach the published object: a filter's synonyms."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import FILTERS, edited, found

NAME = "  - name: is_completed_order\n"


def test_sst_val011_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, FILTERS, NAME, NAME + "    synonyms:\n      - finished\n")
    [diagnostic] = found(project, "SST-VAL011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter 'is_completed_order': 'synonyms' would be dropped by the renderer"
    assert diagnostic.subject == "filter:is_completed_order"


def test_sst_val011_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, FILTERS, NAME, NAME), "SST-VAL011") == []
