"""SST-VAL403: a view declares the 0.3 inline `filters:` list."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.reference_project import VIEWS, edited, project_copy, reported

STALENESS = "    max_staleness: 300\n"


def test_sst_val403_fires(tmp_path: Path) -> None:
    inline = STALENESS + "    filters:\n      - name: completed_only\n        expr: order_state = 'completed'\n"
    [diagnostic] = reported(edited(tmp_path, VIEWS, STALENESS, inline), "SST-VAL403")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter 'completed_only' uses the legacy inline form"
    assert diagnostic.subject == "semantic_view:jaffle_sales"
    assert not ERROR_REGISTRY["SST-VAL403"].demotable


def test_sst_val403_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL403") == []
