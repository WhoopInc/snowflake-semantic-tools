"""SST-VAL206: a range relationship attaches to a view whose target table declares no distinct_range."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import edited, found

MENU = "semantic_models/semantic_views/core/semantic_views.yml"
RANGE = "        distinct_range:\n          start: effective_start_at\n          end: effective_end_at\n"


def test_sst_val206_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, MENU, RANGE, "")
    [diagnostic] = found(project, "SST-VAL206")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "relationship 'orders_to_pricing_periods' is a range join and 'pricing_periods' declares no distinct_range"
    )
    assert diagnostic.subject == "semantic_view:jaffle_menu"


def test_sst_val206_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, MENU, RANGE, RANGE), "SST-VAL206") == []
