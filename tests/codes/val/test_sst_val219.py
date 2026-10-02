"""SST-VAL219: a column under table_config.<model>.distinct_range does not exist on the model."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import edited, found

MENU = "semantic_models/semantic_views/core/semantic_views.yml"
END = "          end: effective_end_at\n"


def test_sst_val219_fires(tmp_path: Path) -> None:
    [diagnostic] = found(edited(tmp_path, MENU, END, "          end: closes_at\n"), "SST-VAL219")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:jaffle_menu: table_config.pricing_periods.distinct_range.end names column 'closes_at', "
        "which does not exist on pricing_periods"
    )
    assert diagnostic.subject == "semantic_view:jaffle_menu"


def test_sst_val219_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, MENU, END, END), "SST-VAL219") == []
