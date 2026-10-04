"""SST-VAL304: a view declares max_staleness under the 120-second floor."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import VIEWS, edited, reported

STALENESS = "    max_staleness: 300\n"


def test_sst_val304_fires(tmp_path: Path) -> None:
    [diagnostic] = reported(edited(tmp_path, VIEWS, STALENESS, "    max_staleness: 60\n"), "SST-VAL304")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:jaffle_sales: max_staleness is 60; the minimum is 120"
    assert diagnostic.subject == "semantic_view:jaffle_sales"


def test_sst_val304_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, VIEWS, STALENESS, "    max_staleness: 120\n"), "SST-VAL304") == []
