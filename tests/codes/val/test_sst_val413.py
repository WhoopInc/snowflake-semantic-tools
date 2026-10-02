"""SST-VAL413: two verified queries one view attaches share their question."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import project_copy
from tests.helpers.semantic_edits import QUERIES, edited, reported


def test_sst_val413_fires(tmp_path: Path) -> None:
    shared = '    question: "how many orders are there in each   fulfilment state?"\n'
    found = reported(edited(tmp_path, QUERIES, '    question: "What is our total revenue?"\n', shared), "SST-VAL413")
    assert found and all(item.severity is Severity.ERROR for item in found)
    sales = next(item for item in found if item.subject == "semantic_view:jaffle_sales")
    assert sales.message == (
        "semantic_view:jaffle_sales: question text is shared by 'order_count_by_state' and 'total_revenue_all_time'"
    )


def test_sst_val413_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL413") == []
