"""SST-VAL401: a filter labelled `filter` has an expression that is not boolean."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import project_copy
from tests.helpers.semantic_edits import FILTERS, edited, reported


def test_sst_val401_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        FILTERS,
        "expr: \"{{ ref('orders', 'order_state') }} = '{{ var('completed_state') }}'\"",
        "expr: \"{{ ref('orders', 'order_total') }}\"",
    )
    [diagnostic] = reported(project, "SST-VAL401")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter 'is_completed_order' carries labels: [filter] and its expr is not boolean"
    assert diagnostic.subject == "filter:is_completed_order"


def test_sst_val401_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL401") == []
