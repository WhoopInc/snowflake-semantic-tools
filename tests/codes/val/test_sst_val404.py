"""SST-VAL404: a filter expression names a column without `ref()`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import edited, member_file, project_copy, reported


def test_sst_val404_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        member_file("filters", "is_large_order"),
        "expr: \"{{ ref('orders', 'order_total') }} > large_order_cents\"",
        # The ref() gives the filter its table; the bare `tax_paid` beside it is the finding.
        "expr: \"{{ ref('orders', 'order_total') }} > tax_paid\"",
    )
    [diagnostic] = reported(project, "SST-VAL404")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "filter 'is_large_order' expression contains bare identifier 'tax_paid'"
    assert diagnostic.subject == "filter:is_large_order"


def test_sst_val404_silent(tmp_path: Path) -> None:
    # `large_order_cents` is bare too, but it is the view's variable, not a column.
    assert reported(project_copy(tmp_path), "SST-VAL404") == []
