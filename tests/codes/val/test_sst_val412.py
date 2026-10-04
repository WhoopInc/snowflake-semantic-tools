"""SST-VAL412: a verified query declares both `sql` and `sql_file`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import QUERIES, edited, project_copy, reported


def test_sst_val412_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        QUERIES,
        '    question: "What is our total revenue?"\n',
        '    question: "What is our total revenue?"\n    sql_file: sql/product_mix_by_type.sql\n',
    )
    [diagnostic] = reported(project, "SST-VAL412")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "verified_query 'total_revenue_all_time': sql and sql_file are both present"
    assert diagnostic.subject == "verified_query:total_revenue_all_time"


def test_sst_val412_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL412") == []
