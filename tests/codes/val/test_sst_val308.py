"""SST-VAL308: a column the semantic layer reads declares no column_type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val308_fires() -> None:
    [found] = dbt_column_findings(DbtColumn("c", "C.", "NUMBER", None), "SST-VAL308")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: column 'c' declares no column_type"
    assert found.subject == "dbt_column:m.c"


def test_sst_val308_silent() -> None:
    assert dbt_column_findings(DbtColumn("c", "C.", "NUMBER", "fact"), "SST-VAL308") == []


def test_sst_val308_silent_for_an_excluded_column() -> None:
    # `exclude: true` keeps the column out of every view, so it needs no role there.
    excluded = DbtColumn("c", "C.", "NUMBER", None, excluded=True)
    assert dbt_column_findings(excluded, "SST-VAL308") == []
