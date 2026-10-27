"""SST-VAL306: a time dimension is declared over a non-temporal column."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val306_fires() -> None:
    [found] = dbt_column_findings(DbtColumn("c", "C.", "NUMBER", "time_dimension"), "SST-VAL306")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: time_dimension 'c' has type NUMBER"
    assert found.subject == "dbt_column:m.c"


def test_sst_val306_silent() -> None:
    assert dbt_column_findings(DbtColumn("c", "C.", "TIMESTAMP_NTZ", "time_dimension"), "SST-VAL306") == []
