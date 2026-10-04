"""SST-VAL305: a fact is declared over a non-numeric column."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val305_fires() -> None:
    [found] = dbt_column_findings(DbtColumn("c", "C.", "VARCHAR", "fact"), "SST-VAL305")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: fact 'c' has type VARCHAR"
    assert found.subject == "dbt_column:m.c"


def test_sst_val305_silent() -> None:
    assert dbt_column_findings(DbtColumn("c", "C.", "NUMBER(38,2)", "fact"), "SST-VAL305") == []
