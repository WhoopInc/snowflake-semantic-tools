"""SST-VAL305: a fact is declared over a non-numeric column."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.validate.semantic.dbt import dbt_column_diagnostics


def _found(column: DbtColumn, code: str) -> list[Diagnostic]:
    model = DbtModel("model.t.m", "m", "DB.S.M", ("id",), (), (DbtColumn("id", "Key.", "NUMBER", "dimension"), column))
    return [item for item in dbt_column_diagnostics({"m": model}) if item.code == code]


def test_sst_val305_fires() -> None:
    [found] = _found(DbtColumn("c", "C.", "VARCHAR", "fact"), "SST-VAL305")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: fact 'c' has type VARCHAR"
    assert found.subject == "dbt_column:m.c"


def test_sst_val305_silent() -> None:
    assert _found(DbtColumn("c", "C.", "NUMBER(38,2)", "fact"), "SST-VAL305") == []
