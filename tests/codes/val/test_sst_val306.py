"""SST-VAL306: a time dimension is declared over a non-temporal column."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.validate.semantic.dbt import dbt_column_diagnostics


def _found(column: DbtColumn, code: str) -> list[Diagnostic]:
    model = DbtModel("model.t.m", "m", "DB.S.M", ("id",), (), (DbtColumn("id", "Key.", "NUMBER", "dimension"), column))
    return [item for item in dbt_column_diagnostics({"m": model}) if item.code == code]


def test_sst_val306_fires() -> None:
    [found] = _found(DbtColumn("c", "C.", "NUMBER", "time_dimension"), "SST-VAL306")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: time_dimension 'c' has type NUMBER"
    assert found.subject == "dbt_column:m.c"


def test_sst_val306_silent() -> None:
    assert _found(DbtColumn("c", "C.", "TIMESTAMP_NTZ", "time_dimension"), "SST-VAL306") == []
