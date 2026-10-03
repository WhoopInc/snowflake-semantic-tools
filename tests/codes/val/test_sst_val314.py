"""SST-VAL314: an is_enum column declares no sample values."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.validate.semantic.dbt import dbt_column_diagnostics


def _found(column: DbtColumn, code: str) -> list[Diagnostic]:
    model = DbtModel("model.t.m", "m", "DB.S.M", ("id",), (), (DbtColumn("id", "Key.", "NUMBER", "dimension"), column))
    return [item for item in dbt_column_diagnostics({"m": model}) if item.code == code]


def test_sst_val314_fires() -> None:
    [found] = _found(DbtColumn("c", "C.", "VARCHAR", "dimension", is_enum=True), "SST-VAL314")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: 'c' is is_enum and declares no sample_values"
    assert found.subject == "dbt_column:m.c"


def test_sst_val314_silent() -> None:
    assert _found(DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a",), is_enum=True), "SST-VAL314") == []
