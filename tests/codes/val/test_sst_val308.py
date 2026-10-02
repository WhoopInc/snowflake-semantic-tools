"""SST-VAL308: a column the semantic layer reads declares no column_type."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _dbt_column_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel


def _found(column: DbtColumn, code: str) -> list[Diagnostic]:
    model = DbtModel("model.t.m", "m", "DB.S.M", ("id",), (), (DbtColumn("id", "Key.", "NUMBER", "dimension"), column))
    return [item for item in _dbt_column_diagnostics({"m": model}) if item.code == code]


def test_sst_val308_fires() -> None:
    [found] = _found(DbtColumn("c", "C.", "NUMBER", None), "SST-VAL308")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: column 'c' declares no column_type"
    assert found.subject == "dbt_column:m.c"


def test_sst_val308_silent() -> None:
    assert _found(DbtColumn("c", "C.", "NUMBER", "fact"), "SST-VAL308") == []
