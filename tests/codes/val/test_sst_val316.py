"""SST-VAL316: an auto-managed field holds a missing value written as text."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _dbt_column_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel


def _found(column: DbtColumn, code: str) -> list[Diagnostic]:
    model = DbtModel("model.t.m", "m", "DB.S.M", ("id",), (), (DbtColumn("id", "Key.", "NUMBER", "dimension"), column))
    return [item for item in _dbt_column_diagnostics({"m": model}) if item.code == code]


def test_sst_val316_fires() -> None:
    [found] = _found(DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "nan")), "SST-VAL316")
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:m: 'c'.sample_values contains 'nan'"
    assert found.subject == "dbt_column:m.c"


def test_sst_val316_silent() -> None:
    assert _found(DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "nanny")), "SST-VAL316") == []
