"""SST-VAL315: a non-enum column declares sample values that look exhaustive."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.validate.semantic.dbt import dbt_column_diagnostics


def _found(column: DbtColumn, code: str) -> list[Diagnostic]:
    model = DbtModel("model.t.m", "m", "DB.S.M", ("id",), (), (DbtColumn("id", "Key.", "NUMBER", "dimension"), column))
    return [item for item in dbt_column_diagnostics({"m": model}) if item.code == code]


def test_sst_val315_fires() -> None:
    [found] = _found(
        DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "b", "c", "d", "e")), "SST-VAL315"
    )
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:m: 'c' declares 5 sample_values and is not is_enum"
    assert found.subject == "dbt_column:m.c"


def test_sst_val315_silent() -> None:
    assert (
        _found(
            DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "b", "c", "d", "e"), is_enum=False),
            "SST-VAL315",
        )
        == []
    )
