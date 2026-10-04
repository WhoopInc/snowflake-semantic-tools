"""SST-VAL314: an is_enum column declares no sample values."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val314_fires() -> None:
    [found] = dbt_column_findings(DbtColumn("c", "C.", "VARCHAR", "dimension", is_enum=True), "SST-VAL314")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: 'c' is is_enum and declares no sample_values"
    assert found.subject == "dbt_column:m.c"


def test_sst_val314_silent() -> None:
    assert (
        dbt_column_findings(
            DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a",), is_enum=True), "SST-VAL314"
        )
        == []
    )
