"""SST-VAL316: an auto-managed field holds a missing value written as text."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val316_fires() -> None:
    [found] = dbt_column_findings(
        DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "nan")), "SST-VAL316"
    )
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:m: 'c'.sample_values contains 'nan'"
    assert found.subject == "dbt_column:m.c"


def test_sst_val316_silent() -> None:
    assert (
        dbt_column_findings(DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "nanny")), "SST-VAL316")
        == []
    )
