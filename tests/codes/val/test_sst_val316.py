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


def test_sst_val316_fires_on_nat_in_text() -> None:
    [found] = dbt_column_findings(DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("NaT",)), "SST-VAL316")
    assert found.message == "dbt_model:m: 'c'.sample_values contains 'NaT'"


def test_sst_val316_leaves_a_value_the_type_refuses_to_sst_val341() -> None:
    column = DbtColumn("c", "C.", "TIMESTAMP_NTZ", "time_dimension", sample_values=("NaT",))
    assert dbt_column_findings(column, "SST-VAL316") == []
    assert len(dbt_column_findings(column, "SST-VAL341")) == 1


def test_sst_val316_still_warns_on_nan_in_a_float() -> None:
    column = DbtColumn("c", "C.", "FLOAT", "fact", sample_values=("nan",))
    assert len(dbt_column_findings(column, "SST-VAL316")) == 1
    assert dbt_column_findings(column, "SST-VAL341") == []


def test_sst_val316_silent() -> None:
    assert (
        dbt_column_findings(DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "nanny")), "SST-VAL316")
        == []
    )
