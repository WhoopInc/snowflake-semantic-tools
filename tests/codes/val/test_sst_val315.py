"""SST-VAL315: a non-enum column declares sample values that look exhaustive."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val315_fires() -> None:
    [found] = dbt_column_findings(
        DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "b", "c", "d", "e")), "SST-VAL315"
    )
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:m: 'c' declares 5 sample_values and is not is_enum"
    assert found.subject == "dbt_column:m.c"


def test_sst_val315_silent() -> None:
    assert (
        dbt_column_findings(
            DbtColumn("c", "C.", "VARCHAR", "dimension", sample_values=("a", "b", "c", "d", "e"), is_enum=False),
            "SST-VAL315",
        )
        == []
    )
