"""SST-VAL309: a column has no data_type from dbt or from meta.sst."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val309_fires() -> None:
    [found] = dbt_column_findings(DbtColumn("c", "C.", None, "dimension"), "SST-VAL309")
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:m: column 'c' declares no data_type"
    assert found.subject == "dbt_column:m.c"


def test_sst_val309_silent() -> None:
    assert dbt_column_findings(DbtColumn("c", "C.", "VARCHAR", "dimension"), "SST-VAL309") == []
