"""SST-VAL325: enrich finds a column described in YAML that the relation does not have."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import WarehouseColumn
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import enrich_findings

STALE = DbtColumn("legacy_code", None, "VARCHAR", "dimension")


def test_sst_val325_fires() -> None:
    [found] = enrich_findings(STALE, (WarehouseColumn("ORDER_ID", "NUMBER"),), {}, "SST-VAL325")
    assert found.severity is Severity.WARNING
    assert found.message == "model 'orders': column 'legacy_code' is described in YAML and absent from the relation"
    assert found.subject == "dbt_column:orders.legacy_code"


def test_sst_val325_silent() -> None:
    assert enrich_findings(STALE, (WarehouseColumn("LEGACY_CODE", "TEXT"),), {}, "SST-VAL325") == []
