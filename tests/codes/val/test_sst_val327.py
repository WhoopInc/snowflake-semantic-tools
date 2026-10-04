"""SST-VAL327: a written meta.sst.data_type differs from the relation's type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import Component, WarehouseColumn
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import enrich_findings

WRITTEN = DbtColumn("amount", None, "VARCHAR", "fact", declared_keys=frozenset(("data_type",)))


def test_sst_val327_fires() -> None:
    [found] = enrich_findings(
        WRITTEN, (WarehouseColumn("AMOUNT", "NUMBER(38,0)"),), {}, "SST-VAL327", Component.DATA_TYPES
    )
    assert found.severity is Severity.WARNING
    assert found.message == "model 'orders': column 'amount' declares data_type VARCHAR, and the relation has NUMBER"
    assert found.subject == "dbt_column:orders.amount"


def test_sst_val327_silent() -> None:
    assert (
        enrich_findings(WRITTEN, (WarehouseColumn("AMOUNT", "VARCHAR"),), {}, "SST-VAL327", Component.DATA_TYPES) == []
    )
