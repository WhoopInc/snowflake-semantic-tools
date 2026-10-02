"""SST-VAL327: a written meta.sst.data_type differs from the relation's type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.enrich import Component, WarehouseColumn, enrich_model, resolve_options
from snowflake_semantic_tools.domain.model.config_schema import EnrichmentConfig
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel

SETTINGS = EnrichmentConfig(distinct_limit=3, display_limit=2)


def _found(
    column: DbtColumn,
    warehouse: tuple[WarehouseColumn, ...],
    samples: dict[str, list[str]],
    code: str,
    *components: Component,
) -> list[Diagnostic]:
    model = DbtModel("model.p.orders", "orders", "DB.S.ORDERS", (), (), (column,))
    options = resolve_options(frozenset(components), frozenset())
    result = enrich_model(model, warehouse, options, SETTINGS, samples=samples, synonyms={})
    return [item for item in result.diagnostics if item.code == code]


WRITTEN = DbtColumn("amount", None, "VARCHAR", "fact", declared_keys=frozenset(("data_type",)))


def test_sst_val327_fires() -> None:
    [found] = _found(WRITTEN, (WarehouseColumn("AMOUNT", "NUMBER(38,0)"),), {}, "SST-VAL327", Component.DATA_TYPES)
    assert found.severity is Severity.WARNING
    assert found.message == "model 'orders': column 'amount' declares data_type VARCHAR, and the relation has NUMBER"
    assert found.subject == "dbt_column:orders.amount"


def test_sst_val327_silent() -> None:
    assert _found(WRITTEN, (WarehouseColumn("AMOUNT", "VARCHAR"),), {}, "SST-VAL327", Component.DATA_TYPES) == []
