"""SST-VAL325: enrich finds a column described in YAML that the relation does not have."""

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


STALE = DbtColumn("legacy_code", None, "VARCHAR", "dimension")


def test_sst_val325_fires() -> None:
    [found] = _found(STALE, (WarehouseColumn("ORDER_ID", "NUMBER"),), {}, "SST-VAL325")
    assert found.severity is Severity.WARNING
    assert found.message == "model 'orders': column 'legacy_code' is described in YAML and absent from the relation"
    assert found.subject == "dbt_column:orders.legacy_code"


def test_sst_val325_silent() -> None:
    assert _found(STALE, (WarehouseColumn("LEGACY_CODE", "TEXT"),), {}, "SST-VAL325") == []
