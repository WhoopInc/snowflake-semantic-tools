"""SST-VAL317: a written is_enum, a field enrich owns, differs from what the data supports."""

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


KIND = (WarehouseColumn("KIND", "TEXT"),)


def _kind(**fields: object) -> DbtColumn:
    return DbtColumn("kind", None, None, "dimension", declared_keys=frozenset(("is_enum",)), **fields)  # type: ignore[arg-type]


def test_sst_val317_fires() -> None:
    [found] = _found(_kind(is_enum=True), KIND, {"kind": ["k1", "k2", "k3", "k4"]}, "SST-VAL317", Component.ENUMS)
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:orders: 'kind'.is_enum differs from the enriched value"
    assert found.subject == "dbt_column:orders.kind"


def test_sst_val317_silent() -> None:
    assert _found(_kind(is_enum=True), KIND, {"kind": ["k1", "k2"]}, "SST-VAL317", Component.ENUMS) == []
