"""SST-VAL328: a column with pii_tags carries sample values."""

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


EMAIL = (WarehouseColumn("EMAIL", "TEXT"),)


def test_sst_val328_fires() -> None:
    tagged = DbtColumn("email", None, "VARCHAR", "dimension", sample_values=("a@b.c",), pii_tagged=True)
    [found] = _found(tagged, EMAIL, {"email": ["x@y.z"]}, "SST-VAL328", Component.SAMPLE_VALUES)
    assert found.severity is Severity.WARNING
    assert found.message == "model 'orders': column 'email' carries pii_tags and 1 sample_values"
    assert found.subject == "dbt_column:orders.email"


def test_sst_val328_silent() -> None:
    untagged = DbtColumn("email", None, "VARCHAR", "dimension", pii_tagged=True)
    assert _found(untagged, EMAIL, {"email": ["x@y.z"]}, "SST-VAL328", Component.SAMPLE_VALUES) == []
