"""SST-VAL317: a written is_enum, a field enrich owns, differs from what the data supports."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import Component, WarehouseColumn
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import enrich_findings

KIND = (WarehouseColumn("KIND", "TEXT"),)


def _kind(**fields: object) -> DbtColumn:
    return DbtColumn("kind", None, None, "dimension", declared_keys=frozenset(("is_enum",)), **fields)  # type: ignore[arg-type]


def test_sst_val317_fires() -> None:
    [found] = enrich_findings(
        _kind(is_enum=True), KIND, {"kind": ["k1", "k2", "k3", "k4"]}, "SST-VAL317", Component.ENUMS
    )
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:orders: 'kind'.is_enum differs from the enriched value"
    assert found.subject == "dbt_column:orders.kind"


def test_sst_val317_silent() -> None:
    assert enrich_findings(_kind(is_enum=True), KIND, {"kind": ["k1", "k2"]}, "SST-VAL317", Component.ENUMS) == []
