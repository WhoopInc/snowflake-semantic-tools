"""SST-VAL327: a written meta.sst.data_type differs from the relation's type."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.enrich import Component, WarehouseColumn
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from snowflake_semantic_tools.domain.render.semantic_view import render
from tests.helpers.diagnostic_filters import coded
from tests.helpers.sql_values import authored
from tests.helpers.val_codes import TypedWarehouse, enrich_findings

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


def _connected_drift(declared: str, described: str) -> list[Diagnostic]:
    column = Column("ORDERS", "AMOUNT", ColumnKind.DIMENSION, authored("ORDERS.AMOUNT"), data_type=declared)
    view = SemanticView("DB.S.SALES", (Table("ORDERS", "DB.S.ORDERS"),), columns=(column,))
    port = TypedWarehouse({"DB.S.ORDERS": (("AMOUNT", described),)})
    result = ValidateArtifacts(port).run(
        CompileResult((CompiledView(view, render(view)),)), strict=False, connected=True
    )
    return coded(result.diagnostics, "SST-VAL327")


def test_sst_val327_fires_connected_on_a_declared_type_the_table_contradicts() -> None:
    [found] = _connected_drift("TEXT", "FLOAT")
    assert found.message == "model 'orders': column 'amount' declares data_type TEXT, and the relation has FLOAT"
    assert found.subject == "dbt_column:orders.amount"


def test_sst_val327_silent_connected_on_another_spelling_of_the_type() -> None:
    assert _connected_drift("bigint", "NUMBER(38,0)") == []
