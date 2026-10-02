"""SST-VAL217: a view's join graph shape is reported."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, SemanticView, Table
from snowflake_semantic_tools.domain.validate.semantic_view import join_graph_diagnostics

TABLES = tuple(Table(name, f"DB.S.{name}") for name in ("ORDERS", "CUSTOMERS", "LOCATIONS"))
TO_CUSTOMERS = Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",))
TO_LOCATIONS = Relationship("ORDERS_TO_LOCATIONS", "ORDERS", ("LOCATION_ID",), "LOCATIONS", ("LOCATION_ID",))


def _found(*relationships: Relationship, code: str) -> list[Diagnostic]:
    view = SemanticView("DB.S.SALES", TABLES, relationships=relationships)
    return [item for item in join_graph_diagnostics(view, artifact="semantic_view:sales") if item.code == code]


def test_sst_val217_fires() -> None:
    [found] = _found(TO_CUSTOMERS, code="SST-VAL217")
    assert found.severity is Severity.INFO
    assert found.message == "semantic_view:sales: 3 tables, 1 relationships in 2 connected parts"
    assert found.subject == "semantic_view:sales"


def test_sst_val217_silent() -> None:
    # A view without relationships has no join graph to describe.
    assert _found(code="SST-VAL217") == []
