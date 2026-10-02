"""SST-VAL216: two tables join many-to-many through a bridge table."""

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


def test_sst_val216_fires() -> None:
    [found] = _found(TO_CUSTOMERS, TO_LOCATIONS, code="SST-VAL216")
    assert found.severity is Severity.INFO
    assert found.message == "semantic_view:sales: many-to-many path CUSTOMERS <-> LOCATIONS inferred through 'ORDERS'"
    assert found.subject == "semantic_view:sales"


def test_sst_val216_silent() -> None:
    assert _found(TO_CUSTOMERS, code="SST-VAL216") == []
