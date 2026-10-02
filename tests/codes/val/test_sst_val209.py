"""SST-VAL209: two relationships join the same two tables of a view."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _multipath_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship

VIEWS = (("semantic_view:v", frozenset(("orders", "customers"))),)
BILLED = Relationship("ORDERS_TO_BILLED", "ORDERS", ("BILLED_ID",), "CUSTOMERS", ("CUSTOMER_ID",))
SHIPPED = Relationship("ORDERS_TO_SHIPPED", "ORDERS", ("SHIPPED_ID",), "CUSTOMERS", ("CUSTOMER_ID",))


def test_sst_val209_fires() -> None:
    [found] = [item for item in _multipath_diagnostics((BILLED, SHIPPED), (), VIEWS) if item.code == "SST-VAL209"]
    assert found.severity is Severity.WARNING
    assert found.message == "semantic_view:v: 2 paths between 'customers' and 'orders'"


def test_sst_val209_silent() -> None:
    assert [item for item in _multipath_diagnostics((BILLED,), (), VIEWS) if item.code == "SST-VAL209"] == []
