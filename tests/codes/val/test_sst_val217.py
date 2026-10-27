"""SST-VAL217: a view's join graph shape is reported."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from tests.helpers.val_codes import join_graph_findings

TO_CUSTOMERS = Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",))
TO_LOCATIONS = Relationship("ORDERS_TO_LOCATIONS", "ORDERS", ("LOCATION_ID",), "LOCATIONS", ("LOCATION_ID",))


def test_sst_val217_fires() -> None:
    [found] = join_graph_findings(TO_CUSTOMERS, code="SST-VAL217")
    assert found.severity is Severity.INFO
    assert found.message == "semantic_view:sales: 3 tables, 1 relationships in 2 connected parts"
    assert found.subject == "semantic_view:sales"


def test_sst_val217_silent() -> None:
    # A view without relationships has no join graph to describe.
    assert join_graph_findings(code="SST-VAL217") == []
