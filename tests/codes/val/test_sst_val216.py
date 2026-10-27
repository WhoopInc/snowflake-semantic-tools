"""SST-VAL216: two tables join many-to-many through a bridge table."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from tests.helpers.val_codes import join_graph_findings

TO_CUSTOMERS = Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",))
TO_LOCATIONS = Relationship("ORDERS_TO_LOCATIONS", "ORDERS", ("LOCATION_ID",), "LOCATIONS", ("LOCATION_ID",))


def test_sst_val216_fires() -> None:
    [found] = join_graph_findings(TO_CUSTOMERS, TO_LOCATIONS, code="SST-VAL216")
    assert found.severity is Severity.INFO
    assert found.message == "semantic_view:sales: many-to-many path CUSTOMERS <-> LOCATIONS inferred through 'ORDERS'"
    assert found.subject == "semantic_view:sales"


def test_sst_val216_silent() -> None:
    assert join_graph_findings(TO_CUSTOMERS, code="SST-VAL216") == []
