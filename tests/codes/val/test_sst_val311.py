"""SST-VAL311: a relationship references a table that declares no key."""

from __future__ import annotations

import dataclasses

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _relationship_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from tests.helpers.semantic_members import ORDERS

JOIN = Relationship("ITEMS_TO_ORDERS", "ITEMS", ("ORDER_ID",), "ORDERS", ("ORDER_ID",))
VIEWS = (("semantic_view:v", frozenset(("items", "orders"))),)


def test_sst_val311_fires() -> None:
    keyless = dataclasses.replace(ORDERS, primary_key=())
    [found] = _relationship_diagnostics((JOIN,), VIEWS, models={"orders": keyless})
    assert found.severity is Severity.ERROR
    assert found.message == (
        "relationship:items_to_orders: 'orders' declares neither primary_key nor unique_keys, "
        "and a relationship references it"
    )
    assert found.subject == "relationship:items_to_orders"


def test_sst_val311_silent() -> None:
    assert _relationship_diagnostics((JOIN,), VIEWS, models={"orders": ORDERS}) == ()
