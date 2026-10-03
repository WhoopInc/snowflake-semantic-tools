"""SST-VAL210: no key of a join's target lies within its join columns."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from snowflake_semantic_tools.domain.validate.semantic.joins import key_diagnostic
from tests.helpers.semantic_members import ORDERS


def test_sst_val210_fires() -> None:
    off_key = Relationship("X_TO_ORDERS", "X", ("CUSTOMER_ID",), "ORDERS", ("CUSTOMER_ID",))
    found = key_diagnostic(off_key, ORDERS, subject="relationship:x_to_orders", origin=None)
    assert found is not None and found.code == "SST-VAL210"
    assert found.severity is Severity.WARNING
    assert (
        found.message
        == "relationship 'x_to_orders': 'orders' declares neither primary_key nor unique_keys over CUSTOMER_ID"
    )
    assert found.subject == "relationship:x_to_orders"


def test_sst_val210_silent() -> None:
    on_key = Relationship("X_TO_ORDERS", "X", ("ORDER_ID",), "ORDERS", ("ORDER_ID",))
    assert key_diagnostic(on_key, ORDERS, subject="relationship:x_to_orders", origin=None) is None
