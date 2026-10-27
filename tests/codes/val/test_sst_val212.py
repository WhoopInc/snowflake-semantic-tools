"""SST-VAL212: the connected spot check finds a join target repeating a join key."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, Relationship, SemanticView, Table
from tests.helpers.sql_values import authored
from tests.helpers.val_codes import validated_live

VIEW = SemanticView(
    "DB.S.SALES",
    (Table("ORDERS", "DB.S.ORDERS"), Table("CUSTOMERS", "DB.S.CUSTOMERS", primary_key=("CUSTOMER_ID",))),
    metrics=(Metric("ROWS", authored("COUNT(1)"), "ORDERS"),),
    relationships=(Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",)),),
)


def test_sst_val212_fires() -> None:
    [found] = [item for item in validated_live(VIEW, {"COUNT(DISTINCT": (3,)}) if item.code == "SST-VAL212"]
    assert found.severity is Severity.WARNING
    assert found.message == (
        "relationship 'orders_to_customers' declares cardinality many-to-one; "
        "the spot-check found 3 rows of 'CUSTOMERS' repeat a join key"
    )
    assert found.subject == "semantic_view:sales"


def test_sst_val212_silent() -> None:
    assert [item for item in validated_live(VIEW, {"COUNT(DISTINCT": (0,)}) if item.code == "SST-VAL212"] == []
