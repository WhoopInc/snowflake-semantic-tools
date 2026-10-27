"""SST-VAL319: what each view holds by table membership is reported."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, Relationship, SemanticView, Table
from snowflake_semantic_tools.domain.validate.semantic_view import fan_out_diagnostics
from tests.helpers.sql_values import authored

TABLES = (Table("ORDERS", "DB.S.ORDERS"), Table("CUSTOMERS", "DB.S.CUSTOMERS"))


def test_sst_val319_fires() -> None:
    view = SemanticView(
        "DB.S.SALES",
        TABLES,
        relationships=(Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",)),),
        metrics=(
            Metric("ORDER_COUNT", authored("COUNT(1)"), "ORDERS"),
            Metric("REVENUE", authored("SUM(1)"), "ORDERS"),
        ),
    )
    [found] = fan_out_diagnostics(((view, "semantic_view:sales"),))
    assert found.severity is Severity.INFO
    assert found.message == "semantic_view:sales: 1 relationship, 2 metrics by table membership"
    assert found.subject == "semantic_view:sales"


def test_sst_val319_silent() -> None:
    assert fan_out_diagnostics(((SemanticView("DB.S.EMPTY", TABLES), "semantic_view:empty"),)) == ()
