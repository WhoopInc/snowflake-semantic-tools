from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.lineage.members import MemberEdges, member_edges
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    Table,
    VerifiedQuery,
)
from tests.helpers.sql_values import authored, authored_query


def _model(name: str) -> DbtModel:
    return DbtModel(f"model.p.{name}", name, f"DB.S.{name.upper()}", (), (), ())


def _view() -> SemanticView:
    return SemanticView(
        fqn="DB.S.SALES",
        tables=(
            Table(logical_name="ORDERS", fqn="DB.S.ORDERS"),
            Table(logical_name="CUSTOMERS", fqn="DB.S.CUSTOMERS"),
            Table(logical_name="EVENTS", fqn="RAW.APP.EVENTS"),
            Table(logical_name="ORPHAN", fqn="X.Y.ORPHAN"),
        ),
        relationships=(Relationship("orders_to_customers", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("ID",)),),
        columns=(
            Column(table="ORDERS", name="order_total", kind=ColumnKind.FACT, expr=authored("ORDERS.ORDER_TOTAL")),
            Column(table="ORDERS", name="ordered_at", kind=ColumnKind.TIME_DIMENSION, expr=authored("ORDERS.AT")),
            Column(table="EVENTS", name="kind", kind=ColumnKind.DIMENSION, expr=authored("EVENTS.KIND")),
            Column(table="ORDERS", name="is_large", kind=ColumnKind.FILTER, expr=authored("ORDERS.TOTAL > 100")),
        ),
        metrics=(
            Metric("revenue", authored("SUM(ORDERS.ORDER_TOTAL)"), table="ORDERS"),
            Metric(
                "customer_count",
                authored('COUNT(DISTINCT "CUSTOMERS".ID)'),
                table="CUSTOMERS",
                using_relationships=("orders_to_customers",),
            ),
            Metric("per_customer", authored("REVENUE / CUSTOMER_COUNT + ORPHAN.X + 'ORDERS'")),
        ),
        verified_queries=(
            VerifiedQuery(
                "by_customer",
                "Revenue by customer?",
                authored_query("-- not ORDERS\nSELECT revenue, customer_count FROM customers"),
            ),
        ),
        custom_instruction_names=("tone",),
        referenced_models=("customers", "orders", "raw.events"),
        table_models=MappingProxyType({"ORDERS": _model("orders"), "CUSTOMERS": _model("customers")}),
    )


def test_each_member_reads_the_models_under_its_tables_and_the_members_it_names() -> None:
    assert member_edges(_view()) == {
        "fact:orders.order_total": MemberEdges(("orders",)),
        "dimension:orders.ordered_at": MemberEdges(("orders",)),
        "dimension:events.kind": MemberEdges(("raw.events",)),
        "filter:is_large": MemberEdges(("orders",)),
        "relationship:orders_to_customers": MemberEdges(("customers", "orders")),
        "metric:revenue": MemberEdges(("orders",)),
        "metric:customer_count": MemberEdges(("customers",), ("relationship:orders_to_customers",)),
        "metric:per_customer": MemberEdges((), ("metric:customer_count", "metric:revenue")),
        "verified_query:by_customer": MemberEdges(("customers",), ("metric:customer_count", "metric:revenue")),
    }
