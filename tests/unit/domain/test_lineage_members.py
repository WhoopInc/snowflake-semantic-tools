from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.lineage.members import MemberEdges, member_edges, member_expressions
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
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
        "fact:orders.order_total": MemberEdges(("orders",), columns=("orders.order_total",)),
        "dimension:orders.ordered_at": MemberEdges(("orders",), columns=("orders.at",)),
        "dimension:events.kind": MemberEdges(("raw.events",), columns=("raw.events.kind",)),
        "filter:is_large": MemberEdges(("orders",), columns=("orders.total",)),
        "relationship:orders_to_customers": MemberEdges(
            ("customers", "orders"), columns=("customers.id", "orders.customer_id")
        ),
        # The fact the metric names reads the column, so the metric reads that column too.
        "metric:revenue": MemberEdges(("orders",), columns=("orders.order_total",)),
        "metric:customer_count": MemberEdges(
            ("customers",), ("relationship:orders_to_customers",), columns=("customers.id",)
        ),
        "metric:per_customer": MemberEdges((), ("metric:customer_count", "metric:revenue")),
        "verified_query:by_customer": MemberEdges(("customers",), ("metric:customer_count", "metric:revenue")),
    }


def _with_columns(name: str, *columns: str) -> DbtModel:
    return DbtModel(
        f"model.p.{name}",
        name,
        f"DB.S.{name.upper()}",
        (),
        (),
        tuple(DbtColumn(column, None, None, None) for column in columns),
    )


def test_a_verified_query_reads_columns_through_the_aliases_its_tables_are_given() -> None:
    sql = (
        "WITH totals AS (SELECT o.customer_id, SUM(o.subtotal) AS spent FROM ORDERS AS o GROUP BY 1)\n"
        "SELECT customers.customer_type, totals.spent, status\n"
        "FROM totals INNER JOIN CUSTOMERS c ON totals.customer_id = c.customer_id"
    )
    view = SemanticView(
        fqn="DB.S.SALES",
        tables=(Table(logical_name="ORDERS", fqn="DB.S.ORDERS"), Table(logical_name="CUSTOMERS", fqn="DB.S.CUSTOMERS")),
        verified_queries=(VerifiedQuery("spend", "Spend by type?", authored_query(sql)),),
        table_models=MappingProxyType(
            {
                "ORDERS": _with_columns("orders", "customer_id", "subtotal", "status"),
                "CUSTOMERS": _with_columns("customers", "customer_id", "customer_type", "status"),
            }
        ),
    )
    edges = member_edges(view)["verified_query:spend"]
    assert edges.columns == (
        "customers.customer_id",
        "customers.customer_type",
        "orders.customer_id",
        "orders.subtotal",
    )
    # A CTE's alias is no table's, and `status` is a column of both tables.
    assert edges.unresolved == ("status", "totals.customer_id", "totals.spent")


def test_a_bare_column_is_recorded_only_on_the_one_table_that_has_it() -> None:
    view = SemanticView(
        fqn="DB.S.SALES",
        tables=(Table(logical_name="ORDERS", fqn="DB.S.ORDERS"),),
        metrics=(Metric("revenue", authored("SUM(subtotal) + COUNT(missing)"), table="ORDERS"),),
        table_models=MappingProxyType({"ORDERS": _with_columns("orders", "subtotal")}),
    )
    edges = member_edges(view)["metric:revenue"]
    assert (edges.columns, edges.unresolved) == (("orders.subtotal",), ())


def test_each_member_s_resolved_expression_is_recorded_by_its_key() -> None:
    assert member_expressions(_view()) == {
        "fact:orders.order_total": "ORDERS.ORDER_TOTAL",
        "dimension:orders.ordered_at": "ORDERS.AT",
        "dimension:events.kind": "EVENTS.KIND",
        "filter:is_large": "ORDERS.TOTAL > 100",
        "metric:revenue": "SUM(ORDERS.ORDER_TOTAL)",
        "metric:customer_count": 'COUNT(DISTINCT "CUSTOMERS".ID)',
        "metric:per_customer": "REVENUE / CUSTOMER_COUNT + ORPHAN.X + 'ORDERS'",
    }


def test_a_longer_name_a_metric_name_and_an_alias_that_is_no_alias_are_not_columns() -> None:
    sql = (
        "SELECT DB.ORDERS.X, ORDERS.REVENUE, o.subtotal, z.y FROM ORDERS AS o "
        "JOIN CUSTOMERS WHERE CUSTOMERS.customer_id = 1 AND x.ORDERS.y = 2"
    )
    view = SemanticView(
        fqn="DB.S.SALES",
        tables=(
            Table(logical_name="ORDERS", fqn="DB.S.ORDERS"),
            Table(logical_name="CUSTOMERS", fqn="DB.S.CUSTOMERS"),
            Table(logical_name="ORPHAN", fqn="X.Y.ORPHAN"),
        ),
        relationships=(Relationship("to_orphan", "ORDERS", ("ID",), "ORPHAN", ("ID",)),),
        metrics=(Metric("revenue", authored("SUM(ORPHAN.X)"), table="ORDERS"),),
        verified_queries=(VerifiedQuery("q", "Q?", authored_query(sql)),),
        table_models=MappingProxyType(
            {"ORDERS": _with_columns("orders", "subtotal"), "CUSTOMERS": _with_columns("customers", "customer_id")}
        ),
    )
    edges = member_edges(view)
    assert edges["verified_query:q"].columns == ("customers.customer_id", "orders.subtotal")
    assert edges["verified_query:q"].unresolved == ("z.y",)
    # A table the view names that has no model or source key records nothing.
    assert edges["metric:revenue"].columns == ()
    assert edges["relationship:to_orphan"].columns == ("orders.id",)
