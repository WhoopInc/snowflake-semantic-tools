"""The one reader of `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` documents, and the two that read through it.

`sst plan` (`live_view_from_yaml`) and `sst import` (`import_semantic_view`) read a live view's
document through `read_view`, so they cannot disagree on what it holds: the agreement test
reads one document that uses every section both read, and requires the plan's members and
the import's files and column edits to name the same tables, columns and members.
"""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.semantic_view import ViewPlacement, import_semantic_view
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.plan.definitions.semantic_view import live_view_from_yaml
from snowflake_semantic_tools.domain.snowflake_yaml.semantic_view import (
    YamlColumn,
    YamlConstraint,
    YamlRelationshipColumn,
    YamlSortKey,
    read_view,
)
from tests.helpers.import_fakes import dbt_column, dbt_model, index

_ORDERS = {"database": "DB", "schema": "MARTS", "table": "ORDERS"}
DOCUMENT: dict[str, Any] = {
    "name": "ORDERS_VIEW",
    "description": "Orders.",
    "tables": [
        {
            "name": "ORDERS",
            "base_table": _ORDERS,
            "primary_key": {"columns": ["ORDER_ID"]},
            "unique_keys": [{"columns": ["ORDER_ID", "STATUS"]}],
            "synonyms": ["sales"],
            "description": "One row per order.",
            "constraints": [
                {"name": "ORDERS_DISTINCT_RANGE", "distinct_range": {"start_column": "FROM_AT", "end_column": "TO_AT"}}
            ],
            "facts": [{"name": "AMOUNT", "expr": "ORDERS.AMOUNT", "synonyms": ["revenue"]}],
            "dimensions": [
                {"name": "STATUS", "expr": "ORDERS.STATUS", "sample_values": ["paid", 7], "is_enum": True},
                {"name": "IS_PAID", "expr": "ORDERS.STATUS = 'paid'", "labels": ["FILTER"], "description": "Paid."},
                "not a mapping",
            ],
            "time_dimensions": [{"name": "ORDERED_AT", "expr": "ORDERS.ORDERED_AT", "description": "When."}],
            "metrics": [{"name": "TOTAL", "expr": "SUM(ORDERS.AMOUNT)", "access_modifier": "private_access"}],
        },
        {"name": "CUSTOMERS", "base_table": {"database": "DB", "schema": "MARTS", "table": "CUSTOMERS"}},
    ],
    "relationships": [
        {
            "name": "ORDERS_TO_CUSTOMERS",
            "left_table": "ORDERS",
            "right_table": "CUSTOMERS",
            "relationship_columns": [
                {"left_column": "CUSTOMER_ID", "right_column": "ID"},
                {"left_column": "ORDERED_AT", "type": "range", "right_range": {"start_column": "A", "end_column": "B"}},
            ],
        }
    ],
    "metrics": [
        {
            "name": "LATEST",
            "expr": "SUM(ORDERS.AMOUNT)",
            "using_relationships": ["ORDERS_TO_CUSTOMERS"],
            "non_additive_dimensions": [{"table": "ORDERS", "dimension": "ORDERED_AT", "sort_direction": "descending"}],
        }
    ],
    "variables": [{"name": "MIN_AMOUNT", "data_type": "NUMBER(38,0)", "default_value": "10"}],
    "verified_queries": [{"name": "PAID", "question": "Paid?", "sql": "SELECT 1", "verified_at": 1700000000}],
    "module_custom_instructions": {"sql_generation": "Round money.", "question_categorization": "Sales only."},
    "max_staleness": "1 hour",
}


def test_the_reader_reads_every_key_both_readers_name() -> None:
    view = read_view(DOCUMENT)
    orders, customers = view.tables
    assert (orders.name, orders.base_table, orders.primary_key, orders.unique_keys) == (
        "ORDERS",
        ("DB", "MARTS", "ORDERS"),
        ("ORDER_ID",),
        (("ORDER_ID", "STATUS"),),
    )
    assert orders.constraints == (YamlConstraint("ORDERS_DISTINCT_RANGE", "FROM_AT", "TO_AT"),)
    # Sections in order, a non-mapping entry skipped, sample values as text.
    assert [(column.section, column.name, column.is_filter) for column in orders.columns] == [
        ("facts", "AMOUNT", False),
        ("dimensions", "STATUS", False),
        ("dimensions", "IS_PAID", True),
        ("time_dimensions", "ORDERED_AT", False),
    ]
    assert orders.columns[1] == YamlColumn(
        "dimensions", "STATUS", "ORDERS.STATUS", (), (), "", ("paid", "7"), is_enum=True, access_modifier=""
    )
    assert orders.metrics[0].access_modifier == "private_access" and customers.columns == ()
    assert view.relationships[0].columns[1] == YamlRelationshipColumn("range", "ORDERED_AT", "", "A", "B")
    assert view.metrics[0].non_additive_dimensions == (YamlSortKey("ORDERS", "ORDERED_AT", "descending", ""),)
    # A variable's default and a query's timestamp keep their type, for each reader to convert.
    assert view.variables[0].default_value == "10" and view.verified_queries[0].verified_at == 1700000000
    assert (view.sql_generation, view.question_categorization, view.max_staleness) == (
        "Round money.",
        "Sales only.",
        "1 hour",
    )


def test_a_document_without_tables_reads_as_listing_none() -> None:
    view = read_view({"tables": "no", "metrics": [{"name": None}]})
    assert (view.lists_tables, view.tables, view.metrics[0].name) == (False, (), "")


def test_plan_and_import_name_the_same_members_of_one_document() -> None:
    orders = dbt_model(
        "orders",
        "DB.MARTS.ORDERS",
        *(dbt_column(name) for name in ("order_id", "amount", "status", "ordered_at", "customer_id")),
    )
    customers = dbt_model("customers", "DB.MARTS.CUSTOMERS", dbt_column("id"))
    project = index(
        models={("DB", "MARTS", "ORDERS"): "orders", ("DB", "MARTS", "CUSTOMERS"): "customers"},
        dbt_models={"orders": orders, "customers": customers},
    )
    imported = import_semantic_view(
        QualifiedName.parse("DB.SV.ORDERS_VIEW"), DOCUMENT, project, ViewPlacement("models", Destination(("sales",)))
    )
    assert [item.code for item in imported.diagnostics if item.blocks] == []
    planned: dict[str, set[str]] = {}
    for member in live_view_from_yaml(DOCUMENT).members:
        planned.setdefault(member.kind, set()).add(member.name.rsplit(".", 1)[-1])
    written: dict[str, set[str]] = {}
    for file in imported.files[1:]:
        document: dict[str, Any] = dict(file.document or {})
        [(root, body)] = document.items()
        written.setdefault(root, set()).add(body["name"])
    columns = {
        (column.column, dict(column.meta).get("column_type"))
        for edit in imported.model_edits
        for column in edit.columns
    }
    assert planned["table"] == {"orders", "customers"}
    assert {edit.model for edit in imported.model_edits} >= {"orders"}
    # A plan compares time dimensions as dimensions; the import keeps their type.
    assert planned["fact"] == {name for name, kind in columns if kind == "fact"}
    assert planned["dimension"] == {name for name, kind in columns if kind in ("dimension", "time_dimension")}
    for kind in ("filter", "metric", "relationship", "verified_query"):
        assert planned[kind] == written[kind], kind
    assert planned["variable"] == {"min_amount"}
    assert written["custom_instruction"] == {"orders_view"}
