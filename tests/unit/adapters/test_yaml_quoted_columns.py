"""Columns dbt quotes keep their exact case, and reserved-word columns are quoted, wherever a view names them."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.verify_schema import verify_columns
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.render.semantic_view import render
from tests.helpers.reference_project import (
    edited_manifest,
    load,
    member_file,
    project_copy,
    reference_node,
    set_model_meta,
)
from tests.helpers.snowflake_fake import FakeSnowflake


def _add_columns(document: dict[str, Any]) -> None:
    """Give `orders` a quoted mixed-case fact, a reserved-word fact, and make the quoted one its key."""
    columns = reference_node(document, "orders")["columns"]
    for name, quote in (("MixedCaseAmount", True), ("order", None)):
        sst = {"column_type": "fact"}
        columns[name] = {"name": name, "data_type": "NUMBER", "quote": quote, "meta": {"sst": sst}, "config": {}}
    set_model_meta(document, "orders", primary_key=["MixedCaseAmount"])


@pytest.fixture(scope="module")
def sales(tmp_path_factory: pytest.TempPathFactory) -> SemanticView:
    """`jaffle_sales` built against a manifest holding the two columns, with a metric over each."""
    root = tmp_path_factory.mktemp("quoted")
    project = project_copy(root)
    (project / member_file("metrics", "quoted_sums")).write_text(
        "snowflake_metrics:\n"
        "  - name: mixed_case_total\n"
        "    description: Sum of the quoted column.\n"
        "    expr: \"SUM({{ ref('orders', 'MixedCaseAmount') }})\"\n"
        "  - name: reserved_total\n"
        "    description: Sum of the reserved-word column.\n"
        "    expr: \"SUM({{ ref('orders', 'order') }})\"\n",
        encoding="utf-8",
    )
    loaded = load(project, edited_manifest(root, _add_columns))
    return next(view for view in loaded.views if view.fqn.endswith(".JAFFLE_SALES"))


def test_a_quoted_column_renders_quoted_in_its_member_name_and_expression(sales: SemanticView) -> None:
    ddl = str(render(sales))
    assert 'ORDERS."MixedCaseAmount" AS ORDERS."MixedCaseAmount"' in ddl
    assert 'PRIMARY KEY ("MixedCaseAmount")' in ddl
    assert 'ORDERS.MIXED_CASE_TOTAL AS SUM(ORDERS."MixedCaseAmount")' in ddl


def test_a_reserved_word_column_is_quoted_inside_an_expression(sales: SemanticView) -> None:
    ddl = str(render(sales))
    assert 'ORDERS."ORDER" AS ORDERS."ORDER"' in ddl
    assert 'ORDERS.RESERVED_TOTAL AS SUM(ORDERS."ORDER")' in ddl


def _verified(sales: SemanticView, *orders: str) -> list[str]:
    """The columns VAL325 reports when the warehouse's ORDERS holds `orders` beside its plain columns."""
    port = FakeSnowflake()
    tables: dict[str, list[tuple[str, str]]] = {}
    for table in sales.tables:
        names = [
            column.qualified_name.split(".", 1)[1] for column in sales.columns if column.table == table.logical_name
        ]
        if table.logical_name == "ORDERS":
            names = [name for name in names if name not in ("MixedCaseAmount", "ORDER")] + list(orders)
        tables[QualifiedName.parse(table.fqn).sql] = [(name, "NUMBER") for name in names]
    port.tables = {fqn: tuple(columns) for fqn, columns in tables.items()}
    found = verify_columns(port, CompileResult((CompiledView(sales, render(sales)),), DiagnosticBag(())))
    return sorted(str(item.context["column"]) for item in found if item.code == "SST-VAL325")


def test_verify_schema_matches_a_quoted_column_exactly(sales: SemanticView) -> None:
    assert _verified(sales, "MixedCaseAmount", "ORDER") == []
    # Each reference is quoted, so it names one spelling: a column created upper-cased is not
    # "MixedCaseAmount", and one created as "order" is not the reserved word's "ORDER".
    assert _verified(sales, "MIXEDCASEAMOUNT", "ORDER") == ["MixedCaseAmount"]
    assert _verified(sales, "MixedCaseAmount", "order") == ["ORDER"]


def test_a_relationship_names_a_quoted_column_quoted(tmp_path: Path) -> None:
    def quote_key(document: dict[str, Any]) -> None:
        reference_node(document, "customers")["columns"]["customer_id"]["quote"] = True

    loaded = load(project_copy(tmp_path), edited_manifest(tmp_path, quote_key))
    ddl = str(render(next(view for view in loaded.views if view.fqn.endswith(".JAFFLE_SALES"))))
    assert 'ORDERS_TO_CUSTOMERS AS ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS ("customer_id")' in ddl
    assert 'CUSTOMERS AS SST_REF_DEV.JAFFLE.CUSTOMERS PRIMARY KEY ("customer_id")' in ddl
