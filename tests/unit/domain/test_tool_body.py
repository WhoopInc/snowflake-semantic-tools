from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel, DbtSource
from snowflake_semantic_tools.domain.validate.tool_body import hardcoded_relations, relation_parts

_CATALOG = DbtCatalog(
    "v12",
    "1.11",
    "p",
    (
        DbtModel("model.p.orders", "orders", "SALES_DB.MARTS.ORDERS", (), (), ()),
        DbtModel("model.p.Mixed", "mixed", '"Sales_Db".MARTS."Mixed"', (), (), ()),
        DbtModel("model.p.short", "short", "MARTS.SHORT", (), (), ()),
    ),
    sources=(
        DbtSource("source.p.raw.items", "raw", "items", "raw_db.ecom.items"),
        DbtSource("source.p.raw.none", "raw", "none", None),
    ),
)


def _found(
    text: str, language: str | None = "sql", catalog: DbtCatalog = _CATALOG
) -> list[tuple[str, str, int | None]]:
    diagnostics = hardcoded_relations(text, language, catalog, file="tools/t.sql", name="t", subject="tool:t")
    return [
        (item.context["relation"], item.context["expected"], item.origin.line if item.origin else None)
        for item in diagnostics
    ]


@pytest.mark.parametrize(
    ("name", "parts"),
    [
        ("sales_db.marts.orders", ("SALES_DB", "MARTS", "ORDERS")),
        ('"a""b".s.t', ('a"b', "S", "T")),
        ("a b", None),
        ("a.", None),
        ("", None),
    ],
)
def test_a_dotted_name_resolves_as_snowflake_resolves_it(name: str, parts: tuple[str, ...] | None) -> None:
    assert relation_parts(name) == parts


def test_a_sql_body_s_literal_project_relations_are_reported_once_where_first_written() -> None:
    body = (
        "SELECT * FROM sales_db.marts.orders o\n"
        "JOIN SALES_DB.MARTS.ORDERS.ID x\n"
        "JOIN RAW_DB.ECOM.ITEMS i -- SALES_DB.MARTS.ORDERS\n"
        'WHERE note = \'SALES_DB.MARTS.ORDERS\' AND "Sales_Db".MARTS."Mixed".X = 1\n'
        "AND PARTNER.DELIVERY.FEES IS NULL AND MARTS.SHORT.ID = 2 AND SALES_DB.MARTS.*\n"
    )
    assert _found(body) == [
        ("SALES_DB.MARTS.ORDERS", "{{ ref('orders') }}", 1),
        ("RAW_DB.ECOM.ITEMS", "{{ source('raw', 'items') }}", 3),
        ("Sales_Db.MARTS.Mixed", "{{ ref('mixed') }}", 4),
    ]


def test_a_quoted_part_matches_only_its_exact_case() -> None:
    assert _found('SELECT 1 FROM "sales_db".MARTS.ORDERS') == []


def test_a_name_beside_another_part_starts_its_own_chain() -> None:
    assert _found('SELECT"SALES_DB".MARTS.ORDERS') == [("SALES_DB.MARTS.ORDERS", "{{ ref('orders') }}", 1)]


def test_a_sql_body_that_does_not_lex_is_matched_whole() -> None:
    assert _found("SELECT 'SALES_DB.MARTS.ORDERS") == [("SALES_DB.MARTS.ORDERS", "{{ ref('orders') }}", 1)]


def test_code_in_another_language_is_scanned_whole_with_its_calls_blanked() -> None:
    body = (
        "def run(session):\n"
        '    a = session.table("SALES_DB.MARTS.ORDERS")\n'
        "    b = session.table('{{ var(\"RAW_DB.ECOM.ITEMS\") }}')\n"
        "    return self.session.sql\n"
    )
    assert _found(body, "python") == [("SALES_DB.MARTS.ORDERS", "{{ ref('orders') }}", 2)]
    assert _found(body, None) == _found(body, "python")


def test_a_body_whose_calls_do_not_parse_is_scanned_as_written() -> None:
    assert _found("x = 'RAW_DB.ECOM.ITEMS' {{ ref('orders'", "javascript") == [
        ("RAW_DB.ECOM.ITEMS", "{{ source('raw', 'items') }}", 1)
    ]


def test_nothing_is_reported_without_a_project_relation() -> None:
    assert _found("SELECT * FROM SALES_DB.MARTS.ORDERS", catalog=DbtCatalog("v12", None, None, ())) == []


def test_a_diagnostic_points_into_the_body_file() -> None:
    (diagnostic,) = hardcoded_relations(
        "SELECT 1\n  FROM SALES_DB.MARTS.ORDERS", "sql", _CATALOG, file="tools/t.sql", name="t", subject="tool:t"
    )
    assert (diagnostic.origin, diagnostic.subject) == (Origin("tools/t.sql", 2, 8), "tool:t")
    assert diagnostic.message == "tool member 't': body hard-codes SALES_DB.MARTS.ORDERS, which is {{ ref('orders') }}"
