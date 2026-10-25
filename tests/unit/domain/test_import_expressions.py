"""Rewriting a live view's expressions with `ref()` and `metric()` calls, and reading a window back."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.importing.expressions import (
    ViewNames,
    parse_window,
    rewrite,
    rewrite_tables,
    split_window,
)
from snowflake_semantic_tools.domain.sql.lexer import LexError

_NAMES = ViewNames(
    MappingProxyType({"ORDERS": "orders", "CUSTOMERS": "customers"}),
    MappingProxyType({"orders": MappingProxyType({"ORDER_TOTAL": "order_total", "AT": "ordered_at"})}),
    MappingProxyType({("ORDERS", "TOTAL_REVENUE"): "total_revenue", ("CUSTOMERS", "N"): "customer_count"}),
    MappingProxyType({"TOTAL_REVENUE": "total_revenue", "GROSS_PROFIT": "gross_profit", "N": "customer_count"}),
)


def test_a_column_becomes_a_ref_and_a_metric_a_metric_call() -> None:
    rewritten = rewrite("DIV0(GROSS_PROFIT, ORDERS.TOTAL_REVENUE) + SUM(ORDERS.ORDER_TOTAL)", _NAMES)
    assert rewritten.text == (
        "DIV0({{ metric('gross_profit') }}, {{ metric('total_revenue') }}) + SUM({{ ref('orders', 'order_total') }})"
    )
    assert rewritten.models == ("orders",)


def test_strings_functions_variables_and_other_tables_are_kept() -> None:
    text = "IFF(ORDERS.TAX > LIMIT_CENTS, 'ORDERS.TAX', OTHER.X) + COUNT(1) + DB.S.T"
    assert rewrite(text, _NAMES).text == (
        "IFF({{ ref('orders', 'tax') }} > LIMIT_CENTS, 'ORDERS.TAX', OTHER.X) + COUNT(1) + DB.S.T"
    )


def test_a_column_takes_the_spelling_of_its_model() -> None:
    assert rewrite("MAX(ORDERS.AT)", _NAMES).text == "MAX({{ ref('orders', 'ordered_at') }})"


def test_an_unterminated_string_does_not_lex() -> None:
    with pytest.raises(LexError):
        rewrite("ORDERS.X = 'open", _NAMES)


def test_a_window_is_split_at_its_top_level_over_and_read_into_its_parts() -> None:
    expression = (
        "SUM(CUSTOMERS.N) OVER (PARTITION BY EXCLUDING CUSTOMERS.FIRST_AT "
        "ORDER BY CUSTOMERS.FIRST_AT DESC NULLS FIRST ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
    )
    aggregate, inside = split_window(expression) or ("", "")
    assert rewrite(aggregate, _NAMES).text == "SUM({{ metric('customer_count') }})"
    window = parse_window(inside, _NAMES)
    assert window is not None
    assert window.partition_excluding == ("{{ ref('customers', 'first_at') }}",)
    assert window.order_by == (
        {"ref": "{{ ref('customers', 'first_at') }}", "sort_direction": "descending", "null_order": "first"},
    )
    assert window.frame == "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"


def test_an_over_inside_parentheses_is_no_window_metric() -> None:
    assert split_window("SUM(ORDERS.X)") is None
    assert split_window("MAX(ROW_NUMBER() OVER (ORDER BY ORDERS.X))") is None


def test_a_window_entry_that_is_no_single_reference_is_not_read() -> None:
    assert parse_window("PARTITION BY ORDERS.X + 1", _NAMES) is None
    assert parse_window("ORDER BY ORDERS.X SIDEWAYS", _NAMES) is None


def test_a_verified_query_reads_its_tables_by_ref() -> None:
    sql = "SELECT orders.x FROM ORDERS AS orders JOIN CUSTOMERS c ON c.id = orders.c, CUSTOMERS d WHERE 1 = 1"
    rewritten = rewrite_tables(sql, _NAMES)
    assert rewritten.text == (
        "SELECT orders.x FROM {{ ref('orders') }} AS orders JOIN {{ ref('customers') }} c ON c.id = orders.c, "
        "CUSTOMERS d WHERE 1 = 1"
    )
    assert rewritten.models == ("orders", "customers")
