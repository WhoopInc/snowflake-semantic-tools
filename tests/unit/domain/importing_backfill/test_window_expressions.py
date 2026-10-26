"""Reading a live window metric's `OVER (...)` and rewriting its references, at the grammar's edges."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.importing.expressions import LiveWindow, ViewNames, parse_window, split_window

_NAMES = ViewNames(tables=MappingProxyType({"ORDERS": "orders"}))


def _ref(column: str) -> str:
    return "{{ ref('orders', '" + column + "') }}"


def test_an_expression_that_does_not_lex_has_no_window_to_split() -> None:
    assert split_window("SUM(x) OVER (ORDER BY 'open") is None


def test_an_over_that_names_a_window_rather_than_holding_one_is_not_split() -> None:
    assert split_window("SUM(ORDERS.X) OVER w") is None


def test_a_window_whose_inside_does_not_lex_is_not_read() -> None:
    assert parse_window("ORDER BY 'open", _NAMES) is None


def test_a_partition_by_lists_each_reference_and_an_ascending_order_is_stated() -> None:
    window = parse_window("PARTITION BY ORDERS.REGION, ORDERS.STORE ORDER BY ORDERS.DAY ASC", _NAMES)
    assert window == LiveWindow(
        partition_by=(_ref("region"), _ref("store")),
        order_by=({"ref": _ref("day"), "sort_direction": "ascending"},),
    )


def test_a_partition_missing_its_by_still_reads_its_references_and_no_order_is_fine() -> None:
    assert parse_window("PARTITION ORDERS.REGION ROWS UNBOUNDED  PRECEDING", _NAMES) == LiveWindow(
        partition_by=(_ref("region"),), frame="ROWS UNBOUNDED PRECEDING"
    )


def test_a_partition_entry_that_is_an_expression_rather_than_a_reference_is_not_read() -> None:
    assert parse_window("PARTITION BY COALESCE(ORDERS.REGION, 'x')", _NAMES) is None
    assert parse_window("PARTITION BY (ORDERS.REGION)", _NAMES) is None


def test_an_inside_with_none_of_the_window_keywords_is_not_read() -> None:
    assert parse_window("ORDERS.DAY", _NAMES) is None
    assert parse_window("   ", _NAMES) == LiveWindow()


def test_an_order_entry_that_splits_a_string_or_is_not_a_reference_is_not_read() -> None:
    assert parse_window("ORDER BY 'a b'", _NAMES) is None
    assert parse_window("ORDER BY 1", _NAMES) is None
