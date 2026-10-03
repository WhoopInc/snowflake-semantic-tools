"""What the authored records derive from what was written, the frame grammar, and `Sql`'s edges."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.authored import MetricDef, NonAdditiveDef, WindowDef, WindowOrderDef
from snowflake_semantic_tools.domain.parse.frame import canonical_frame
from snowflake_semantic_tools.domain.parse.template import scan_template_calls
from snowflake_semantic_tools.domain.sql import join, literal, sql
from snowflake_semantic_tools.domain.sql.core import _seal


def test_a_non_additive_entry_sorts_by_its_table_then_its_dimension() -> None:
    assert NonAdditiveDef("as_of").key == ("AS_OF", None, None)
    assert NonAdditiveDef("as_of", "balances", True, False).key == ("BALANCES.AS_OF", True, False)


def test_a_window_lists_every_entry_with_the_field_it_came_from() -> None:
    window = WindowDef(("p",), ("x",), (WindowOrderDef("o"),))
    assert window.references() == (("partition_by[0]", "p"), ("partition_by_excluding[0]", "x"), ("order_by[0]", "o"))


def test_a_metric_reads_its_calls_from_the_scan_or_from_its_expression() -> None:
    expr = "{{ ref('orders', 'a') }} + {{ ref('Orders', 'b') }} + {{ ref('items', 'c') }} + {{ metric('m') }}"
    scanned = MetricDef("m", expr, None, (), ("orders",), template_calls=scan_template_calls(expr))
    assert scanned.referenced_models == ("orders", "items")
    assert MetricDef("m", expr, None, (), ()).referenced_models == ("orders", "items")
    assert MetricDef("m", "{{ ref('orders'", None, (), ()).calls == ()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("rows between unbounded preceding and current row", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
        (
            " RANGE BETWEEN interval '7 days' preceding AND 1 following ",
            "RANGE BETWEEN INTERVAL '7 days' PRECEDING AND 1 FOLLOWING",
        ),
        ("ROWS 3 PRECEDING", None),
        (7, None),
    ],
)
def test_a_frame_is_spelled_canonically_or_refused(value: object, expected: str | None) -> None:
    assert canonical_frame(value) == expected


def test_a_bound_statement_doubles_its_percents_and_keeps_its_placeholders() -> None:
    statement = sql("SELECT %s, {value}", value=literal("50%"))
    assert statement.for_driver(bound=True) == "SELECT %s, '50%%'"
    with pytest.raises(ValueError, match="no parameters are bound"):
        statement.for_driver(bound=False)
    assert sql("SELECT {value}", value=literal("50%")).for_driver(bound=False) == "SELECT '50%'"


def test_no_text_or_template_or_separator_may_hold_the_placeholder_mark() -> None:
    with pytest.raises(ValueError, match="NUL"):
        _seal("a\x00b")
    with pytest.raises(ValueError, match="NUL"):
        sql("SELECT \x00")
    with pytest.raises(ValueError, match="NUL"):
        join("\x00", ())
