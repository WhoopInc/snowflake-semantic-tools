"""The template scanner parses calls structurally and retains provenance."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.parse.template import (
    TemplateSyntaxError,
    replace_template_calls,
    scan_template_calls,
    single_template_call,
)


def test_quoted_spellings_have_the_same_tree_and_position() -> None:
    single = scan_template_calls("x\n  {{ ref('orders', 'order_id') }}")[0]
    double = scan_template_calls('x\n  {{ ref("orders", "order_id") }}')[0]
    assert (
        (single.function, single.args, single.line, single.col)
        == (
            double.function,
            double.args,
            double.line,
            double.col,
        )
        == ("ref", ("orders", "order_id"), 2, 3)
    )


def test_scans_multiple_functions_in_source_order() -> None:
    calls = scan_template_calls("DIV0({{ metric('revenue') }}, {{ var('floor') }})")
    assert [(call.function, call.args) for call in calls] == [
        ("metric", ("revenue",)),
        ("var", ("floor",)),
    ]


def test_replacement_changes_only_the_selected_function() -> None:
    text = "{{ ref('orders', 'id') }} = '{{ var('state') }}'"
    replaced = replace_template_calls(text, "ref", lambda call: ".".join(arg.upper() for arg in call.args))
    assert replaced == "ORDERS.ID = '{{ var('state') }}'"


def test_single_call_rejects_surrounding_expression_text() -> None:
    assert single_template_call("{{ ref('orders') }}", "ref") is not None
    assert single_template_call("x + {{ ref('orders') }}", "ref") is None
    assert single_template_call("{{ var('orders') }}", "ref") is None


def test_reference_parser_covers_empty_args_escapes_and_multiple_calls() -> None:
    empty = scan_template_calls("{{ ref() }}")[0]
    escaped = scan_template_calls(r"{{ var('it\'s') }}")[0]
    assert empty.args == ()
    assert escaped.args == ("it's",)
    assert scan_template_calls("plain text") == ()


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("{{ ref('orders')", "unterminated template expression"),
        ("{{ ref(orders) }}", "arguments must be quoted"),
        ("{{ ref('orders') trailing }}", "unexpected text"),
        ("{{ ref('{{ nested }}') }}", "nested template expression"),
        ("{{ 1bad() }}", "expected a template function name"),
        ("{{ ref }}", "expected"),
        ("{{ ref('orders' x) }}", "expected"),
        ("{{ ref('orders) }}", "unterminated quoted template argument"),
    ],
)
def test_malformed_templates_report_a_source_position(text: str, reason: str) -> None:
    with pytest.raises(TemplateSyntaxError, match=reason) as exc_info:
        scan_template_calls(text)
    assert exc_info.value.line == 1
    assert exc_info.value.col >= 1
