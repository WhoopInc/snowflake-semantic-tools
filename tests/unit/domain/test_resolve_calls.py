"""The template-call rules every field shares, the syntax routing, and the metric() depth bound."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import (
    KNOWN_FUNCTIONS,
    accepted_arity,
    call_problem,
    literal_problem,
    malformed_field,
    member_reference,
    syntax_problem,
    variable_problem,
)
from snowflake_semantic_tools.domain.resolve.depth import MAX_METRIC_DEPTH, over_deep, reference_depths

ORIGIN = Origin("views.yml", 4, 7)
EXPRESSION = frozenset(("ref", "metric", "var"))


def _problem(text: str, allowed: frozenset[str] = EXPRESSION, origin: Origin | None = ORIGIN) -> str | None:
    (call,) = scan_template_calls(text)
    found = call_problem(call, allowed, origin, field="expr", artifact="metric:m", subject="metric:m")
    return None if found is None else found.code


@pytest.mark.parametrize(
    ("text", "allowed", "code"),
    [
        ("{{ table('orders') }}", EXPRESSION, "SST-REF034"),
        ("{{ column('orders', 'id') }}", EXPRESSION, "SST-REF035"),
        ("{{ nope('x') }}", EXPRESSION, "SST-REF004"),
        ("{{ ref('orders') }}", frozenset(), "SST-REF008"),
        ("{{ tag('x') }}", EXPRESSION, "SST-REF041"),
        ("{{ metric('a', 'b') }}", EXPRESSION, "SST-REF015"),
        ("{{ ref('orders', 'id') }}", EXPRESSION, None),
    ],
)
def test_a_call_is_judged_legacy_unknown_forbidden_then_by_arity(
    text: str, allowed: frozenset[str], code: str | None
) -> None:
    assert _problem(text, allowed) == code


def test_a_legacy_call_is_located_at_the_call_even_without_an_origin() -> None:
    (call,) = scan_template_calls("x + {{ table('orders') }}")
    found = call_problem(call, EXPRESSION, None, field="expr", artifact="metric:m")
    assert found is not None and found.origin == Origin("<template>", 1, 5)
    (call,) = scan_template_calls("{{ column() }}")
    found = call_problem(call, EXPRESSION, ORIGIN, field="expr", artifact="metric:m")
    assert found is not None and dict(found.context)["model"] == "" and dict(found.context)["column"] == ""


def test_every_function_names_its_arity() -> None:
    assert len(KNOWN_FUNCTIONS) == 18
    assert (accepted_arity("ref"), accepted_arity("tool"), accepted_arity("tag")) == ((1, 2), (2,), (1,))


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("{{ ref('orders')", "SST-LOD004"),
        ("{{ a {{ b }}", "SST-LOD004"),
        ("{{ sha_version }}", "SST-REF033"),
        ("{{ ref('orders' }}", "SST-REF033"),
        ("{{ (x) }}", "SST-REF033"),
        ("{{ ref('a') extra }}", "SST-REF033"),
        ("{{ ref(orders) }}", "SST-REF003"),
        ("{{ ref('a' 'b') }}", "SST-REF003"),
        ("{{ ref('a) }}", "SST-REF003"),
    ],
)
def test_a_span_that_does_not_parse_is_reported_by_how_it_breaks(text: str, code: str) -> None:
    with pytest.raises(TemplateSyntaxError) as raised:
        scan_template_calls(text)
    found = syntax_problem(raised.value, "views.yml", subject="semantic_view:v")
    assert (found.code, found.subject) == (code, "semantic_view:v")
    assert found.origin == Origin("views.yml", raised.value.line, raised.value.col)


def test_a_malformed_field_is_located_at_its_origin_or_the_start() -> None:
    assert malformed_field("x", ORIGIN).message == "views.yml:4:7: malformed template expression 'x'"
    assert malformed_field("x", Origin("views.yml")).message == "views.yml:1:1: malformed template expression 'x'"
    assert malformed_field("x", None).message == "<template>:1:1: malformed template expression 'x'"


def test_a_literal_field_refuses_any_template() -> None:
    assert literal_problem("Total revenue.", ORIGIN, field="description", artifact="metric:m") is None
    found = literal_problem("From {{ ref('orders') }", ORIGIN, field="description", artifact="metric:m")
    assert found is not None and found.message == "metric:m: 'description' does not accept template expressions"


def test_a_variable_must_be_declared_and_not_empty() -> None:
    variables = {"state": "completed", "blank": ""}
    assert variable_problem("state", variables, ORIGIN) is None
    missing = variable_problem("nope", variables, ORIGIN, subject="metric:m")
    blank = variable_problem("blank", variables, ORIGIN)
    assert missing is not None and (missing.code, missing.subject) == ("SST-CFG029", "metric:m")
    assert blank is not None and blank.message == "{ var('blank') } resolved to an empty string"


@pytest.mark.parametrize(
    ("text", "name"),
    [
        ("{{ relationship('orders_to_customers') }}", "orders_to_customers"),
        ("orders_to_customers", None),
        ("{{ filter('x') }}", None),
        ("{{ relationship('a', 'b') }}", None),
        ("{{ relationship('a'", None),
    ],
)
def test_a_member_reference_is_one_whole_one_name_call(text: str, name: str | None) -> None:
    assert member_reference(text, "relationship") == name


def test_metric_depth_counts_hops_and_ignores_cycles_and_unknown_names() -> None:
    graph = {
        "a": ("b", "missing"),
        "b": ("c",),
        "c": (),
        "loop": ("loop", "c"),
        "shared": ("b", "c"),
    }
    assert reference_depths(graph) == {"a": 2, "b": 1, "c": 0, "loop": 1, "shared": 2}
    assert over_deep(graph) == ()
    assert over_deep(graph, limit=1) == (("a", 2), ("shared", 2))


def test_metric_depth_measures_a_chain_too_long_to_recurse_through() -> None:
    length = 5000
    graph = {f"m{index}": ((f"m{index + 1}",) if index < length else ()) for index in range(length + 1)}
    depths = reference_depths(graph)
    assert depths["m0"] == length
    assert over_deep(graph)[0] == ("m0", length)
    assert all(depth > MAX_METRIC_DEPTH for _, depth in over_deep(graph))
