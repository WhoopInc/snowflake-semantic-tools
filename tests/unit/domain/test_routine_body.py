from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel, DbtSource
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_calls_of
from snowflake_semantic_tools.domain.resolve.routine_body import expand_body

_CATALOG = DbtCatalog(
    "v12",
    "1.11",
    "p",
    (
        DbtModel("model.p.orders", "orders", "SALES_DB.MARTS.ORDERS", (), (), ()),
        DbtModel("model.p.short", "short", "MARTS.SHORT", (), (), ()),
    ),
    unreadable_models={"scratch": "ephemeral"},
    sources=(
        DbtSource("source.p.raw.orders", "raw", "orders", "raw_db.ecom.orders"),
        DbtSource("source.p.raw.empty", "raw", "empty", None),
    ),
)


def _expand(text: str, variables: dict[str, object] | None = None) -> tuple[str, dict[str, str], list[str]]:
    expanded, diagnostics = expand_body(text, "tools/t.sql", _CATALOG, variables or {}, subject="tool:t")
    return expanded.text, dict(expanded.relations), [item.code for item in diagnostics]


def test_only_calls_of_the_named_functions_are_scanned() -> None:
    text = "a {{ ref('x') }} b {{ notacall }} c {{ other('y') }} d {{1(}} e {{  }} f {{ ref"
    calls = scan_calls_of(text, frozenset(("ref",)))
    assert [(call.function, call.args, call.line, call.col) for call in calls] == [("ref", ("x",), 1, 3)]


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("{{ ref('x')", "unterminated template expression"),
        ("{{ ref('x' {{ }}", "nested template expression"),
        ("{{ ref(x) }}", "template arguments must be quoted strings"),
    ],
)
def test_a_named_call_that_does_not_parse_is_refused(text: str, reason: str) -> None:
    with pytest.raises(TemplateSyntaxError) as raised:
        scan_calls_of(text, frozenset(("ref",)))
    assert raised.value.reason == reason


def test_ref_source_and_var_are_replaced_and_everything_else_is_kept() -> None:
    body = (
        "SELECT * FROM {{ ref('ORDERS') }} JOIN {{ source('raw', 'orders') }}\n"
        "WHERE d > DATEADD(day, -{{ var('lookback') }}, CURRENT_DATE)\n"
        "-- f'{{x}}' and {{ ref }} stay\n"
    )
    text, relations, codes = _expand(body, {"lookback": 7})
    assert codes == []
    assert text == (
        "SELECT * FROM SALES_DB.MARTS.ORDERS JOIN RAW_DB.ECOM.ORDERS\n"
        "WHERE d > DATEADD(day, -7, CURRENT_DATE)\n"
        "-- f'{{x}}' and {{ ref }} stay\n"
    )
    assert relations == {"orders": "SALES_DB.MARTS.ORDERS", "raw.orders": "RAW_DB.ECOM.ORDERS"}


def test_a_body_without_calls_is_unchanged() -> None:
    body = 'def run(session):\n    return f"{{{{ {session} }}}}"\n'
    assert _expand(body) == (body, {}, [])


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ("{{ ref('orders')", "SST-LOD004"),
        ("{{ metric('revenue') }}", "SST-REF041"),
        ("{{ table('orders') }}", "SST-REF034"),
        ("{{ ref('orders', 'id') }}", "SST-REF015"),
        ("{{ source('raw') }}", "SST-REF015"),
        ("{{ ref('missing') }}", "SST-REF001"),
        ("{{ ref('scratch') }}", "SST-DBT009"),
        ("{{ ref('short') }}", "SST-REF019"),
        ("{{ source('raw', 'missing') }}", "SST-DBT011"),
        ("{{ source('raw', 'empty') }}", "SST-DBT011"),
        ("{{ var('missing') }}", "SST-CFG029"),
    ],
)
def test_a_call_that_cannot_resolve_is_reported_and_kept(body: str, code: str) -> None:
    text, relations, codes = _expand(body)
    assert (text, relations, codes) == (body, {}, [code])


def test_a_diagnostic_points_at_its_call_in_the_body_file() -> None:
    expanded, diagnostics = expand_body("SELECT 1;\n  {{ ref('nope') }}", "tools/t.sql", _CATALOG, {}, subject="tool:t")
    (diagnostic,) = diagnostics
    assert (diagnostic.origin, diagnostic.subject) == (Origin("tools/t.sql", 2, 3), "tool:t")
    assert expanded.relations == {}


def test_an_unreadable_model_points_at_its_call() -> None:
    _, diagnostics = expand_body("{{ ref('scratch') }}", "tools/t.sql", _CATALOG, {}, subject="tool:t")
    assert [item.origin for item in diagnostics] == [Origin("tools/t.sql", 1, 1)]
