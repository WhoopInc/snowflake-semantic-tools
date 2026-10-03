"""The metric rules called directly: cycles, duplicates, structure, ordering, references and equivalence."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.authored import MetricDef, NonAdditiveDef, WindowDef, WindowOrderDef
from snowflake_semantic_tools.domain.validate.semantic.metrics import (
    metric_cycles,
    metric_diagnostics,
    unguarded_division,
)
from tests.helpers.semantic_members import MODELS, metric


def _found(*metrics: MetricDef, variables: dict[str, object] | None = None) -> list[tuple[str, str | None]]:
    return [(item.code, item.subject) for item in metric_diagnostics(metrics, dict(MODELS), variables)]


def _codes(*metrics: MetricDef, variables: dict[str, object] | None = None) -> list[str]:
    return [code for code, _ in _found(*metrics, variables=variables)]


SUM = "SUM({{ ref('orders', 'total') }})"


def test_every_metric_on_a_cycle_is_named_by_one_listed_cycle() -> None:
    metrics = (
        metric("a", "{{ metric('b') }} + {{ metric('c') }}", derived=True),
        metric("b", "{{ metric('a') }}", derived=True),
        metric("c", "{{ metric('b') }}", derived=True),
        metric("d", "{{ metric('ghost') }}", derived=True),
        metric("e", "{{ metric('E') }}", derived=True),
    )
    # The walk finishes b before it reaches c, so the cycle through c is added afterwards.
    assert metric_cycles(metrics) == (("a", "b", "a"), ("e", "e"), ("a", "c", "b", "a"))
    assert metric_cycles((metric("x", SUM),)) == ()


def test_a_repeated_name_is_reported_once_and_checked_no_further() -> None:
    metrics = (
        metric("Dup", SUM),
        metric("dup", SUM, tables=("balances",)),
        metric("mixed", "SUM({{ ref('orders', 'cost') }})"),
        metric("MIXED", "{{ metric('dup') }}", derived=True),
    )
    assert _codes(*metrics) == ["SST-VAL001", "SST-PRS008"]


def test_a_metrics_kind_rules_out_the_keys_it_may_not_declare() -> None:
    assert _codes(MetricDef("no_key", SUM, "The no_key.", ())) == ["SST-VAL109"]
    assert _codes(MetricDef("empty", SUM, "The empty.", (), (), has_tables_key=True)) == ["SST-PRS102"]
    derived = MetricDef(
        "derived",
        "{{ metric('base') }}",
        "The derived.",
        (),
        has_tables_key=True,
        derived=True,
        using_relationships=("A", "B"),
        access_modifier="hidden",
    )
    assert _codes(derived, metric("base", SUM)) == ["SST-VAL108", "SST-VAL113", "SST-VAL115", "SST-VAL121"]


def test_non_additive_dimensions_are_dimensions_of_their_table_in_a_stated_order() -> None:
    entries = (
        NonAdditiveDef("as_of", "balances", True, None),
        NonAdditiveDef("balance", "balances", False, True),
        NonAdditiveDef("secret", "orders", None, False),
        NonAdditiveDef("ghost"),
        NonAdditiveDef("account_id", "nowhere"),
    )
    found = metric_diagnostics(
        (metric("m", SUM, tables=("orders", "balances"), non_additive=entries),), dict(MODELS), {}
    )
    assert [
        (item.code, item.context.get("value"))
        for item in found
        if item.code in ("SST-VAL118", "SST-VAL119", "SST-VAL120")
    ] == [
        ("SST-VAL118", "balances.balance"),
        ("SST-VAL118", "orders.secret"),
        ("SST-VAL118", "ghost"),
        ("SST-VAL118", "nowhere.account_id"),
        (
            "SST-VAL119",
            "BALANCES.AS_OF DESC, BALANCES.BALANCE ASC NULLS FIRST, ORDERS.SECRET NULLS LAST, "
            "GHOST, NOWHERE.ACCOUNT_ID",
        ),
        ("SST-VAL120", None),
    ]
    ordered = WindowDef(order_by=(WindowOrderDef("{{ ref('orders', 'ordered_at') }}", True),))
    windowed = metric("w", f"SUM({SUM})", window=ordered)
    assert "SST-VAL120" in _codes(windowed)
    assert "SST-VAL120" not in _codes(metric("n", SUM, non_additive=(NonAdditiveDef("ordered_at", None, True, True),)))


def test_a_table_metric_aggregates_and_a_snapshot_sum_declares_its_non_additive_dimension() -> None:
    assert _codes(metric("raw", "{{ ref('orders', 'total') }}")) == ["SST-VAL101"]
    assert _codes(
        metric("w", "SUM(1)", window=WindowDef(order_by=(WindowOrderDef("{{ ref('orders', 'state') }}"),)))
    ) == ["SST-VAL126"]
    assert _codes(metric("snap", "SUM({{ ref('balances', 'balance') }})", tables=("balances",))) == ["SST-VAL117"]
    snapshot = metric(
        "kept",
        "SUM({{ ref('balances', 'balance') }})",
        tables=("balances",),
        non_additive=(NonAdditiveDef("as_of", None, True, False),),
    )
    assert _codes(snapshot) == []
    assert _codes(metric("count", "COUNT({{ ref('balances', 'balance') }})", tables=("balances",))) == []
    assert _codes(metric("ratio", f"{SUM} / SUM({{{{ ref('orders', 'cost') }}}})")) == ["SST-VAL111"]


def test_a_division_is_guarded_by_nullif_or_a_non_zero_number() -> None:
    assert unguarded_division("a / b")
    assert unguarded_division("a / 0")
    assert not unguarded_division("a / NULLIF(b, 0)")
    assert not unguarded_division("a / (2.5)")
    assert not unguarded_division("a / .5")
    assert not unguarded_division("'a/b' || {{ ref('x/y') }} -- c/d\n /* e/f */")


def test_a_derived_metric_combines_metrics_without_windows_columns_or_members() -> None:
    derived = metric(
        "derived",
        "RANK() OVER (ORDER BY 1) + SUM({{ metric('base') }}) + {{ ref('orders', 'total') }}"
        " + fact('f') + dimension('d')",
        derived=True,
    )
    windowed = metric("windowed", "LAG({{ metric('base') }})", derived=True, window=WindowDef())
    framed = metric("framed", "{{ metric('base') }} + 1", derived=True, window=WindowDef())
    found = _found(derived, windowed, framed, metric("base", SUM))
    assert found == [
        ("SST-VAL102", "metric:derived"),
        ("SST-VAL103", "metric:derived"),
        ("SST-VAL104", "metric:derived"),
        ("SST-VAL105", "metric:derived"),
        ("SST-VAL105", "metric:derived"),
        ("SST-VAL102", "metric:windowed"),
        ("SST-VAL102", "metric:framed"),
    ]
    functions = [
        item.context["function"]
        for item in metric_diagnostics((derived, windowed, framed, metric("base", SUM)), dict(MODELS), {})
        if item.code == "SST-VAL102"
    ]
    assert functions == ["RANK", "LAG", "window"]


def test_a_table_metric_builds_only_on_existing_additive_unwindowed_table_metrics() -> None:
    metrics = (
        metric(
            "top",
            "SUM({{ metric('derived') }}) + SUM({{ metric('snap') }})"
            " + SUM({{ metric('win') }}) + SUM({{ metric('none') }})",
        ),
        metric("derived", "{{ metric('base') }}", derived=True),
        metric("snap", SUM, non_additive=(NonAdditiveDef("ordered_at", None, True, False),)),
        metric("win", f"SUM({SUM})", window=WindowDef(order_by=(WindowOrderDef("{{ ref('orders', 'ordered_at') }}"),))),
        metric("base", SUM),
    )
    assert [code for code, subject in _found(*metrics) if subject == "metric:top"] == [
        "SST-VAL106",
        "SST-VAL107",
        "SST-VAL128",
        "SST-REF006",
    ]


def test_each_template_call_of_a_metric_names_a_variable_model_and_column_it_may_read() -> None:
    expr = (
        "SUM({{ ref('orders', 'total') }}) + {{ var('missing') }} + {{ table('orders') }} + {{ filter('f') }}"
        " + SUM({{ ref('nowhere', 'x') }}) + SUM({{ ref('orders', 'gone') }}) + SUM({{ ref('orders', 'secret') }})"
        " + SUM({{ ref('balances', 'balance') }}) + SUM({{ ref('balances') }})"
    )
    assert _codes(metric("m", expr), variables={}) == [
        "SST-CFG029",
        "SST-REF041",
        "SST-REF001",
        "SST-REF002",
        "SST-VAL318",
        "SST-VAL112",
        "SST-VAL112",
    ]
    assert _codes(metric("unchecked", f"{SUM} + {{{{ var('missing') }}}}")) == []
    assert _codes(metric("unknown_table", "SUM({{ ref('balances', 'balance') }})", tables=("orders", "ghost"))) == []
    assert _codes(metric("broken", "SUM({{ ref('orders', 'total') )"), variables={}) == ["SST-LOD004"]


def test_a_metric_names_its_tables_columns_through_ref_and_is_reported_at_the_first_bare_one() -> None:
    assert _codes(metric("bare", "SUM(total) + SUM(cost)"), variables={}) == ["SST-VAL110"]


def test_a_table_metric_computing_what_an_earlier_one_computes_is_a_duplicate() -> None:
    metrics = (
        metric("first", SUM),
        metric("second", "Sum({{  ref('orders', 'total')  }})"),
        metric("other_tables", SUM, tables=("orders", "balances")),
        metric("derived_one", "{{ metric('first') }}", derived=True),
        metric("derived_two", "{{ metric('first') }}", derived=True),
    )
    assert [(code, subject) for code, subject in _found(*metrics) if code == "SST-VAL124"] == [
        ("SST-VAL124", "metric:second")
    ]
    assert _codes(metric("same", SUM), metric("SAME", SUM)) == ["SST-VAL001"]
