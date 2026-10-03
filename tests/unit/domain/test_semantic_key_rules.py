"""The keys and names of authored nodes: unread keys, 0.3 spellings, missing or repeated names, legacy refs."""

from __future__ import annotations

from snowflake_semantic_tools.domain.validate.semantic.authored_keys import (
    authored_key_diagnostics,
    legacy_reference_diagnostics,
    member_name_diagnostics,
)
from snowflake_semantic_tools.domain.validate.semantic.nodes import node_root
from tests.helpers.authored_documents import document, documents

VIEWS, METRICS, FILTERS, RELATIONSHIPS = (
    node_root(name) for name in ("semantic_view", "metric", "filter", "relationship")
)
INSTRUCTIONS = node_root("custom_instruction")


def test_every_key_the_loader_does_not_read_is_named_by_what_it_is() -> None:
    tree = {
        VIEWS: [
            {
                "name": "v",
                "filters": [{"name": "inline"}, "bare"],
                "variables": [{"name": "x", "dflt": 1}, "not a mapping"],
                "table_config": {"orders": {"synonym": ["o"]}, "items": "not a mapping"},
                "tags": {"name": "wrong shape"},
            },
            {"name": "w", "filters": []},
            "not a node",
        ],
        METRICS: [
            {
                "name": "m",
                "labels": ["Filter"],
                "non_additive_by": [],
                "non_additive_dimensions": [{"dimension": "d", "order": "asc", "nulls": "first"}],
                "window": {"order_by": [{"column": "c", "direction": "asc"}, "x"], "frames": "x"},
            },
            {"labels": "kpi", "descriptin": "typo"},
        ],
        FILTERS: [{"name": "f", "synonyms": ["x"]}],
        RELATIONSHIPS: [{"name": "r", "left_column": "a"}],
    }
    file = document("m.yml", tree, ((VIEWS, 0, "filters"), 4), ((METRICS, 0, "window", "order_by", 0, "column"), 9))
    found = authored_key_diagnostics(documents(file, document("e.yml", {METRICS: "not a list"})))
    assert [(item.code, item.subject, item.context.get("field") or item.context.get("member")) for item in found] == [
        ("SST-VAL403", "semantic_view:v", "inline"),
        ("SST-VAL403", "semantic_view:v", "v.filters[1]"),
        ("SST-PRS022", "semantic_view:v", "table_config.orders.synonym"),
        ("SST-PRS004", "semantic_view:v", "variables[0].dflt"),
        ("SST-VAL403", "semantic_view:w", "w.filters[0]"),
        ("SST-VAL402", "metric:m", "m"),
        ("SST-PRS020", "metric:m", "non_additive_by"),
        ("SST-PRS020", "metric:m", "non_additive_dimensions[0].order"),
        ("SST-PRS020", "metric:m", "non_additive_dimensions[0].nulls"),
        ("SST-PRS022", "metric:m", "window.frames"),
        ("SST-PRS020", "metric:m", "window.order_by[0].column"),
        ("SST-PRS020", "metric:m", "window.order_by[0].direction"),
        ("SST-PRS022", "metric:1", "labels"),
        ("SST-PRS022", "metric:1", "descriptin"),
        ("SST-VAL406", "filter:f", "f"),
        ("SST-PRS021", "relationship:r", None),
    ]
    assert found[0].origin.line == 4 and found[10].origin.line == 9  # type: ignore[union-attr]
    assert found[2].origin.line is None  # type: ignore[union-attr]


def test_each_node_needs_a_name_its_type_uses_once() -> None:
    first = document(
        "metrics/a.yml",
        {
            METRICS: [{"name": "m"}, {"name": "m"}, {"expr": "x"}, "bare"],
            FILTERS: [{"name": "f"}, {"name": "F"}, {"name": "g"}, {"name": "h"}],
            VIEWS: [{"name": "v"}],
        },
        hint_root="metrics",
    )
    second = document("filters/b.yml", {FILTERS: [{"name": "g"}]}, hint_root="filters")
    third = document("loose.yml", {FILTERS: [{"name": "h"}], VIEWS: "not a list"})
    views = document("semantic_views/v.yml", {FILTERS: [{"name": "g"}]}, hint_root="semantic_views")
    found = member_name_diagnostics(documents(first, second, third, views))
    assert [(item.code, item.subject) for item in found] == [
        ("SST-PRS105", None),
        ("SST-PRS107", "metric:2"),
        ("SST-PRS107", "metric:3"),
        ("SST-PRS106", "filter:F"),
        ("SST-PRS104", "filter:g"),
        ("SST-PRS007", "filter:h"),
        ("SST-PRS104", "filter:g"),
    ]
    assert found[4].context["a"] == "metrics/" and found[4].context["b"] == "filters/"
    assert found[5].context["other"] == "the one in metrics/a.yml"
    assert member_name_diagnostics(documents(document("x/a.yml", {METRICS: [{"name": "Select"}]}, hint_root="x"))) != ()


def test_a_legacy_table_or_column_call_is_reported_wherever_a_string_holds_it() -> None:
    tree = {
        METRICS: [{"name": "m", "expr": "{{ table('orders') }} {{ column('orders', 'id') }} {{ ref('orders') }}"}],
        INSTRUCTIONS: [{"name": "i", "notes": ["{{ table('a', 'b') }}", "{{ column('x') }}", "{{ table('", 3]}],
    }
    found = legacy_reference_diagnostics(documents(document("m.yml", tree)))
    assert [(item.code, item.context["model"]) for item in found] == [
        ("SST-REF034", "orders"),
        ("SST-REF035", "orders"),
    ]
