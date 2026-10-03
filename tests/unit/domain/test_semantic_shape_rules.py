"""The shape of metric and filter entries, checked from the documents before any record is read."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.domain.validate.semantic.nodes import member_root
from snowflake_semantic_tools.domain.validate.semantic.shape import filter_parse_diagnostics, metric_parse_diagnostics
from tests.helpers.authored_documents import document, documents

METRICS = member_root("metric")
FILTERS = member_root("filter")


def _metric_findings(*nodes: dict[str, Any]) -> list[tuple[str, str | None]]:
    found = metric_parse_diagnostics(documents(document("m.yml", {METRICS: list(nodes)})))
    return [(item.code, item.context.get("field")) for item in found]


def test_a_metric_needs_a_string_expr_and_lists_of_the_right_types() -> None:
    assert _metric_findings({"tables": "orders"}) == [("SST-PRS002", "expr"), ("SST-PRS003", "tables")]
    assert _metric_findings({"name": "m", "expr": 1, "using_relationships": ["a", 2], "access_modifier": "hidden"}) == [
        ("SST-PRS113", "expr"),
        ("SST-PRS003", "using_relationships"),
        ("SST-PRS103", "access_modifier"),
    ]
    assert _metric_findings(
        {"name": "m", "expr": "x", "using_relationships": "a", "access_modifier": "public_access"}
    ) == [("SST-PRS003", "using_relationships")]
    assert _metric_findings({"name": "m", "expr": "x", "tables": [], "using_relationships": ["a"]}) == []


def test_each_non_additive_entry_names_a_dimension_a_bare_table_and_an_allowed_sort() -> None:
    assert _metric_findings({"name": "m", "expr": "x", "non_additive_dimensions": "as_of"}) == [
        ("SST-PRS003", "non_additive_dimensions")
    ]
    entries = [
        "as_of",
        {"dimension": " "},
        {"dimension": "as_of", "table": "{{ ref('b') }}", "sort_direction": "up", "null_order": 1},
        {"dimension": "as_of", "table": "balances", "sort_direction": "descending", "null_order": "last"},
        {"dimension": "as_of", "table": 3},
    ]
    assert _metric_findings({"name": "m", "expr": "x", "non_additive_dimensions": entries}) == [
        ("SST-PRS003", "non_additive_dimensions[0]"),
        ("SST-PRS002", "non_additive_dimensions[1].dimension"),
        ("SST-PRS003", "non_additive_dimensions[2].table"),
        ("SST-PRS103", "non_additive_dimensions[2].sort_direction"),
        ("SST-PRS103", "non_additive_dimensions[2].null_order"),
        ("SST-PRS003", "non_additive_dimensions[4].table"),
    ]


def test_a_window_is_a_mapping_of_reference_lists_an_order_and_a_frame() -> None:
    assert _metric_findings({"name": "m", "expr": "x", "window": ["x"]}) == [("SST-PRS003", "window")]
    window = {
        "partition_by": ["a"],
        "partition_by_excluding": "b",
        "order_by": [
            "{{ ref('o', 'x') }}",
            3,
            {"sort_direction": "sideways"},
            {"ref": "x", "null_order": 2, "sort_direction": "ascending"},
        ],
        "frame": "ROWS 1",
    }
    node = {"name": "m", "expr": "x", "window": window, "using_relationships": ["r"], "non_additive_dimensions": []}
    assert _metric_findings(node) == [
        ("SST-PRS014", "window"),
        ("SST-PRS003", "window.partition_by_excluding"),
        ("SST-PRS014", "window.partition_by"),
        ("SST-PRS003", "window.order_by[1]"),
        ("SST-PRS002", "window.order_by[2].ref"),
        ("SST-PRS103", "window.order_by[2].sort_direction"),
        ("SST-PRS103", "window.order_by[3].null_order"),
        ("SST-PRS124", None),
    ]
    assert _metric_findings({"name": "m", "expr": "x", "window": {"order_by": "x", "partition_by": [1]}}) == [
        ("SST-PRS003", "window.partition_by"),
        ("SST-PRS003", "window.order_by"),
    ]
    good = {"partition_by": ["a"], "order_by": ["b"], "frame": "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"}
    assert _metric_findings({"name": "m", "expr": "x", "window": good}) == []


def test_synonyms_are_strings_without_quotes_or_control_characters() -> None:
    assert _metric_findings({"name": "m", "expr": "x", "synonyms": "revenue"}) == [("SST-PRS029", None)]
    assert _metric_findings({"name": "m", "expr": "x", "synonyms": ["ok", 3]}) == [("SST-PRS029", None)]
    found = metric_parse_diagnostics(
        documents(document("m.yml", {METRICS: [{"name": "m", "expr": "x", "synonyms": ["ok", 'say "x"', "a\x01b"]}]}))
    )
    assert [(item.code, item.context["value"]) for item in found] == [
        ("SST-PRS030", 'say "x"'),
        ("SST-PRS030", "a\\u0001b"),
    ]


def test_a_filters_labels_are_a_list_of_strings() -> None:
    nodes = [
        {"name": "f", "labels": "filter"},
        {"labels": ["filter", 1]},
        {"name": "g", "labels": ["filter"]},
        {"name": "h"},
    ]
    found = filter_parse_diagnostics(documents(document("f.yml", {FILTERS: nodes})))
    assert [(item.subject, item.context["found"]) for item in found] == [
        ("filter:f", "str"),
        ("filter:<unnamed>", "list"),
    ]
