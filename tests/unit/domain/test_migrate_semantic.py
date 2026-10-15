"""`sst migrate refs` for semantic model files: bare references, non-additive entries, view settings."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.semantic import (
    members,
    metric_model,
    semantic_references,
    semantic_structure,
)
from tests.helpers.migrate_outline import applied, lines, outline

SOURCE = """snowflake_metrics:
  - name: m
    expr: "SUM({{ ref('orders', 'total') }})"
    using_relationships:
      - orders_to_customers
      - "{{ relationship('kept') }}"
    non_additive_dimensions:
      - dimension: snapshot_month
        # The model the dimension is in.
        table: supplies
        sort_direction: descending
      - table: "{{ ref('supplies') }}"
        dimension: month
      - {dimension: day, null_order: first}
      - dimension: "{{ ref('x', 'y') }}"
      - sort_direction: ascending
semantic_views:
  - name: v
    metrics: [m, "{{ metric('kept') }}"]
    exclude_relationships:
      - r
    enabled: true
    tags:
      - name: cost_center
        value: x
      - name: "{{ tag('kept') }}"
        value: y
"""


def test_bare_references_become_calls() -> None:
    root = outline(SOURCE)
    text = applied(SOURCE, semantic_references(root))
    assert "      - \"{{ relationship('orders_to_customers') }}\"\n" in text
    assert "metrics: [\"{{ metric('m') }}\", \"{{ metric('kept') }}\"]" in text
    assert "      - \"{{ relationship('r') }}\"\n" in text
    assert "      - name: \"{{ tag('cost_center') }}\"\n" in text and "{{ tag('kept') }}" in text


def test_non_additive_entries_become_refs_and_view_settings_move() -> None:
    root = outline(SOURCE)
    changes = semantic_structure(lines(SOURCE), root)
    text = applied(SOURCE, changes)
    assert "      - ref: \"{{ ref('supplies', 'snapshot_month') }}\"\n        sort_direction: descending\n" in text
    assert "# The model the dimension is in." not in text
    assert "      - ref: \"{{ ref('supplies', 'month') }}\"\n      - {ref" in text
    assert "      - {ref: \"{{ ref('orders', 'day') }}\", null_order: first}\n" in text
    assert "    config:\n      enabled: true\n      tags:\n" in text
    assert [item.reason for item in changes.untouched] == ["the dimension is not a bare column; write ref: by hand"]


def test_a_metric_naming_no_single_model_is_left_alone() -> None:
    source = (
        "metric:\n  name: m\n  expr: \"{{ ref('a', 'x') }} + {{ ref('b', 'y') }}\"\n"
        "  non_additive_dimensions:\n    - dimension: d\n"
    )
    changes = semantic_structure(lines(source), outline(source))
    assert changes.untouched[0].reason == "the metric names no single model; write ref: by hand"
    head = "metric:\n  name: m\n  expr: x\n  non_additive_dimensions:\n"
    flow = head + "    - {dimension: d,\n       table: a}\n"
    assert semantic_structure(lines(flow), outline(flow)).untouched[0].reason == "a flow entry over several lines"
    tabled = (
        'metric:\n  name: m\n  expr: x\n  non_additive_dimensions:\n    - dimension: d\n      table: "{{ broken( }}"\n'
    )
    assert semantic_structure(lines(tabled), outline(tabled)).untouched
    called = head + "    - dimension: d\n      table: \"{{ source('a', 'b') }}\"\n"
    assert semantic_structure(lines(called), outline(called)).untouched


def test_metric_model_and_members() -> None:
    root = outline("metric:\n  name: m\n  expr: \"{{ ref('A', 'x') }} / {{ ref('a', 'y') }} + {{ metric('z') }}\"\n")
    (metric,) = members(root, "snowflake_metrics")
    assert metric_model(metric) == "A"
    assert metric_model(outline('name: m\nexpr: "{{ broken( }}"\n')) is None
    assert metric_model(outline("name: m\n")) is None
    assert list(members(outline("metric: [a]\nsemantic_views: {}\n"), "snowflake_metrics")) == []
    assert semantic_references(outline("snowflake_metrics:\n  - name: m\n    using_relationships: x\n")).edits == []
    assert (
        semantic_structure(
            lines("metric:\n  name: m\n  non_additive_dimensions: x\n"),
            outline("metric:\n  name: m\n  non_additive_dimensions: x\n"),
        ).edits
        == []
    )
