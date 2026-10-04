"""SST-VAL332: a metric the view keeps needs, in `using_relationships`, a relationship the view excludes."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import reported, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about line items.
    tables:
      - "{{ ref('order_items') }}"
      - "{{ ref('orders') }}"
      - "{{ ref('products') }}"
    variables:
      - name: large_order_cents
        data_type: NUMBER
        default_value: 1000
      - name: tax_inclusive
        data_type: BOOLEAN
        default_value: false
"""


def test_sst_val332_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW + "    exclude_relationships:\n      - order_items_to_orders\n")
    [diagnostic] = reported(project, "SST-VAL332")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:v: metric 'line_item_count' needs relationship 'order_items_to_orders', which this view excludes"
    )
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val332_fires_when_the_view_lacks_the_relationship_s_table(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW.replace("      - \"{{ ref('orders') }}\"\n", ""))
    [diagnostic] = reported(project, "SST-VAL332")
    assert diagnostic.message.endswith("needs relationship 'order_items_to_orders', which this view excludes")


def test_sst_val332_silent(tmp_path: Path) -> None:
    project = with_view(
        tmp_path,
        VIEW
        + "    exclude_metrics:\n      - line_item_count\n    exclude_relationships:\n      - order_items_to_orders\n",
    )
    assert reported(project, "SST-VAL332") == []
    assert "V" in view_names(project)
