"""SST-VAL336: a view keeps a metric built on a metric its scope removes."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import reported, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about orders and the customers who placed them.
    tables:
      - "{{ ref('orders') }}"
      - "{{ ref('customers') }}"
    variables:
      - name: large_order_cents
        data_type: NUMBER
        default_value: 1000
      - name: tax_inclusive
        data_type: BOOLEAN
        default_value: false
    exclude_metrics:
      - "{{ metric('order_count') }}"
"""


def test_sst_val336_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW)
    [diagnostic] = reported(project, "SST-VAL336")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:v: keeps metric 'revenue_per_order', which is built on 'order_count', which its scope removes"
    )
    assert diagnostic.subject == "semantic_view:v"
    # The view is refused before it builds, so the base is not reported as undeclared.
    assert reported(project, "SST-REF006") == []
    assert "V" not in view_names(project)


def test_sst_val336_silent(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW + "      - \"{{ metric('revenue_per_order') }}\"\n")
    assert reported(project, "SST-VAL336") == []
    assert "V" in view_names(project)
