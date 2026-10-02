"""SST-VAL326: an attached member's expression holds a name only another view or table provides."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import found, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about orders.
    tables:
      - "{{ ref('orders') }}"
    variables:
      - name: tax_inclusive
        data_type: BOOLEAN
        default_value: false
"""
LARGE = """      - name: large_order_cents
        data_type: NUMBER
        default_value: 1000
"""


def test_sst_val326_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW)
    diagnostic = next(item for item in found(project, "SST-VAL326") if item.context["member"] == "large_order_count")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "view 'v': member 'large_order_count' references 'large_order_cents', which is neither a column on the "
        "view's tables nor a variable the view declares"
    )
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val326_silent(tmp_path: Path) -> None:
    assert found(with_view(tmp_path, VIEW + LARGE), "SST-VAL326") == []
