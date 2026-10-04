"""SST-VAL203: a view's scope lists a relationship one of whose tables the view does not hold."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import reported, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about customers.
    tables:
      - "{{ ref('customers') }}"
TABLES    relationships:
      - orders_to_customers
"""
VARIABLES = """    variables:
      - name: large_order_cents
        data_type: NUMBER
        default_value: 1000
      - name: tax_inclusive
        data_type: BOOLEAN
        default_value: false
"""


def test_sst_val203_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW.replace("TABLES", ""))
    [diagnostic] = reported(project, "SST-VAL203")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "relationship 'orders_to_customers' names 'orders', absent from semantic_view:v"
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val203_silent(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW.replace("TABLES", "      - \"{{ ref('orders') }}\"\n" + VARIABLES))
    assert reported(project, "SST-VAL203") == []
