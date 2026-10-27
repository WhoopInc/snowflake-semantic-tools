"""SST-VAL337: a view's scope leaves out a filter or verified query that reads a column it removes."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import reported, view_names, with_view

SCOPED = """  - name: v
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
"""


def _scoped(tmp_path: Path, scope: str) -> Path:
    return with_view(tmp_path, SCOPED + scope)


def test_sst_val337_fires(tmp_path: Path) -> None:
    project = _scoped(tmp_path, "    exclude_columns:\n      - \"{{ ref('orders', 'order_state') }}\"\n")
    [found, *_] = [item for item in reported(project, "SST-VAL337") if "filter:" in item.message]
    assert found.severity is Severity.INFO
    assert found.message == (
        "semantic_view:v: leaves out 'filter:is_completed_order', which reads column 'orders.order_state' "
        "its scope removes"
    )
    assert found.subject == "semantic_view:v"
    # Kept metrics reading the column are still refused; the filter is only left out.
    assert not [item for item in reported(project, "SST-VAL318") if "filter:" in item.message]


def test_sst_val337_fires_for_a_verified_query(tmp_path: Path) -> None:
    project = _scoped(tmp_path, "    exclude_columns:\n      - \"{{ ref('customers', 'customer_type') }}\"\n")
    (project / "semantic_models" / "verified_queries" / "by_type.yml").write_text(
        "verified_query:\n"
        "  name: customers_by_type\n"
        '  question: "How many customers of each type are there?"\n'
        "  sql: \"SELECT {{ ref('customers', 'customer_type') }}, COUNT(*) FROM {{ ref('customers') }} GROUP BY 1\"\n",
        encoding="utf-8",
    )
    messages = [item.message for item in reported(project, "SST-VAL337")]
    assert (
        "semantic_view:v: leaves out 'verified_query:customers_by_type', which reads column "
        "'customers.customer_type' its scope removes"
    ) in messages


def test_sst_val337_silent(tmp_path: Path) -> None:
    project = _scoped(tmp_path, "    exclude_columns:\n      - \"{{ ref('customers', 'customer_name') }}\"\n")
    assert reported(project, "SST-VAL337") == []
    assert "V" in view_names(project)
