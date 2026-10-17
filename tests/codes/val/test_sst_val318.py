"""SST-VAL318: an expression names a column marked exclude."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import reported, view_names, with_view
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val318_fires() -> None:
    [found] = metric_findings(metric("secrets", "SUM({{ ref('orders', 'secret') }})"), code="SST-VAL318")
    assert found.severity is Severity.ERROR
    assert found.message == "metric:secrets: 'secrets' references excluded column 'secret'"
    assert found.subject == "metric:secrets"


def test_sst_val318_silent() -> None:
    assert metric_findings(metric("total", "SUM({{ ref('orders', 'total') }})"), code="SST-VAL318") == []


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


def test_sst_val318_fires_for_a_kept_metric_reading_a_column_the_scope_removes(tmp_path: Path) -> None:
    project = _scoped(tmp_path, "    exclude_columns:\n      - \"{{ ref('orders', 'order_total') }}\"\n")
    messages = [item.message for item in reported(project, "SST-VAL318")]
    assert "semantic_view:v: 'total_revenue' references excluded column 'orders.order_total'" in messages
    assert "V" not in view_names(project)


def test_sst_val318_silent_for_a_scope_no_member_reads(tmp_path: Path) -> None:
    project = _scoped(tmp_path, "    exclude_columns:\n      - \"{{ ref('customers', 'customer_name') }}\"\n")
    assert reported(project, "SST-VAL318") == []
    assert "V" in view_names(project)
