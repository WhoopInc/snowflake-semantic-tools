"""SST-VAL129: a window entry names a dimension the metric's table cannot reach in the view."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.reference_project import METRICS, edited, reported, view_names

EXCLUDING = "      partition_by_excluding:\n        - \"{{ ref('customers', 'first_ordered_at') }}\"\n"


def test_sst_val129_fires(tmp_path: Path) -> None:
    # customers is the one side of orders_to_customers, so it reaches no order column.
    unreachable = "      partition_by:\n        - \"{{ ref('orders', 'order_state') }}\"\n"
    project = edited(tmp_path, METRICS, EXCLUDING, unreachable)
    [diagnostic] = reported(project, "SST-VAL129")
    assert (diagnostic.severity, ERROR_REGISTRY[diagnostic.code].demotable) == (Severity.ERROR, False)
    assert diagnostic.message == (
        "metric 'cumulative_customer_count': window partition_by[0] names {{ ref('orders', 'order_state') }}, "
        "which is not a dimension CUSTOMERS reaches in this view"
    )
    assert diagnostic.subject == "semantic_view:jaffle_sales"
    assert "JAFFLE_SALES" not in view_names(project)


def test_sst_val129_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, METRICS, EXCLUDING, EXCLUDING), "SST-VAL129") == []
