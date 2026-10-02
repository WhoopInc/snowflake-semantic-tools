"""SST-VAL114: a metric's using_relationships names a relationship that starts at another table."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import METRICS, edited, found

USING = "    using_relationships:\n      - order_items_to_orders\n"


def test_sst_val114_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, METRICS, USING, "    using_relationships:\n      - orders_to_customers\n")
    [diagnostic] = found(project, "SST-VAL114")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "metric 'line_item_count': relationship 'ORDERS_TO_CUSTOMERS' does not start from 'order_items'"
    )
    assert diagnostic.subject == "metric:line_item_count"


def test_sst_val114_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, METRICS, USING, USING), "SST-VAL114") == []
