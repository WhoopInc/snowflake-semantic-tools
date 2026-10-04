"""SST-VAL416: a verified query's SQL reads the clock."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import QUERIES, edited, reported


def test_sst_val416_fires(tmp_path: Path) -> None:
    dated = (
        "          SUM(orders.order_total) / 100 AS revenue\n      FROM orders AS orders"
        + "\n      WHERE orders.ordered_at >= CURRENT_DATE - 30"
    )
    [diagnostic] = reported(
        edited(
            tmp_path, QUERIES, "          SUM(orders.order_total) / 100 AS revenue\n      FROM orders AS orders", dated
        ),
        "SST-VAL416",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "verified_query 'total_revenue_all_time' contains relative date 'CURRENT_DATE'"
    assert diagnostic.subject == "verified_query:total_revenue_all_time"


def test_sst_val416_silent(tmp_path: Path) -> None:
    pinned = (
        "          SUM(orders.order_total) / 100 AS revenue\n      FROM orders AS orders"
        + "\n      WHERE orders.ordered_at >= '2026-01-01' -- not CURRENT_DATE"
    )
    assert (
        reported(
            edited(
                tmp_path,
                QUERIES,
                "          SUM(orders.order_total) / 100 AS revenue\n      FROM orders AS orders",
                pinned,
            ),
            "SST-VAL416",
        )
        == []
    )
