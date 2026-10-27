"""SST-VAL402: a metric carries the `filter` label."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import METRICS, edited, reported


def test_sst_val402_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        METRICS,
        "    expr: \"COUNT(DISTINCT {{ ref('orders', 'order_id') }})\"\n",
        "    expr: \"COUNT(DISTINCT {{ ref('orders', 'order_id') }})\"\n    labels: [filter]\n",
    )
    [diagnostic] = reported(project, "SST-VAL402")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "'order_count' carries labels: [filter] and is a metric"
    assert diagnostic.subject == "metric:order_count"


def test_sst_val402_silent(tmp_path: Path) -> None:
    # Any other label is an unknown key, not a filter label.
    project = edited(
        tmp_path,
        METRICS,
        "    expr: \"COUNT(DISTINCT {{ ref('orders', 'order_id') }})\"\n",
        "    expr: \"COUNT(DISTINCT {{ ref('orders', 'order_id') }})\"\n    labels: [kpi]\n",
    )
    assert reported(project, "SST-VAL402") == []
