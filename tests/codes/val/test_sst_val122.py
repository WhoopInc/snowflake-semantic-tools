"""SST-VAL122: a metric writes the 0.3 `visibility` key, which SST honours as `access_modifier`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import METRICS, edited, load, reported

ANCHOR = "    expr: \"COUNT(DISTINCT {{ ref('orders', 'order_id') }})\"\n"


def test_sst_val122_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, METRICS, ANCHOR, ANCHOR + "    visibility: private\n")
    [diagnostic] = reported(project, "SST-VAL122")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric 'order_count' uses visibility; the current key is access_modifier"
    assert diagnostic.subject == "metric:order_count"
    # Honoured: the metric renders private.
    sales = next(view for view in load(project).views if view.fqn.endswith(".JAFFLE_SALES"))
    assert next(item for item in sales.metrics if item.name == "ORDER_COUNT").access_modifier == "private_access"


def test_sst_val122_silent(tmp_path: Path) -> None:
    project = edited(tmp_path, METRICS, ANCHOR, ANCHOR + "    access_modifier: private_access\n")
    assert reported(project, "SST-VAL122") == []
