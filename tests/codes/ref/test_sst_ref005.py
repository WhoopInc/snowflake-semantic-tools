"""SST-REF005: a metric reaches itself through `metric()` references; the cycle is reported once."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, load

FILE = "semantic_models/metrics/metrics.yml"
BEFORE = "expr: \"SUM({{ ref('orders', 'order_total') }})\""


def test_sst_ref005_fires(tmp_path: Path) -> None:
    project = load(
        edited(
            tmp_path,
            FILE,
            BEFORE,
            "expr: \"SUM({{ ref('orders', 'order_total') }}) + {{ metric('revenue_per_order') }}\"",
        )
    )
    [diagnostic] = coded(project.diagnostics, "SST-REF005")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric reference cycle: revenue_per_order -> total_revenue -> revenue_per_order"
    assert diagnostic.subject == "metric:revenue_per_order"


def test_sst_ref005_silent(tmp_path: Path) -> None:
    project = load(
        edited(
            tmp_path, FILE, BEFORE, "expr: \"SUM({{ ref('orders', 'order_total') }}) + {{ metric('order_count') }}\""
        )
    )
    assert coded(project.diagnostics, "SST-REF005") == []
