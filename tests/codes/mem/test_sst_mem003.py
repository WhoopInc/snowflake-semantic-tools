"""SST-MEM003: a member's `tables:` names a table that is not a dbt model the manifest knows."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, load, project_copy

METRICS = "semantic_models/metrics/metrics.yml"
ORDER_COUNT = "  - name: order_count\n    tables:\n      - orders"


def test_sst_mem003_fires(tmp_path: Path) -> None:
    project = load(edited(tmp_path, METRICS, ORDER_COUNT, ORDER_COUNT.removesuffix("s")))
    [diagnostic] = coded(project.diagnostics, "SST-MEM003")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:order_count declares table 'order', which is not a known dbt model"
    assert diagnostic.subject == "metric:order_count"


def test_sst_mem003_silent(tmp_path: Path) -> None:
    assert coded(load(project_copy(tmp_path)).diagnostics, "SST-MEM003") == []
