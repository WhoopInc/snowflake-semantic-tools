"""SST-MEM002: a metric, filter or verified query names no table, so it attaches to no view."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership
from tests.helpers.seam_projects import SmallProject, metric_file


def test_sst_mem002_fires() -> None:
    [diagnostic, *_] = coded(membership(member("filter", "f", ())).diagnostics, "SST-MEM002")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter:f names no table, so it attaches to no view"
    assert diagnostic.subject == "filter:f"


def test_sst_mem002_fires_for_a_metric_counting_rows(tmp_path: Path) -> None:
    # COUNT(*) names no column, so the metric has no table to attach under.
    project = SmallProject(tmp_path, files=metric_file('    expr: "COUNT(*)"\n')).load()
    [diagnostic] = coded(project.diagnostics, "SST-MEM002")
    assert diagnostic.subject == "metric:total"
    assert diagnostic.suggestion is not None and "COUNT({{ ref('orders', 'order_id') }})" in diagnostic.suggestion


def test_sst_mem002_silent(tmp_path: Path) -> None:
    assert coded(membership(member("filter", "f", (), "{{ ref('orders', 'id') }} > 0")).diagnostics, "SST-MEM002") == []
    # A derived metric names no table either, and is not reported.
    derived = metric_file("    expr: \"COUNT({{ ref('products', 'products_id') }})\"\n")
    derived["semantic_models/metrics/m.yml"] += (
        "  - name: doubled\n    description: Twice.\n    expr: \"{{ metric('total') }} * 2\"\n"
    )
    assert coded(SmallProject(tmp_path, files=derived).load().diagnostics, "SST-MEM002") == []
