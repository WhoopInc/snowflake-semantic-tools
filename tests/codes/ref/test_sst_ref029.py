"""SST-REF029: a `using_relationships` entry written as `relationship()` names no relationship."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, load

FILE = "semantic_models/metrics/metrics.yml"
BEFORE = "      - order_items_to_orders"


def test_sst_ref029_fires(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "      - \"{{ relationship('order_items_to_order') }}\""))
    [diagnostic] = coded(project.diagnostics, "SST-REF029")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ relationship('order_items_to_order') } does not resolve"
    assert diagnostic.subject == "metric:line_item_count"


def test_sst_ref029_silent(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "      - \"{{ relationship('order_items_to_orders') }}\""))
    assert coded(project.diagnostics, "SST-REF029") == []
