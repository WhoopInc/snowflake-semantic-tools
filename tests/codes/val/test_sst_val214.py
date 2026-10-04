"""SST-VAL214: a metric's using_relationships names a relationship that is not declared."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import METRICS, edited, reported

USING = "    using_relationships:\n      - order_items_to_orders\n"


def test_sst_val214_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, METRICS, USING, "    using_relationships:\n      - order_items_to_nowhere\n")
    [diagnostic] = reported(project, "SST-VAL214")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == "metric 'line_item_count' names relationship 'ORDER_ITEMS_TO_NOWHERE', which is not declared"
    )
    assert diagnostic.subject == "metric:line_item_count"


def test_sst_val214_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, METRICS, USING, USING), "SST-VAL214") == []
