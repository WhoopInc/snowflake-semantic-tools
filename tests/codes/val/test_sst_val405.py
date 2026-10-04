"""SST-VAL405: a boolean filter declares no `labels:` key."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import FILTERS, edited, project_copy, reported


def test_sst_val405_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        FILTERS,
        "expr: \"{{ ref('orders', 'order_total') }} > large_order_cents\"\n    labels:\n      - filter",
        "expr: \"{{ ref('orders', 'order_total') }} > large_order_cents\"",
    )
    [diagnostic] = reported(project, "SST-VAL405")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter 'is_large_order' is boolean-valued and declares no labels: key"
    assert diagnostic.subject == "filter:is_large_order"


def test_sst_val405_silent(tmp_path: Path) -> None:
    # The non-boolean `high_value_threshold_cents` declares no labels: key either.
    assert reported(project_copy(tmp_path), "SST-VAL405") == []
