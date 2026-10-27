"""SST-VAL406: a filter declares `synonyms:`, which nothing renders."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import FILTERS, edited, project_copy, reported


def test_sst_val406_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        FILTERS,
        "expr: \"{{ ref('orders', 'order_state') }} = '{{ var('completed_state') }}'\"",
        "expr: \"{{ ref('orders', 'order_state') }} = '{{ var('completed_state') }}'\"\n    synonyms: [done]",
    )
    [diagnostic] = reported(project, "SST-VAL406")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "filter 'is_completed_order' declares synonyms that the renderer drops"
    assert diagnostic.subject == "filter:is_completed_order"


def test_sst_val406_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL406") == []
