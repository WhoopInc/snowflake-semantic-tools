"""SST-VAL329: a view declares an include list and the matching exclude list for one kind."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import found, view_names, with_view

BOTH = """  - name: v
    description: |-
      Use this view for questions about orders.
    tables:
      - "{{ ref('customers') }}"
    metrics:
      - "{{ metric('customer_count') }}"
"""


def test_sst_val329_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, BOTH + "    exclude_metrics:\n      - cumulative_customer_count\n")
    [diagnostic] = found(project, "SST-VAL329")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:v: 'metrics' and 'exclude_metrics' are both declared; a view either includes or excludes metrics"
    )
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val329_silent(tmp_path: Path) -> None:
    project = with_view(tmp_path, BOTH)
    assert found(project, "SST-VAL329") == []
    assert "V" in view_names(project)
