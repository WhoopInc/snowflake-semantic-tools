"""SST-VAL301: a view resolves no dimension and no metric."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import found, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about customers.
    tables:
      - "{{ ref('customers') }}"
"""


def test_sst_val301_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW + "    columns: []\n    metrics: []\n")
    [diagnostic] = found(project, "SST-VAL301")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:v resolves no dimension and no metric"
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val301_silent(tmp_path: Path) -> None:
    assert found(with_view(tmp_path, VIEW + "    metrics: []\n"), "SST-VAL301") == []
