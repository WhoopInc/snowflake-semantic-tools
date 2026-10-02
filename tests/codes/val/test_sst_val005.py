"""SST-VAL005: a routed object's description says what it is and never when to use it."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import found, view_names, with_view

VIEW = """  - name: v
    description: |-
      {description}
    tables:
      - "{{{{ ref('customers') }}}}"
"""


def test_sst_val005_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW.format(description="Customers and their first orders."))
    [diagnostic] = found(project, "SST-VAL005")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view 'v' description describes what it is, not when to use it"
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val005_silent(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW.format(description="Use for questions about customers."))
    assert found(project, "SST-VAL005") == []
