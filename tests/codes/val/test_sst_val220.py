"""SST-VAL220: a view declares a variable no expression it attaches uses."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import reported, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about customers.
    tables:
      - "{{ ref('customers') }}"
"""
UNUSED = """    variables:
      - name: churn_days
        data_type: NUMBER
        default_value: 90
"""


def test_sst_val220_fires(tmp_path: Path) -> None:
    [diagnostic] = reported(with_view(tmp_path, VIEW + UNUSED), "SST-VAL220")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:v: variable 'churn_days' is declared and never used"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_val220_silent(tmp_path: Path) -> None:
    assert reported(with_view(tmp_path, VIEW), "SST-VAL220") == []
