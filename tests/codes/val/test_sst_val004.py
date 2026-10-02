"""SST-VAL004: a description is present and shorter than `validation.description_floor`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import found, with_view

VIEW = """  - name: v
    description: |-
      Use this for orders.
    tables:
      - "{{ ref('customers') }}"
"""


def _with_floor(tmp_path: Path, floor: int) -> Path:
    project = with_view(tmp_path, VIEW)
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text().replace("  strict: false\n", f"  strict: false\n  description_floor: {floor}\n", 1)
    )
    return project


def test_sst_val004_fires(tmp_path: Path) -> None:
    [diagnostic] = [
        item for item in found(_with_floor(tmp_path, 30), "SST-VAL004") if item.subject == "semantic_view:v"
    ]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view 'v' description is 20 chars, under 30"


def test_sst_val004_silent(tmp_path: Path) -> None:
    assert [item for item in found(_with_floor(tmp_path, 20), "SST-VAL004") if item.subject == "semantic_view:v"] == []
