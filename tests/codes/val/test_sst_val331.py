"""SST-VAL331: `exclude_columns` names a column its dbt metadata already excludes from every view."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import found, manifest, set_column_meta, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about orders.
    tables:
      - "{{ ref('orders') }}"
    exclude_columns:
      - "{{ ref('orders', 'subtotal') }}"
"""


def _manifest(tmp_path: Path, *, excluded: bool) -> Path:
    """The reference manifest, with `orders.subtotal` excluded globally when `excluded`."""
    return manifest(tmp_path, lambda document: set_column_meta(document, "orders", "subtotal", exclude=excluded))


def test_sst_val331_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW)
    edited_manifest = _manifest(tmp_path, excluded=True)
    [diagnostic] = found(project, "SST-VAL331", edited_manifest)
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:v: exclude_columns names 'orders.subtotal', which is already excluded globally"
    )
    assert diagnostic.subject == "semantic_view:v"
    assert found(project, "SST-VAL330", edited_manifest) == []


def test_sst_val331_silent(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW)
    assert found(project, "SST-VAL331", _manifest(tmp_path, excluded=False)) == []
