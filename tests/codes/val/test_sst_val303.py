"""SST-VAL303: a view's table names a dbt model that is disabled or ephemeral."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import edited_manifest, reported, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about retired products.
    tables:
      - "{{ ref('retired_products') }}"
"""


def _disable(document: dict[str, Any]) -> None:
    document["disabled"] = {
        "model.sst_reference_impl.retired_products": [{"resource_type": "model", "name": "retired_products"}]
    }


def test_sst_val303_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW)
    disabled_manifest = edited_manifest(tmp_path, _disable)
    [diagnostic] = reported(project, "SST-VAL303", disabled_manifest)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:v: 'retired_products' is disabled in dbt and produces no relation"
    assert diagnostic.subject == "semantic_view:v"
    assert reported(project, "SST-REF001", disabled_manifest) == []
    assert "V" not in view_names(project, disabled_manifest)


def test_sst_val303_silent(tmp_path: Path) -> None:
    # A model dbt does not know at all is SST-REF001's, not this code's.
    project = with_view(tmp_path, VIEW)
    assert reported(project, "SST-VAL303") == []
    assert reported(project, "SST-REF001") != []
