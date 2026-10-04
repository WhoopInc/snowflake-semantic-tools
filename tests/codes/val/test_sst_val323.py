"""SST-VAL323: a model a view reads exposes no column metadata."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import edited_manifest, project_copy, reference_node, reported


def _no_columns(document: dict[str, Any]) -> None:
    reference_node(document, "products")["columns"] = {}


def test_sst_val323_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    diagnostics = reported(project, "SST-VAL323", edited_manifest(tmp_path, _no_columns))
    diagnostic = next(item for item in diagnostics if item.subject == "semantic_view:jaffle_minimal")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:jaffle_minimal: 'products' has no columns: block in dbt"


def test_sst_val323_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL323") == []
