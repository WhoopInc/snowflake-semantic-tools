"""SST-VAL324: a model feeding a view has neither a contract nor a test."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import edited_manifest, project_copy, reference_node, reported


def _uncontracted(document: dict[str, Any]) -> None:
    reference_node(document, "products")["config"].pop("contract")
    # And untested: drop every test attached to products.
    document["nodes"] = {
        key: node
        for key, node in document["nodes"].items()
        if not (
            node.get("resource_type") == "test" and node.get("attached_node") == "model.sst_reference_impl.products"
        )
    }


def test_sst_val324_fires(tmp_path: Path) -> None:
    diagnostics = reported(project_copy(tmp_path), "SST-VAL324", edited_manifest(tmp_path, _uncontracted))
    diagnostic = next(item for item in diagnostics if item.subject == "semantic_view:jaffle_minimal")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:jaffle_minimal: 'products' has no contract and no tests"


def test_sst_val324_silent(tmp_path: Path) -> None:
    # supplies has no contract, but its test is enough.
    assert reported(project_copy(tmp_path), "SST-VAL324") == []
