"""Load semantic views from a project on disk for tests that need a clean project."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.loader import load_semantic_views_result
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView


def load_views(project_dir: Path, *, manifest_path: Path, target_name: str | None = None) -> tuple[SemanticView, ...]:
    """Every healthy view, failing the test when the project reports an error."""
    project = load_semantic_views_result(
        project_dir, target_name=target_name, manifest_path=manifest_path, invoke_dbt=False
    )
    errors = [diagnostic for diagnostic in project.diagnostics if diagnostic.severity.name == "ERROR"]
    assert not errors, "project reported errors: " + "; ".join(f"{d.code} {d.message}" for d in errors)
    return project.views
