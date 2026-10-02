"""Load semantic views from a project on disk for tests that need a clean project."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView

# The dbt seam's notes, which every load reports; `tests/codes/dbt` pins them.
SEAM_NOTES = frozenset(("SST-DBT016", "SST-DBT025"))


def load_project(project_dir: Path, *, manifest_path: Path, target_name: str | None = None) -> SemanticViewProject:
    """Every view and diagnostic, loaded the way `sst` loads them, from the dbt manifest given.

    The dbt seam's notes are left out, so a test can compare the whole list of what is wrong.
    """
    source = YamlProjectSource(project_dir, target_name=target_name, manifest_path=manifest_path, invoke_dbt=False)
    project = source.load_project()
    kept = DiagnosticBag(item for item in project.diagnostics if item.code not in SEAM_NOTES)
    return SemanticViewProject(project.views, kept)


def load_views(project_dir: Path, *, manifest_path: Path, target_name: str | None = None) -> tuple[SemanticView, ...]:
    """Every healthy view, failing the test when the project reports an error."""
    project = load_project(project_dir, manifest_path=manifest_path, target_name=target_name)
    errors = [diagnostic for diagnostic in project.diagnostics if diagnostic.severity.name == "ERROR"]
    assert not errors, "project reported errors: " + "; ".join(f"{d.code} {d.message}" for d in errors)
    return project.views
