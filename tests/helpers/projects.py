"""Load semantic views from a project on disk for tests that need a clean project."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView


def load_project(project_dir: Path, *, manifest_path: Path, target_name: str | None = None) -> SemanticViewProject:
    """Every view and diagnostic, loaded the way `sst` loads them, from the dbt manifest given."""
    source = YamlProjectSource(project_dir, target_name=target_name, manifest_path=manifest_path, invoke_dbt=False)
    return source.load_project()


def load_views(project_dir: Path, *, manifest_path: Path, target_name: str | None = None) -> tuple[SemanticView, ...]:
    """Every healthy view, failing the test when the project reports an error."""
    project = load_project(project_dir, manifest_path=manifest_path, target_name=target_name)
    errors = [diagnostic for diagnostic in project.diagnostics if diagnostic.severity.name == "ERROR"]
    assert not errors, "project reported errors: " + "; ".join(f"{d.code} {d.message}" for d in errors)
    return project.views


def findings(diagnostics: Iterable[Diagnostic]) -> tuple[Diagnostic, ...]:
    """The diagnostics that are not INFO: every load reports what attached where, which a test of a fault ignores."""
    return tuple(diagnostic for diagnostic in diagnostics if diagnostic.severity is not Severity.INFO)
