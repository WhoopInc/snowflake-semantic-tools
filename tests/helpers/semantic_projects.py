"""Copies of the reference project with one file edited or added, loaded the way `sst` loads them."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from tests.helpers.cli_projects import DBT_MANIFEST, project_copy
from tests.helpers.projects import load_project

VIEWS = "semantic_models/semantic_views/semantic_views.yml"
METRICS = "semantic_models/metrics/metrics.yml"
RELATIONSHIPS = "semantic_models/relationships/relationships.yml"
FILTERS = "semantic_models/filters/filters.yml"


def with_view(tmp_path: Path, view: str) -> Path:
    """A project copy with one more view, `view` being its YAML list entry."""
    project = project_copy(tmp_path)
    folder = project / "semantic_models" / "semantic_views" / "extra"
    folder.mkdir()
    (folder / "semantic_views.yml").write_text(f"semantic_views:\n{view}", encoding="utf-8")
    return project


def edited(tmp_path: Path, relative: str, old: str, new: str) -> Path:
    """A project copy with the first `old` in one file replaced by `new`, which must be present."""
    project = project_copy(tmp_path)
    path = project / relative
    text = path.read_text(encoding="utf-8")
    assert old in text, f"{old!r} is not in {relative}"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    return project


def manifest(tmp_path: Path, edit: Callable[[dict[str, Any]], None]) -> Path:
    """A copy of the reference manifest that `edit` changed in place, written under `tmp_path`."""
    document = json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))
    edit(document)
    path = tmp_path / "edited_manifest.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def model_node(document: dict[str, Any], model: str) -> dict[str, Any]:
    """The manifest node of the reference project's model `model`."""
    node: dict[str, Any] = document["nodes"][f"model.sst_reference_impl.{model}"]
    return node


def set_column_meta(document: dict[str, Any], model: str, column: str, **values: object) -> None:
    """Set `meta.sst` keys on one manifest column, in both places dbt writes them."""
    entry = model_node(document, model)["columns"][column]
    for meta in (entry.setdefault("meta", {}), entry.setdefault("config", {}).setdefault("meta", {})):
        meta.setdefault("sst", {}).update(values)


def load(project: Path, manifest_path: Path = DBT_MANIFEST) -> SemanticViewProject:
    """Every view and diagnostic of a project, against the reference manifest unless told otherwise."""
    return load_project(project, manifest_path=manifest_path)


def found(project: Path, code: str, manifest_path: Path = DBT_MANIFEST) -> list[Diagnostic]:
    """The diagnostics loading `project` reports under `code`, in order."""
    return [item for item in load(project, manifest_path).diagnostics if item.code == code]


def view_names(project: Path, manifest_path: Path = DBT_MANIFEST) -> list[str]:
    """The unqualified names of the views that built."""
    return [view.fqn.rsplit(".", 1)[-1] for view in load(project, manifest_path).views]


CONFIG = "sst_config.yml"
INSTRUCTIONS = "semantic_models/custom_instructions/custom_instructions.yml"


def compiled_diagnostics(project: Path, code: str) -> list[dict[str, Any]]:
    """The diagnostics `sst compile --output json` reports under `code`, as JSON objects."""
    result = CliRunner().invoke(
        cli, ["compile", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "--output", "json"]
    )
    payload = json.loads(result.output)
    return [item for item in payload["diagnostics"] if item["code"] == code]
