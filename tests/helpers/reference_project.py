"""The reference project: where it is, copies of it with one file edited or added, and loads.

`project_copy` is the one way a test gets a writable copy of `tests/fixtures/reference_project`
(by default made offline: no Snowflake syntax check, not strict). `edited`, `appended`,
`with_view` and `added` copy it and change one file; `load` and `reported` load a copy the way
`sst` loads it, against the vendored dbt manifest; `edited_manifest` writes a changed copy of
that manifest. The paths of its authored files are the constants below.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from tests.helpers.cli_json import invoke_json
from tests.helpers.projects import load_project

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
DBT_MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"

CONFIG = "sst_config.yml"
VIEWS = "semantic_models/semantic_views/semantic_views.yml"
METRICS = "semantic_models/metrics/metrics.yml"
RELATIONSHIPS = "semantic_models/relationships/relationships.yml"
FILTERS = "semantic_models/filters/filters.yml"
INSTRUCTIONS = "semantic_models/custom_instructions/custom_instructions.yml"
QUERIES = "semantic_models/verified_queries/verified_queries.yml"


def project_copy(root: Path, *, offline: bool = True) -> Path:
    """A fresh copy of the reference project at `root/project`, without a compiled target.

    `offline` turns off the Snowflake syntax check and strict validation in the copy's
    `sst_config.yml`; an end-to-end test that runs the project as committed passes False.
    """
    project = root / "project"
    # Other tests compile the fixture in place, so its target/ may change while this copies.
    shutil.copytree(FIXTURE, project, ignore=shutil.ignore_patterns("target"))
    if offline:
        config = project / CONFIG
        config.write_text(
            config.read_text(encoding="utf-8")
            .replace("snowflake_syntax_check: true", "snowflake_syntax_check: false")
            .replace("strict: true", "strict: false"),
            encoding="utf-8",
        )
    return project


def edited(root: Path, relative: str, old: str, new: str) -> Path:
    """A project copy with the first `old` in one file replaced by `new`, which must be present."""
    project = project_copy(root)
    path = project / relative
    text = path.read_text(encoding="utf-8")
    assert old in text, f"{old!r} is not in {relative}"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    return project


def appended(root: Path, relative: str, text: str) -> Path:
    """A project copy with `text` appended to one file."""
    project = project_copy(root)
    path = project / relative
    path.write_text(path.read_text(encoding="utf-8") + text, encoding="utf-8")
    return project


def added(root: Path, relative: str, text: str) -> Path:
    """A project copy with one more file, `relative`, holding `text`."""
    project = project_copy(root)
    (project / relative).write_text(text, encoding="utf-8")
    return project


def with_view(root: Path, view: str) -> Path:
    """A project copy with one more view, `view` being its YAML list entry."""
    project = project_copy(root)
    folder = project / "semantic_models" / "semantic_views" / "extra"
    folder.mkdir()
    (folder / "semantic_views.yml").write_text(f"semantic_views:\n{view}", encoding="utf-8")
    return project


def break_menu_view(project: Path) -> None:
    """Point the `menu` view of a copy at a bare table name, which does not resolve."""
    path = project / "semantic_models" / "semantic_views" / "core" / "semantic_views.yml"
    text = path.read_text(encoding="utf-8")
    entry = "- \"{{ ref('products') }}\""
    assert entry in text
    path.write_text(text.replace(entry, '- "products"', 1), encoding="utf-8")


def load(project: Path, manifest_path: Path = DBT_MANIFEST) -> SemanticViewProject:
    """Every view and diagnostic of a project, against the reference manifest unless told otherwise."""
    return load_project(project, manifest_path=manifest_path)


def reported(project: Path, code: str, manifest_path: Path = DBT_MANIFEST) -> list[Diagnostic]:
    """The diagnostics loading `project` reports under `code`, in order."""
    return [item for item in load(project, manifest_path).diagnostics if item.code == code]


def view_names(project: Path, manifest_path: Path = DBT_MANIFEST) -> list[str]:
    """The unqualified names of the views that built."""
    return [view.fqn.rsplit(".", 1)[-1] for view in load(project, manifest_path).views]


def compile_json(project: Path) -> tuple[int, list[dict[str, Any]]]:
    """Run `sst compile --output json` on `project` with the vendored manifest: exit code and diagnostics."""
    return invoke_json(["compile", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST)])


def compiled_diagnostics(project: Path, code: str) -> list[dict[str, Any]]:
    """The diagnostics `sst compile --output json` reports under `code`, as JSON objects."""
    _, diagnostics = compile_json(project)
    return [item for item in diagnostics if item["code"] == code]


def edited_manifest(root: Path, edit: Callable[[dict[str, Any]], None]) -> Path:
    """A copy of the reference manifest that `edit` changed in place, written under `root`."""
    document = json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))
    edit(document)
    path = root / "edited_manifest.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def reference_node(document: dict[str, Any], model: str) -> dict[str, Any]:
    """The manifest node of the reference project's model `model`."""
    node: dict[str, Any] = document["nodes"][f"model.sst_reference_impl.{model}"]
    return node


def set_column_meta(document: dict[str, Any], model: str, column: str, **values: object) -> None:
    """Set `meta.sst` keys on one manifest column, in both places dbt writes them."""
    entry = reference_node(document, model)["columns"][column]
    for meta in (entry.setdefault("meta", {}), entry.setdefault("config", {}).setdefault("meta", {})):
        meta.setdefault("sst", {}).update(values)
