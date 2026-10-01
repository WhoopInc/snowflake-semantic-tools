"""Load what SST needs from a dbt project: the resolved target, its name, and its manifest's models.

`dbt parse` runs here, and only when no manifest is given. `load_models` reads
dbt_project.yml through the YAML reader the caller passes; `dbt_project_name` reads it
as plain YAML, as dbt does. This package never parses an SST file.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import yaml

from snowflake_semantic_tools.adapters.dbt.manifest import load_manifest_catalog
from snowflake_semantic_tools.adapters.dbt.profiles import profile_output
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel, DbtTarget

YamlReader = Callable[[Path], Mapping[str, Any]]


def resolve_target(project_dir: Path, target_name: str | None = None) -> DbtTarget:
    """Resolve a declared dbt target's database and schema from `profiles.yml`.

    The profile adapter reads the file: it is plain YAML, not a semantic model, so
    the template-preserving parser would keep a single-quoted scalar's `''`
    escapes inside `{{ env_var(...) }}` and the value would never resolve.
    """
    try:
        _profile, selected_target, output = profile_output(project_dir, target_name)
    except ValueError as exc:
        raise ProjectError(str(exc)) from exc
    database, schema = output.get("database"), output.get("schema")
    if not database or not schema:
        raise ProjectError(f"target {selected_target!r} must set both `database` and `schema`")
    return DbtTarget(database=str(database), schema=str(schema))


def target_path(project_dir: Path, read_yaml: YamlReader) -> Path:
    """The `manifest.json` dbt writes for this project, under dbt_project.yml's `target-path`."""
    target_path = str(read_yaml(project_dir / "dbt_project.yml").get("target-path") or "target")
    return project_dir / target_path / "manifest.json"


def run_dbt_parse(project_dir: Path, target_name: str | None) -> None:
    """Run `dbt parse` on the project, with its own profiles.yml, for the target named.

    Raises:
        ProjectError: dbt cannot be started, or exits non-zero; the message carries its output.
    """
    command = [
        "dbt",
        "parse",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
    ]
    if target_name:
        command.extend(("--target", target_name))
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        raise ProjectError(f"cannot run dbt parse: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise ProjectError(f"dbt parse failed with exit {completed.returncode}: {detail}")


def load_models(
    project_dir: Path,
    *,
    read_yaml: YamlReader,
    target_name: str | None = None,
    manifest_path: Path | None = None,
    invoke_dbt: bool = True,
) -> dict[str, DbtModel]:
    """Load dbt models only from the manifest dbt resolved for this target.

    Args:
        read_yaml: Reads dbt_project.yml for its `target-path`; called only when no
            `manifest_path` is given, and before dbt runs.
        manifest_path: A manifest to read instead; dbt is then not run.
        invoke_dbt: Whether to run `dbt parse` first when no manifest is given.

    Returns:
        Each model by its casefolded name.
    """
    path = manifest_path or target_path(project_dir, read_yaml)
    if manifest_path is None and invoke_dbt:
        run_dbt_parse(project_dir, target_name)
    catalog: DbtCatalog = load_manifest_catalog(path)
    return {model.name.casefold(): model for model in catalog.models}


def dbt_project_name(project_dir: Path) -> str:
    """Return the `name:` of `dbt_project.yml`, or "" when the file names none or is not a mapping."""
    value = yaml.safe_load((project_dir / "dbt_project.yml").read_text(encoding="utf-8")) or {}
    return str(value.get("name") or "") if isinstance(value, dict) else ""
