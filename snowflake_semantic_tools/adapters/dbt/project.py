"""Load what SST needs from a dbt project: the resolved target, its name, and its manifest's models.

`invoke` runs dbt; this module reads what the project declares about where dbt writes and
reads: the manifest's path and the model directories, through the YAML reader the caller
passes, and whether the manifest still matches the model files. `dbt_project_name` reads
dbt_project.yml as plain YAML, as dbt does. This package never parses an SST file.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from snowflake_semantic_tools.adapters.dbt.profiles import profile_output
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import resolve_within
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtTarget

YamlReader = Callable[[Path], Mapping[str, Any]]
# dbt's own default when dbt_project.yml declares no `model-paths`.
DEFAULT_MODEL_PATHS = ("models",)


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
    """The `manifest.json` dbt writes for this project, under dbt_project.yml's `target-path`.

    Raises:
        ProjectError: `target-path` resolves outside the project directory (SST-PRT009).

    Diagnostics:
        SST-PRT009: `target-path` resolves outside the project; raised.
    """
    target_path = str(read_yaml(project_dir / "dbt_project.yml").get("target-path") or "target")
    if resolve_within(project_dir, project_dir / target_path) is None:
        diagnostic = D(
            "SST-PRT009",
            origin=Origin("dbt_project.yml"),
            subject="config:dbt_project.yml",
            path=f"target-path {target_path!r}",
            detail="it resolves outside the project",
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return project_dir / target_path / "manifest.json"


def model_paths(project_dir: Path, read_yaml: YamlReader) -> tuple[tuple[str, ...], tuple[Diagnostic, ...]]:
    """Return dbt_project.yml's `model-paths`, or dbt's default when it declares none.

    Diagnostics:
        SST-DBT022: `model-paths` is declared but is not a list of directories; the default is used.
    """
    value = read_yaml(project_dir / "dbt_project.yml").get("model-paths")
    if value is None:
        return DEFAULT_MODEL_PATHS, ()
    if isinstance(value, list) and value and all(isinstance(item, str) and item.strip() for item in value):
        return tuple(str(item).strip().rstrip("/") for item in value), ()
    diagnostic = D(
        "SST-DBT022",
        origin=Origin("dbt_project.yml"),
        subject="config:dbt_project.yml",
        expected=repr(list(DEFAULT_MODEL_PATHS)),
    )
    return DEFAULT_MODEL_PATHS, (diagnostic,)


def _fresh(path: Path, checksum: str) -> bool:
    """Report whether a model file still has the checksum dbt recorded, read raw or stripped as dbt may."""
    raw = path.read_bytes()
    return checksum in (sha256(raw).hexdigest(), sha256(raw.decode("utf-8", "replace").strip().encode()).hexdigest())


def stale_models(project_dir: Path, catalog: DbtCatalog, paths: tuple[str, ...]) -> tuple[Diagnostic, ...]:
    """Report each model file under `paths` that changed after the manifest recorded its checksum.

    Only the root project's models are compared, by the file dbt read them from; a model with no
    checksum, or whose file is gone, is not compared.

    Diagnostics:
        SST-DBT005: a model file's contents no longer match the manifest, once per file.
    """
    found: list[Diagnostic] = []
    for model in sorted(catalog.models, key=lambda item: item.original_file_path or ""):
        file = model.original_file_path
        if file is None or model.checksum is None or model.package_name not in (None, catalog.project_name):
            continue
        if not any(file == root or file.startswith(f"{root}/") for root in paths):
            continue
        path = project_dir / file
        if path.is_file() and not _fresh(path, model.checksum):
            found.append(D("SST-DBT005", origin=Origin(file), subject=f"dbt_model:{model.name}", value=file))
    return tuple(found)


def dbt_project_name(project_dir: Path) -> str:
    """Return the `name:` of `dbt_project.yml`, or "" when the file names none or is not a mapping."""
    value = yaml.safe_load((project_dir / "dbt_project.yml").read_text(encoding="utf-8")) or {}
    return str(value.get("name") or "") if isinstance(value, dict) else ""
