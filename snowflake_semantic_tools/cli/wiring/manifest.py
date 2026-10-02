"""Build the SST manifest, read back the one `sst compile` wrote, and refuse a stale one."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.manifest import manifest_for, stale_manifest, target_mismatch
from snowflake_semantic_tools.cli.wiring.project import project_inputs, target_dir
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.state import Manifest


def build_manifest(
    paths: ProjectPaths, result: CompileResult, manifest_path: Path | None, target_name: str | None = None
) -> Manifest:
    """Build the manifest `result` publishes for `target_name`, reading what it records of the files now."""
    return manifest_for(result, project_inputs(paths, target_name, manifest_path).manifest_sources())


def compiled_manifest(project_dir: Path) -> Manifest:
    """The manifest `sst compile` wrote; plan, apply, list, and the suites read it.

    Raises:
        ProjectError: no compiled manifest exists (SST-MAN001).
    """
    path = target_dir(project_dir) / "manifest.json"
    manifest = ManifestFileStore(path).read()
    if manifest is None:
        diagnostic = D("SST-MAN001", path=str(path))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return manifest


def current_manifest(project_dir: Path, result: CompileResult, inputs: YamlProjectInputs, before: str) -> Manifest:
    """Return the manifest `result` publishes, refusing the run when `sst compile` wrote another.

    Raises:
        ProjectError: no compiled manifest exists (SST-MAN001), it was compiled for another
            target (SST-MAN006), or it is stale.
    """
    compiled = compiled_manifest(project_dir)
    current = manifest_for(result, inputs.manifest_sources())
    mismatch = target_mismatch(compiled, current)
    if mismatch is not None:
        raise ProjectError(mismatch.message, diagnostics=(mismatch,))
    stale = stale_manifest(compiled, current, before=before)
    if stale is not None:
        raise ProjectError(stale)
    return current
