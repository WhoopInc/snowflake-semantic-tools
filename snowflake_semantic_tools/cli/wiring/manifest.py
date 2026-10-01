"""Build the SST manifest, read back the one `sst compile` wrote, and refuse a stale one."""

from __future__ import annotations

from pathlib import Path

from ...adapters.errors import ProjectError
from ...adapters.fs.local import ManifestFileStore
from ...adapters.project_source import YamlProjectInputs
from ...app.compile import CompileResult
from ...app.manifest import manifest_for, stale_manifest
from ...domain.model.diagnostic import D
from ...domain.state import Manifest
from .project import project_inputs, target_dir


def build_manifest(project_dir: Path, result: CompileResult, manifest_path: Path | None) -> Manifest:
    """Build the manifest `result` publishes, reading what it records about the project's files now."""
    return manifest_for(result, project_inputs(project_dir, None, manifest_path).manifest_sources())


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
        ProjectError: no compiled manifest exists (SST-MAN001), or it is stale.
    """
    compiled = compiled_manifest(project_dir)
    current = manifest_for(result, inputs.manifest_sources())
    stale = stale_manifest(compiled, current, before=before)
    if stale is not None:
        raise ProjectError(stale)
    return current
