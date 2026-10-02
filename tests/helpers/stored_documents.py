"""Stored manifests and state caches on disk and in memory, for the manifest and state code tests."""

from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore, StateFileStore
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry, LastRun, Manifest, State
from tests.helpers.artifact_builders import manifest, rendered, target

ENTRY = AppliedEntry("f" * 64, "DB.SCHEMA.V", "then", "run", APPLIED, "f" * 64, "m" * 64)


def valid_manifest() -> Manifest:
    """A one-view manifest whose id is the hash of its content."""
    return manifest({"semantic_view:v": rendered()})


def written(tmp_path: Path, document: object, name: str = "manifest.json") -> Path:
    """Write `document` as JSON, or as given when it is text, and return its path."""
    path = tmp_path / name
    path.write_text(document if isinstance(document, str) else json.dumps(document), encoding="utf-8")
    return path


def manifest_refusal(path: Path) -> Diagnostic:
    """Read a manifest file that must be refused, and return the one diagnostic it is refused with."""
    with pytest.raises(ProjectError) as raised:
        ManifestFileStore(path).read()
    [diagnostic] = raised.value.diagnostics
    return diagnostic


def state_refusal(path: Path) -> Diagnostic:
    """Read a state file that must be refused, and return the one diagnostic it is refused with."""
    with pytest.raises(ProjectError) as raised:
        StateFileStore(path).read_local()
    [diagnostic] = raised.value.diagnostics
    return diagnostic


def cached_state(*, recorded_for: TargetIdentity | None = None, last_run: LastRun | None = None) -> State:
    """A local cache holding one applied view, for `recorded_for`, the test target unless given."""
    return State(
        2,
        recorded_for or target(),
        "m" * 64,
        "sst_config.yml",
        last_run,
        MappingProxyType({"semantic_view:v": ENTRY}),
    )
