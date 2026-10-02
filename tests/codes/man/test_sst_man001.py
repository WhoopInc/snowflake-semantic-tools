"""SST-MAN001: no compiled SST manifest where plan, apply, list and the suites read it."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.artifact_builders import manifest, rendered


def test_sst_man001_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        compiled_manifest(tmp_path)
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN001", Severity.ERROR)
    assert diagnostic.message == f"no SST manifest at {tmp_path / 'target' / 'sst' / 'manifest.json'}"


def test_sst_man001_silent(tmp_path: Path) -> None:
    written = manifest({"semantic_view:v": rendered()})
    ManifestFileStore(tmp_path / "target" / "sst" / "manifest.json").write(written)
    assert compiled_manifest(tmp_path) == written
