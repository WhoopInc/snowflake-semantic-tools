"""SST-MAN201: a schema-1 manifest was migrated in memory by a command that reads it."""

from __future__ import annotations

import json
from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.app.manifest import read_notes
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import valid_manifest, written


def test_sst_man201_fires(tmp_path: Path) -> None:
    document = valid_manifest().as_dict()
    document["schema_version"] = 1
    path = written(tmp_path, document)
    read = ManifestFileStore(path).read()
    assert read is not None and read.migrated_from == 1
    [diagnostic] = read_notes(read, str(path))
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN201", Severity.INFO)
    assert diagnostic.message == f"{path} schema 1 migrated to 2 in memory"
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == 1


def test_sst_man201_silent(tmp_path: Path) -> None:
    path = written(tmp_path, valid_manifest().as_dict())
    read = ManifestFileStore(path).read()
    assert read is not None and read_notes(read, str(path)) == ()
