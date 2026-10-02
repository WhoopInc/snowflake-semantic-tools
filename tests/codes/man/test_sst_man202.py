"""SST-MAN202: a manifest of a schema too old to migrate is discarded for a full recompile."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import manifest_refusal, valid_manifest, written


def test_sst_man202_fires(tmp_path: Path) -> None:
    document = valid_manifest().as_dict()
    document["schema_version"] = 0
    path = written(tmp_path, document)
    diagnostic = manifest_refusal(path)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN202", Severity.WARNING)
    assert diagnostic.message == f"{path} schema 0 has no migration; full recompile"


def test_sst_man202_silent(tmp_path: Path) -> None:
    document = valid_manifest().as_dict()
    document["schema_version"] = 1
    assert ManifestFileStore(written(tmp_path, document)).read() is not None
