"""SST-MAN203: a manifest declares a schema newer than this SST reads."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import manifest_refusal, valid_manifest, written


def test_sst_man203_fires(tmp_path: Path) -> None:
    document = valid_manifest().as_dict()
    document["schema_version"] = 3
    path = written(tmp_path, document)
    diagnostic = manifest_refusal(path)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN203", Severity.ERROR)
    assert diagnostic.message == f"{path} schema 3; this binary supports 2"


def test_sst_man203_silent(tmp_path: Path) -> None:
    assert ManifestFileStore(written(tmp_path, valid_manifest().as_dict())).read() == valid_manifest()
