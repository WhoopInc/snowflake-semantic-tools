"""SST-MAN005: the manifest's id is not the hash of its content."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import manifest_refusal, valid_manifest, written


def test_sst_man005_fires(tmp_path: Path) -> None:
    document = valid_manifest().as_dict()
    document["manifest_id"] = "0" * 64
    diagnostic = manifest_refusal(written(tmp_path, document))
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN005", Severity.ERROR)
    assert diagnostic.message == f"manifest_id {'0' * 64}, recomputed {valid_manifest().manifest_id}"


def test_sst_man005_silent(tmp_path: Path) -> None:
    assert ManifestFileStore(written(tmp_path, valid_manifest().as_dict())).read() == valid_manifest()
