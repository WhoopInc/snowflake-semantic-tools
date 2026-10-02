"""SST-MAN002: the compiled manifest is not valid JSON."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import manifest_refusal, valid_manifest, written


def test_sst_man002_fires(tmp_path: Path) -> None:
    path = written(tmp_path, "{not json")
    diagnostic = manifest_refusal(path)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN002", Severity.ERROR)
    assert diagnostic.message.startswith(f"{path} is not a readable SST manifest: Expecting property name")


def test_sst_man002_silent(tmp_path: Path) -> None:
    assert ManifestFileStore(written(tmp_path, valid_manifest().as_dict())).read() == valid_manifest()
