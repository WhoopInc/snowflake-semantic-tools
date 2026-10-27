"""SST-MAN003: the compiled manifest omits a key every manifest has."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import manifest_refusal, valid_manifest, written


def test_sst_man003_fires(tmp_path: Path) -> None:
    document = valid_manifest().as_dict()
    del document["schema_version"]
    path = written(tmp_path, document)
    diagnostic = manifest_refusal(path)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN003", Severity.ERROR)
    assert diagnostic.message == f"{path} omits required key 'schema_version'"


def test_sst_man003_silent(tmp_path: Path) -> None:
    assert ManifestFileStore(written(tmp_path, valid_manifest().as_dict())).read() == valid_manifest()
