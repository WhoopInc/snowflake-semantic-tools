"""SST-MAN004: the manifest's impact index leaves an artifact out under a file it came from."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.state import ImpactIndex, Manifest
from tests.helpers.stored_documents import manifest_refusal, valid_manifest, written


def indexed(*, listed: bool) -> Manifest:
    base = valid_manifest()
    entry = replace(base.artifacts["semantic_view:v"], source_files=("semantic_models/v.yml",))
    by_file = {"semantic_models/v.yml": ("semantic_view:v",)} if listed else {}
    return replace(
        base, artifacts=MappingProxyType({"semantic_view:v": entry}), impact=ImpactIndex(MappingProxyType(by_file))
    ).with_computed_id()


def test_sst_man004_fires(tmp_path: Path) -> None:
    diagnostic = manifest_refusal(written(tmp_path, indexed(listed=False).as_dict()))
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN004", Severity.ERROR)
    assert diagnostic.message == "semantic_view:v has no reverse-index entry"


def test_sst_man004_silent(tmp_path: Path) -> None:
    complete = indexed(listed=True)
    assert ManifestFileStore(written(tmp_path, complete.as_dict())).read() == complete
