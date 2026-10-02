"""Reading stored state and manifests: a writer's release, and an incomplete impact index."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.state import ImpactIndex, Manifest, StoredDocumentError, written_by_newer
from tests.helpers.artifact_builders import manifest, rendered


@pytest.mark.parametrize(
    ("writer", "current", "newer"),
    [
        ("1.0.1", "1.0.0", True),
        ("2.0", "1.9.9", True),
        ("1.0.0", "1.0.0.dev0", False),
        ("1.0.0.dev3", "1.0.0", False),
        ("1.0.0rc1", "1.0.0", False),
        ("0.3.1", "1.0.0", False),
        ("unknown", "1.0.0", False),
        ("", "1.0.0", False),
        ("1.1", "1.0.9", True),
    ],
)
def test_a_writer_is_newer_only_by_its_leading_release_numbers(writer: str, current: str, newer: bool) -> None:
    assert written_by_newer(writer, current) is newer


def indexed(files: dict[str, tuple[str, ...]]) -> Manifest:
    base = manifest({"semantic_view:v": rendered()})
    entry = replace(base.artifacts["semantic_view:v"], source_files=("a.yml", "b.yml"))
    return replace(
        base, artifacts=MappingProxyType({"semantic_view:v": entry}), impact=ImpactIndex(MappingProxyType(files))
    ).with_computed_id()


def test_an_artifact_left_out_under_any_of_its_files_is_refused() -> None:
    complete = indexed({"a.yml": ("semantic_view:v",), "b.yml": ("semantic_view:v",)})
    assert complete.unindexed() is None
    assert Manifest.from_dict(complete.as_dict()) == complete
    partial = indexed({"a.yml": ("semantic_view:v",)})
    assert partial.unindexed() == "semantic_view:v"
    with pytest.raises(StoredDocumentError) as raised:
        Manifest.from_dict(partial.as_dict())
    assert (raised.value.code, raised.value.context) == ("SST-MAN004", {"artifact": "semantic_view:v"})


def test_a_migrated_manifest_records_its_schema_and_skips_the_index_it_never_had() -> None:
    document = indexed({}).as_dict()
    document["schema_version"] = 1
    migrated = Manifest.from_dict(document)
    assert migrated.migrated_from == 1 and migrated.schema_version == 2
    assert Manifest.from_dict(indexed({"a.yml": ("semantic_view:v",), "b.yml": ("semantic_view:v",)}).as_dict())
