"""Reading stored state and manifests: a writer's release, and a manifest of an older schema."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.state import (
    MANIFEST_SCHEMA_VERSION,
    Manifest,
    StoredDocumentError,
    written_by_newer,
)
from tests.helpers.artifact_builders import manifest, rendered
from tests.helpers.stored_documents import schema_2_document, schema_3_document


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


def sourced() -> Manifest:
    base = manifest({"semantic_view:v": rendered()})
    entry = replace(base.artifacts["semantic_view:v"], source_files=("a.yml", "b.yml"))
    return replace(base, artifacts=MappingProxyType({"semantic_view:v": entry})).with_computed_id()


def test_a_written_manifest_has_no_impact_index() -> None:
    assert "impact" not in sourced().as_dict()
    assert sourced().schema_version == MANIFEST_SCHEMA_VERSION == 6


def test_a_schema_2_manifest_reads_with_its_impact_index_ignored() -> None:
    current = sourced()
    document = schema_2_document(current, {"by_file": {"a.yml": ["semantic_view:v"]}, "by_member": {}})
    read = Manifest.from_dict(document)
    assert read == current and read.migrated_from == 2
    assert "impact" in document and document["schema_version"] == 2
    # An index that leaves an artifact out under one of its files no longer refuses the read.
    assert Manifest.from_dict(schema_2_document(current, {"by_file": {}})) == current


def test_an_edited_schema_2_manifest_is_still_refused() -> None:
    document = schema_2_document(sourced())
    document["impact"] = {"by_file": {"a.yml": ["semantic_view:v"]}}
    with pytest.raises(StoredDocumentError) as raised:
        Manifest.from_dict(document)
    assert raised.value.code == "SST-MAN005"


def test_a_schema_1_manifest_migrates_and_drops_any_impact_key() -> None:
    document = schema_2_document(sourced())
    document["schema_version"] = 1
    migrated = Manifest.from_dict(document)
    assert migrated.migrated_from == 1 and migrated.schema_version == MANIFEST_SCHEMA_VERSION
    assert "impact" not in migrated.as_dict()


def test_a_schema_3_manifest_migrates_with_empty_references() -> None:
    current = sourced()
    read = Manifest.from_dict(schema_3_document(current))
    assert read == current and read.migrated_from == 3
    assert all(entry.references == () for entry in read.artifacts.values())


def test_an_edited_schema_3_manifest_is_still_refused() -> None:
    document = schema_3_document(sourced())
    artifacts = document["artifacts"]
    assert isinstance(artifacts, dict)
    artifacts["semantic_view:v"]["source_files"] = ["c.yml"]
    with pytest.raises(StoredDocumentError) as raised:
        Manifest.from_dict(document)
    assert raised.value.code == "SST-MAN005"
