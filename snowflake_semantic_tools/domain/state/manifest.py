"""The compile manifest: every rendered artifact, the files it came from, and the inputs it uses.

`Manifest.as_dict` is the `manifest.json` document. Its `manifest_id` is the `content_hash`
of everything but the id and the schema version, so `Manifest.from_dict` detects an edited
manifest. A schema-1 manifest is migrated in memory and a newer schema refused; nothing here
rewrites the file.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Mapping

from .codec import optional_object, pairs_from_json, pairs_to_json, resources_from_json, resources_to_json
from .documents import StoredDocumentError, content_hash

MANIFEST_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class ArtifactEntry:
    """One rendered artifact as the manifest records it.

    Attributes:
        name: The artifact's name, casefolded.
        fingerprint: The rendered artifact's fingerprint; the document records it twice, as the
            entry's fingerprint and as the render's `sha256`.
        member_keys: The keys of the members the artifact carries, such as its metrics.
        publish_target: The qualified name the artifact publishes to.
        byte_length: The size of the rendered text, in UTF-8 bytes.
        object_type: The Snowflake object type; empty for an artifact that spans several.
        render_dialect: What the rendered text is, such as `ddl` or `json`.
        diagnostic_codes: The codes of the diagnostics compile reported against the artifact.
        component_fingerprints, physical_resources: A composite artifact's parts, and the
            objects it spans as `(object type, qualified name)`; empty for any other artifact.
    """

    type: str
    name: str
    fingerprint: str
    source_files: tuple[str, ...]
    member_keys: tuple[str, ...]
    depends_on: tuple[str, ...]
    publish_target: str
    byte_length: int
    object_type: str = "SEMANTIC VIEW"
    render_dialect: str = "ddl"
    diagnostic_codes: tuple[str, ...] = ()
    status: str = "ok"
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, str], ...] = ()

    def as_dict(self) -> dict[str, object]:
        """Return the entry as `manifest.json` stores it under `artifacts`."""
        return {
            "type": self.type,
            "name": self.name,
            "fingerprint": self.fingerprint,
            "source_files": list(self.source_files),
            "member_keys": list(self.member_keys),
            "depends_on": list(self.depends_on),
            "render": {
                "dialect": self.render_dialect,
                "byte_length": self.byte_length,
                "sha256": self.fingerprint,
            },
            "publish_target": {
                "object_type": self.object_type,
                "qualified_name": self.publish_target,
            },
            "status": self.status,
            "diagnostic_codes": list(self.diagnostic_codes),
            "component_fingerprints": pairs_to_json(self.component_fingerprints),
            "physical_resources": resources_to_json(self.physical_resources),
        }

    @classmethod
    def from_dict(cls, value: object) -> ArtifactEntry:
        """Read an entry as `as_dict` writes it; an optional key it lacks takes its default.

        Raises:
            ValueError: the entry, its `render`, its `publish_target`, or its component
                metadata is not the structure `as_dict` writes.
            KeyError: the entry lacks a required key, such as `type` or `render.byte_length`.
        """
        if not isinstance(value, dict):
            raise ValueError("artifact entry must be an object")
        rendered = value.get("render")
        target = value.get("publish_target")
        if not isinstance(rendered, dict) or not isinstance(target, dict):
            raise ValueError("artifact entry requires render and publish_target")
        components = value.get("component_fingerprints", {})
        resources = value.get("physical_resources", [])
        if not isinstance(components, dict) or not isinstance(resources, list):
            raise ValueError("artifact entry component metadata must be structured")
        parsed_resources = resources_from_json(resources, "artifact physical resource must be an object")
        return cls(
            type=str(value["type"]),
            name=str(value["name"]),
            fingerprint=str(value["fingerprint"]),
            source_files=tuple(str(item) for item in value.get("source_files", [])),
            member_keys=tuple(str(item) for item in value.get("member_keys", [])),
            depends_on=tuple(str(item) for item in value.get("depends_on", [])),
            publish_target=str(target["qualified_name"]),
            byte_length=int(rendered["byte_length"]),
            object_type=(str(target["object_type"]) if "object_type" in target else "SEMANTIC VIEW"),
            render_dialect=str(rendered.get("dialect") or "ddl"),
            diagnostic_codes=tuple(str(item) for item in value.get("diagnostic_codes", [])),
            status=str(value.get("status", "ok")),
            component_fingerprints=pairs_from_json(components),
            physical_resources=parsed_resources,
        )


@dataclass(frozen=True, slots=True)
class ImpactIndex:
    """Which artifacts use each input: each source file, dbt model, and member, by its key.

    Every value is a tuple of artifact keys; `as_dict` writes each mapping sorted by key.
    """

    by_file: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    by_dbt_model: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    by_member: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))

    def as_dict(self) -> dict[str, object]:
        """Return the index as `manifest.json` stores it under `impact`."""
        return {
            "by_file": {key: list(value) for key, value in sorted(self.by_file.items())},
            "by_dbt_model": {key: list(value) for key, value in sorted(self.by_dbt_model.items())},
            "by_member": {key: list(value) for key, value in sorted(self.by_member.items())},
        }

    @classmethod
    def from_dict(cls, value: object) -> ImpactIndex:
        """Read an index as `as_dict` writes it; a mapping it lacks is empty.

        Raises:
            ValueError: the index or one of its mappings is not an object.
        """
        if not isinstance(value, dict):
            raise ValueError("impact index must be an object")

        def entries(name: str) -> Mapping[str, tuple[str, ...]]:
            raw = value.get(name, {})
            if not isinstance(raw, dict):
                raise ValueError(f"impact.{name} must be an object")
            return MappingProxyType({str(key): tuple(str(item) for item in items) for key, items in raw.items()})

        return cls(entries("by_file"), entries("by_dbt_model"), entries("by_member"))


@dataclass(frozen=True, slots=True)
class Manifest:
    """The compile manifest, identified by the hash of its content.

    `manifest_id` is empty until `with_computed_id` fills it. The mappings besides `artifacts`
    and `impact` hold what compile recorded; nothing in this module reads into them.

    Attributes:
        generator: The writer's name and SST version.
        diagnostics_summary: How many diagnostics compile reported, per severity.
    """

    schema_version: int
    manifest_id: str
    generator: Mapping[str, str]
    project: Mapping[str, object]
    sources: Mapping[str, object]
    artifacts: Mapping[str, ArtifactEntry]
    members: Mapping[str, object]
    dbt_models: Mapping[str, object]
    files: Mapping[str, object]
    impact: ImpactIndex
    diagnostics_summary: Mapping[str, int]

    def hash_material(self) -> dict[str, object]:
        """Return what `manifest_id` hashes: the document without the id and the schema version."""
        document = self.as_dict()
        document.pop("manifest_id")
        document.pop("schema_version")
        return document

    def with_computed_id(self) -> Manifest:
        """Return a copy whose `manifest_id` is the hash of its content."""
        return replace(self, manifest_id=content_hash(self.hash_material()))

    def as_dict(self) -> dict[str, object]:
        """Return the `manifest.json` document, which `from_dict` reads back."""
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "generator": dict(self.generator),
            "project": dict(self.project),
            "sources": dict(self.sources),
            "artifacts": {key: value.as_dict() for key, value in sorted(self.artifacts.items())},
            "members": {key: value for key, value in sorted(self.members.items())},
            "dbt_models": {key: value for key, value in sorted(self.dbt_models.items())},
            "files": {key: value for key, value in sorted(self.files.items())},
            "impact": self.impact.as_dict(),
            "diagnostics_summary": dict(self.diagnostics_summary),
        }

    @classmethod
    def from_dict(cls, value: object) -> Manifest:
        """Read a stored manifest, migrating an older schema and refusing an id that is not its hash.

        Raises:
            StoredDocumentError: SST-MAN002 when the document is not an object; SST-MAN003 when
                it lacks an integer `schema_version` or an `artifacts` object; SST-MAN203 when
                its schema is newer; SST-MAN202 when an older schema has no migration; and
                SST-MAN005 when its `manifest_id` is not the hash of its content.
            ValueError: another part is malformed, such as an artifact entry or the summary.
            KeyError: an artifact entry lacks a required key.
        """
        if not isinstance(value, dict):
            raise StoredDocumentError(
                "SST-MAN002", "manifest must be an object", detail="the document is not an object"
            )
        version = value.get("schema_version")
        if not isinstance(version, int):
            raise StoredDocumentError("SST-MAN003", "manifest schema_version is required", key="schema_version")
        if version > MANIFEST_SCHEMA_VERSION:
            raise StoredDocumentError(
                "SST-MAN203",
                f"manifest schema {version} is newer than {MANIFEST_SCHEMA_VERSION}",
                found=version,
                expected=MANIFEST_SCHEMA_VERSION,
            )
        if version < MANIFEST_SCHEMA_VERSION:
            value = migrate_manifest(value)
            version = MANIFEST_SCHEMA_VERSION
        raw_artifacts = value.get("artifacts")
        if not isinstance(raw_artifacts, dict):
            raise StoredDocumentError("SST-MAN003", "manifest artifacts is required", key="artifacts")
        manifest = _manifest_from_dict_unchecked(value)
        expected = content_hash(manifest.hash_material())
        if manifest.manifest_id != expected:
            raise StoredDocumentError(
                "SST-MAN005",
                f"manifest_id {manifest.manifest_id}, recomputed {expected}",
                found=manifest.manifest_id,
                expected=expected,
            )
        return manifest


def migrate_manifest(value: Mapping[str, object]) -> dict[str, object]:
    """Migrate a schema-1 manifest document to the current schema, leaving the input unchanged.

    The sections schema 2 added start empty, and the id is recomputed rather than checked, so
    an edit to a schema-1 manifest goes undetected.

    Raises:
        StoredDocumentError: SST-MAN202 when the document is not schema 1.
    """
    version = value.get("schema_version")
    if version == 1:
        migrated = dict(value)
        migrated["schema_version"] = MANIFEST_SCHEMA_VERSION
        migrated.setdefault("members", {})
        migrated.setdefault("dbt_models", {})
        migrated.setdefault("files", {})
        migrated.setdefault("impact", {"by_file": {}, "by_dbt_model": {}, "by_member": {}})
        manifest = _manifest_from_dict_unchecked(migrated)
        migrated["manifest_id"] = content_hash(manifest.hash_material())
        return migrated
    raise StoredDocumentError("SST-MAN202", f"manifest schema {version} has no migration", found=version)


def _manifest_from_dict_unchecked(value: Mapping[str, object]) -> Manifest:
    """Read a manifest document of the current shape without checking its schema version or id.

    Raises:
        ValueError: `artifacts`, `schema_version`, or a section is not the type `as_dict` writes.
    """
    raw_artifacts = value.get("artifacts", {})
    if not isinstance(raw_artifacts, dict):
        raise ValueError("manifest artifacts must be an object")
    raw_version = value.get("schema_version")
    if not isinstance(raw_version, int):
        raise ValueError("manifest schema_version must be an integer")
    summary = _object_map(value.get("diagnostics_summary"))
    parsed_summary: dict[str, int] = {}
    for key, item in summary.items():
        if not isinstance(item, int):
            raise ValueError(f"diagnostics_summary.{key} must be an integer")
        parsed_summary[str(key)] = item
    return Manifest(
        schema_version=raw_version,
        manifest_id=str(value.get("manifest_id") or ""),
        generator=MappingProxyType(_string_map(value.get("generator"))),
        project=MappingProxyType(_object_map(value.get("project"))),
        sources=MappingProxyType(_object_map(value.get("sources"))),
        artifacts=MappingProxyType({str(key): ArtifactEntry.from_dict(item) for key, item in raw_artifacts.items()}),
        members=MappingProxyType(_object_map(value.get("members"))),
        dbt_models=MappingProxyType(_object_map(value.get("dbt_models"))),
        files=MappingProxyType(_object_map(value.get("files"))),
        impact=ImpactIndex.from_dict(value.get("impact", {})),
        diagnostics_summary=MappingProxyType(parsed_summary),
    )


def _object_map(value: object) -> dict[str, object]:
    return optional_object(value, "expected an object")


def _string_map(value: object) -> dict[str, str]:
    return {key: str(item) for key, item in _object_map(value).items()}
