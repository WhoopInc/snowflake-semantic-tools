"""The compile manifest: every rendered artifact, the files it came from, and the inputs it uses.

`Manifest.as_dict` is the `manifest.json` document. Its `manifest_id` is the `content_hash`
of everything but the id and the schema version, so `Manifest.from_dict` detects an edited
manifest. A schema-1 to schema-5 manifest is migrated in memory, and records the
schema it was read from; an older schema, which has no migration, and a newer one are refused.
Nothing here rewrites the file.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.state.codec import (
    optional_object,
    pairs_from_json,
    pairs_to_json,
    resources_from_json,
    resources_to_json,
)
from snowflake_semantic_tools.domain.state.documents import StoredDocumentError, content_hash

MANIFEST_SCHEMA_VERSION = 6

_UNHASHED = ("manifest_id", "schema_version")


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
        references: What the artifact names but does not wait on, such as a profile's skills
            and commands; never a publish edge, so `depends_on` does not hold them.
        settings: The artifact's resolved settings, each with where its value came from, sorted
            by key; empty for a tool, and in a manifest written before they were recorded.
        member_fingerprints: The fingerprint of each member of the artifact's definition, keyed
            `<kind>:<name>`; empty for an artifact plan does not compare.
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
    references: tuple[str, ...] = ()
    settings: tuple[Setting, ...] = ()
    member_fingerprints: tuple[tuple[str, str], ...] = ()

    def as_dict(self) -> dict[str, object]:
        """Return the entry as `manifest.json` stores it under `artifacts`."""
        return {
            "type": self.type,
            "name": self.name,
            "fingerprint": self.fingerprint,
            "source_files": list(self.source_files),
            "member_keys": list(self.member_keys),
            "depends_on": list(self.depends_on),
            "references": list(self.references),
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
            "settings": {item.key: item.as_dict() for item in self.settings},
            "member_fingerprints": pairs_to_json(self.member_fingerprints),
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
        members = value.get("member_fingerprints", {})
        if not isinstance(components, dict) or not isinstance(resources, list) or not isinstance(members, dict):
            raise ValueError("artifact entry component metadata must be structured")
        parsed_resources = resources_from_json(resources, "artifact physical resource must be an object")
        settings = value.get("settings", {})
        if not isinstance(settings, dict) or not all(isinstance(item, dict) for item in settings.values()):
            raise ValueError("artifact settings must be an object of objects")
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
            references=tuple(str(item) for item in value.get("references", [])),
            settings=tuple(
                Setting(str(key), item.get("value"), str(item.get("source", ""))) for key, item in settings.items()
            ),
            member_fingerprints=pairs_from_json(members),
        )


@dataclass(frozen=True, slots=True)
class Manifest:
    """The compile manifest, identified by the hash of its content.

    `manifest_id` is empty until `with_computed_id` fills it. The mappings besides `artifacts`
    hold what compile recorded; nothing in this module reads into them.

    Attributes:
        members: By member key, the views it attaches to (`attached_to`), the dbt models and
            sources under the tables it reads (`tables`), the members it uses (`uses`), the
            columns it reads as `<table>.<column>` (`columns`), the columns it names that no
            one table was found for (`unresolved_columns`), and a fact's, dimension's, filter's
            or metric's resolved expression (`expr`).
        dbt_nodes: dbt's graph, by unique id: each node's `kind`, `name`, `package`, `relation`
            for the target, and the unique ids it `depends_on`.
        generator: The writer's name and SST version.
        diagnostics_summary: How many diagnostics compile reported, per severity.
        migrated_from: The schema the stored document declared when `from_dict` migrated it
            in memory; None when it was current. Never written, hashed, or compared.
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
    diagnostics_summary: Mapping[str, int]
    dbt_nodes: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    migrated_from: int | None = field(default=None, compare=False)

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
            "members": dict(sorted(self.members.items())),
            "dbt_models": dict(sorted(self.dbt_models.items())),
            "dbt_nodes": dict(sorted(self.dbt_nodes.items())),
            "files": dict(sorted(self.files.items())),
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
        raw_artifacts = value.get("artifacts")
        if not isinstance(raw_artifacts, dict):
            raise StoredDocumentError("SST-MAN003", "manifest artifacts is required", key="artifacts")
        migrated_from = version if version < MANIFEST_SCHEMA_VERSION else None
        if migrated_from is not None:
            value = migrate_manifest(value)
        manifest = _manifest_from_dict_unchecked(value)
        expected = content_hash(manifest.hash_material())
        if manifest.manifest_id != expected:
            raise StoredDocumentError(
                "SST-MAN005",
                f"manifest_id {manifest.manifest_id}, recomputed {expected}",
                found=manifest.manifest_id,
                expected=expected,
            )
        if migrated_from is not None:
            return replace(manifest, migrated_from=migrated_from)
        return manifest


def migrate_manifest(value: Mapping[str, object]) -> dict[str, object]:
    """Migrate a schema-1 to schema-5 manifest document to the current schema, leaving the input unchanged.

    The sections schema 2 added start empty. Schema 3 stopped writing the impact index, so an
    `impact` key is dropped unread. Schema 4 records each artifact's `references`, which start
    empty. Schema 5 records dbt's graph, which starts empty, and each member's tables and uses,
    which a member without them lacks. Schema 6 records the columns each member reads, which a
    member migrated from an earlier schema lacks until the project compiles again. The migrated
    document's id is recomputed. A schema-2 to schema-5 id is first checked against the document
    as stored, so an edited manifest of any of them is still refused; a schema-1 id is not, so an
    edit to a schema-1 manifest goes undetected.

    Raises:
        StoredDocumentError: SST-MAN202 when the document is not schema 1 to 5; SST-MAN005 when a
            schema-2 to schema-5 document's `manifest_id` is not the hash of its content.
    """
    version = value.get("schema_version")
    if version not in (1, 2, 3, 4, 5):
        raise StoredDocumentError("SST-MAN202", f"manifest schema {version} has no migration", found=version)
    if version in (2, 3, 4, 5):
        found = value.get("manifest_id")
        expected = content_hash({key: item for key, item in value.items() if key not in _UNHASHED})
        if found != expected:
            raise StoredDocumentError(
                "SST-MAN005", f"manifest_id {found}, recomputed {expected}", found=found, expected=expected
            )
    migrated = {key: item for key, item in value.items() if key != "impact"}
    migrated["schema_version"] = MANIFEST_SCHEMA_VERSION
    migrated.setdefault("members", {})
    migrated.setdefault("dbt_models", {})
    migrated.setdefault("files", {})
    migrated.setdefault("dbt_nodes", {})
    manifest = _manifest_from_dict_unchecked(migrated)
    migrated["manifest_id"] = content_hash(manifest.hash_material())
    return migrated


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
        diagnostics_summary=MappingProxyType(parsed_summary),
        dbt_nodes=MappingProxyType(_object_map(value.get("dbt_nodes"))),
    )


def _object_map(value: object) -> dict[str, object]:
    return optional_object(value, "expected an object")


def _string_map(value: object) -> dict[str, str]:
    return {key: str(item) for key, item in _object_map(value).items()}
