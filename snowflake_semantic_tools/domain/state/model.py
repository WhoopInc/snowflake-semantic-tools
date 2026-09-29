"""Deterministic manifest, state, and saved-plan values."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from enum import Enum
from hashlib import sha256
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping, cast

from ..model.identifier import TargetIdentity
from ..model.lifecycle import Action, Change, ChangeReason, ChangeSet, RenderedArtifact

MANIFEST_SCHEMA_VERSION = 2
STATE_SCHEMA_VERSION = 2
PLAN_SCHEMA_VERSION = 2
SST_VERSION = "1.0.0.dev0"


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def content_hash(value: object) -> str:
    return sha256(canonical_json(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class ArtifactEntry:
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
            "component_fingerprints": {key: item for key, item in self.component_fingerprints},
            "physical_resources": [
                {"object_type": object_type, "qualified_name": qualified_name}
                for object_type, qualified_name in self.physical_resources
            ],
        }

    @classmethod
    def from_dict(cls, value: object) -> ArtifactEntry:
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
        parsed_resources: list[tuple[str, str]] = []
        for resource in resources:
            if not isinstance(resource, dict):
                raise ValueError("artifact physical resource must be an object")
            parsed_resources.append((str(resource["object_type"]), str(resource["qualified_name"])))
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
            component_fingerprints=tuple(sorted((str(key), str(item)) for key, item in components.items())),
            physical_resources=tuple(parsed_resources),
        )


@dataclass(frozen=True, slots=True)
class ImpactIndex:
    by_file: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    by_dbt_model: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    by_member: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))

    def as_dict(self) -> dict[str, object]:
        return {
            "by_file": {key: list(value) for key, value in sorted(self.by_file.items())},
            "by_dbt_model": {key: list(value) for key, value in sorted(self.by_dbt_model.items())},
            "by_member": {key: list(value) for key, value in sorted(self.by_member.items())},
        }

    @classmethod
    def from_dict(cls, value: object) -> ImpactIndex:
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
        document = self.as_dict()
        document.pop("manifest_id")
        document.pop("schema_version")
        return document

    def with_computed_id(self) -> Manifest:
        return replace(self, manifest_id=content_hash(self.hash_material()))

    def as_dict(self) -> dict[str, object]:
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
        if not isinstance(value, dict):
            raise ValueError("manifest must be an object")
        version = value.get("schema_version")
        if not isinstance(version, int):
            raise ValueError("manifest schema_version is required")
        if version > MANIFEST_SCHEMA_VERSION:
            raise ValueError(f"manifest schema {version} is newer than {MANIFEST_SCHEMA_VERSION}")
        if version < MANIFEST_SCHEMA_VERSION:
            value = migrate_manifest(value)
            version = MANIFEST_SCHEMA_VERSION
        raw_artifacts = value.get("artifacts")
        if not isinstance(raw_artifacts, dict):
            raise ValueError("manifest artifacts is required")
        manifest = _manifest_from_dict_unchecked(value)
        expected = content_hash(manifest.hash_material())
        if manifest.manifest_id != expected:
            raise ValueError(f"manifest_id {manifest.manifest_id}, recomputed {expected}")
        return manifest


def migrate_manifest(value: Mapping[str, object]) -> dict[str, object]:
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
    raise ValueError(f"manifest schema {version} has no migration")


def _manifest_from_dict_unchecked(value: Mapping[str, object]) -> Manifest:
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


class ResourceStatus(Enum):
    VERIFIED = "verified"
    RETAINED = "retained"
    UNVERIFIED_AFTER_WRITE = "unverified_after_write"


@dataclass(frozen=True, slots=True, order=True)
class AppliedResource:
    object_type: str
    qualified_name: str
    status: ResourceStatus = ResourceStatus.VERIFIED

    def __post_init__(self) -> None:
        object.__setattr__(self, "object_type", self.object_type.upper())

    def as_dict(self) -> dict[str, str]:
        return {
            "object_type": self.object_type,
            "qualified_name": self.qualified_name,
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, value: object) -> AppliedResource:
        if not isinstance(value, dict):
            raise ValueError("applied physical resource must be an object")
        try:
            status = ResourceStatus(str(value.get("status", ResourceStatus.VERIFIED.value)))
        except ValueError as exc:
            raise ValueError(f"applied physical resource has invalid status {value.get('status')!r}") from exc
        return cls(
            object_type=str(value.get("object_type") or ""),
            qualified_name=str(value.get("qualified_name") or ""),
            status=status,
        )

    def __iter__(self) -> Iterator[str]:
        yield self.object_type
        yield self.qualified_name


AppliedResourceInput = AppliedResource | tuple[str, str] | tuple[str, str, str | ResourceStatus]


def _applied_resources(values: Iterable[AppliedResourceInput]) -> tuple[AppliedResource, ...]:
    resources = []
    for value in values:
        if isinstance(value, AppliedResource):
            resources.append(value)
            continue
        if len(value) == 2:
            object_type, qualified_name = value
            status = ResourceStatus.VERIFIED
        elif len(value) == 3:
            object_type, qualified_name, raw_status = value
            status = raw_status if isinstance(raw_status, ResourceStatus) else ResourceStatus(raw_status)
        else:
            raise ValueError("applied physical resource tuple must have two or three values")
        resources.append(AppliedResource(object_type, qualified_name, status))
    return tuple(resources)


# An executed prune that deactivates rather than drops leaves this outcome: the
# object is still SST's, so the entry is kept as a tombstone it can reactivate.
DEACTIVATED = "deactivated"
# A publish that wrote something and then failed; the next plan retries it.
FAILED_AFTER_WRITE = "failed_after_write"


@dataclass(frozen=True, slots=True)
class AppliedEntry:
    fingerprint: str
    qualified_name: str
    applied_at: str
    run_id: str
    outcome: str
    ddl_sha256: str
    manifest_id: str
    git_sha: str = ""
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[AppliedResourceInput, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "component_fingerprints", tuple(sorted(self.component_fingerprints)))
        object.__setattr__(self, "physical_resources", _applied_resources(self.physical_resources))

    @property
    def applied_resources(self) -> tuple[AppliedResource, ...]:
        return cast(tuple[AppliedResource, ...], self.physical_resources)

    def as_dict(self) -> dict[str, object]:
        return {
            "fingerprint": self.fingerprint,
            "qualified_name": self.qualified_name,
            "applied_at": self.applied_at,
            "run_id": self.run_id,
            "outcome": self.outcome,
            "ddl_sha256": self.ddl_sha256,
            "manifest_id": self.manifest_id,
            "git_sha": self.git_sha,
            "component_fingerprints": {key: item for key, item in self.component_fingerprints},
            "physical_resources": [resource.as_dict() for resource in self.applied_resources],
        }

    @classmethod
    def from_dict(cls, value: object) -> AppliedEntry:
        if not isinstance(value, dict):
            raise ValueError("applied entry must be an object")
        components = value.get("component_fingerprints", {})
        resources = value.get("physical_resources", [])
        if not isinstance(components, dict) or not isinstance(resources, list):
            raise ValueError("applied entry component metadata must be structured")
        parsed_resources = tuple(AppliedResource.from_dict(resource) for resource in resources)
        return cls(
            fingerprint=str(value.get("fingerprint", "")),
            qualified_name=str(value.get("qualified_name", "")),
            applied_at=str(value.get("applied_at", "")),
            run_id=str(value.get("run_id", "")),
            outcome=str(value.get("outcome", "")),
            ddl_sha256=str(value.get("ddl_sha256", "")),
            manifest_id=str(value.get("manifest_id", "")),
            git_sha=str(value.get("git_sha", "")),
            component_fingerprints=tuple(sorted((str(key), str(item)) for key, item in components.items())),
            physical_resources=parsed_resources,
        )


@dataclass(frozen=True, slots=True)
class LastRun:
    run_id: str
    started_at: str
    finished_at: str
    sst_version: str
    command: str
    outcome: str
    actor: str = ""

    def as_dict(self) -> dict[str, str]:
        return {name: str(getattr(self, name)) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class State:
    schema_version: int
    target: TargetIdentity
    manifest_id: str
    config_path: str
    last_run: LastRun | None
    applied: Mapping[str, AppliedEntry]

    @classmethod
    def empty(cls, target: TargetIdentity, config_path: str = "sst_config.yml") -> State:
        return cls(STATE_SCHEMA_VERSION, target, "", config_path, None, MappingProxyType({}))

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "target": self.target.as_dict(),
            "manifest_id": self.manifest_id,
            "config_path": self.config_path,
            "last_run": self.last_run.as_dict() if self.last_run else None,
            "applied": {key: value.as_dict() for key, value in sorted(self.applied.items())},
            "observed": None,
            "lock": None,
        }

    @classmethod
    def from_dict(cls, value: object) -> State:
        if not isinstance(value, dict):
            raise ValueError("state must be an object")
        version = value.get("schema_version")
        if version is None or not isinstance(version, int):
            raise ValueError(f"state schema {version} is not supported")
        if version > STATE_SCHEMA_VERSION:
            raise ValueError(f"state schema {version} is not supported")
        if version < STATE_SCHEMA_VERSION:
            value = migrate_state(value)
            version = STATE_SCHEMA_VERSION
        raw_applied = value.get("applied")
        if not isinstance(raw_applied, dict):
            raise ValueError("state.applied must be an object")
        last_run = value.get("last_run")
        parsed_last: LastRun | None = None
        if isinstance(last_run, dict):
            parsed_last = LastRun(*(str(last_run.get(name, "")) for name in LastRun.__dataclass_fields__))
        return cls(
            schema_version=version,
            target=TargetIdentity.from_dict(value.get("target")),
            manifest_id=str(value.get("manifest_id") or ""),
            config_path=str(value.get("config_path") or "sst_config.yml"),
            last_run=parsed_last,
            applied=MappingProxyType({str(key): AppliedEntry.from_dict(item) for key, item in raw_applied.items()}),
        )


def migrate_state(value: Mapping[str, object]) -> dict[str, object]:
    version = value.get("schema_version")
    if version != 1:
        raise ValueError(f"state schema {version} has no migration")
    migrated = dict(value)
    raw_applied = value.get("applied")
    if not isinstance(raw_applied, dict):
        raise ValueError("state.applied must be an object")
    applied: dict[str, object] = {}
    for key, raw_entry in raw_applied.items():
        if not isinstance(raw_entry, dict):
            raise ValueError("applied entry must be an object")
        entry = dict(raw_entry)
        entry.setdefault("component_fingerprints", {})
        qualified_name = str(entry.get("qualified_name") or "")
        entry.setdefault(
            "physical_resources",
            (
                [
                    {
                        "object_type": "",
                        "qualified_name": qualified_name,
                        "status": ResourceStatus.VERIFIED.value,
                    }
                ]
                if qualified_name
                else []
            ),
        )
        applied[str(key)] = entry
    migrated["schema_version"] = STATE_SCHEMA_VERSION
    migrated["applied"] = applied
    return migrated


@dataclass(frozen=True, slots=True)
class SavedChange:
    key: str
    artifact_type: str
    action: str
    reason: str
    target: str | None
    fingerprint: str | None
    previous_marker: str | None
    statement_hashes: tuple[str, ...]
    depends_on: tuple[str, ...]
    order: int
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, str], ...] = ()
    prune_executable: bool = True

    @classmethod
    def from_change(cls, change: Change) -> SavedChange:
        return cls(
            key=change.key,
            artifact_type=change.artifact_type,
            action=change.action.value,
            reason=change.reason.value,
            target=(
                change.rendered.target.sql
                if change.rendered is not None
                else change.observed.qualified_name.sql if change.observed is not None else None
            ),
            fingerprint=change.rendered.fingerprint if change.rendered else None,
            previous_marker=(
                change.observed.marker.text
                if change.observed and change.observed.marker
                else (
                    content_hash(
                        {
                            "resources": [
                                (resource.object_type, resource.qualified_name.sql, resource.exists)
                                for resource in change.composite_observation.resources
                            ],
                            "stage_exists": change.composite_observation.stage_exists,
                            "stage_file_format": change.composite_observation.stage_file_format,
                            "config_path": change.composite_observation.config_path,
                            "config_exists": change.composite_observation.config_exists,
                            "config_size": change.composite_observation.config_size,
                            "config_md5": change.composite_observation.config_md5,
                            # Only handlers that record details add the key, so
                            # an eval plan's hash is unchanged by its existence.
                            **(
                                {"details": [list(item) for item in change.composite_observation.details]}
                                if change.composite_observation.details
                                else {}
                            ),
                        }
                    )
                    if change.composite_observation is not None
                    else None
                )
            ),
            statement_hashes=(
                tuple(sha256(statement.encode("utf-8")).hexdigest() for statement in change.rendered.statements)
                if change.rendered is not None
                else ()
            ),
            depends_on=change.depends_on,
            order=change.order,
            component_fingerprints=(
                tuple(sorted(change.rendered.component_fingerprints)) if change.rendered is not None else ()
            ),
            physical_resources=(
                tuple((object_type, name.sql) for object_type, name in change.rendered.physical_resources)
                if change.rendered is not None
                else ()
            ),
            prune_executable=change.prune_executable,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "artifact_type": self.artifact_type,
            "action": self.action,
            "reason": self.reason,
            "target": self.target,
            "fingerprint": self.fingerprint,
            "previous_marker": self.previous_marker,
            "statement_hashes": list(self.statement_hashes),
            "depends_on": list(self.depends_on),
            "order": self.order,
            "component_fingerprints": {key: item for key, item in self.component_fingerprints},
            "physical_resources": [
                {"object_type": object_type, "qualified_name": qualified_name}
                for object_type, qualified_name in self.physical_resources
            ],
            "prune_executable": self.prune_executable,
        }


@dataclass(frozen=True, slots=True)
class SavedPlan:
    schema_version: int
    plan_id: str
    manifest_id: str
    target: TargetIdentity
    observation_at: str
    observation_fingerprint: str
    changes: tuple[SavedChange, ...]
    selected: tuple[str, ...] = ()
    excluded: tuple[str, ...] = ()
    include_prune: bool = False

    @classmethod
    def from_changeset(
        cls,
        changeset: ChangeSet,
        *,
        selected: tuple[str, ...] = (),
        excluded: tuple[str, ...] = (),
        include_prune: bool = False,
    ) -> SavedPlan:
        changes = tuple(SavedChange.from_change(change) for change in changeset.changes)
        observed = [
            {
                "key": change.key,
                "marker": change.previous_marker,
                "target": change.target,
                "components": dict(change.component_fingerprints),
                "resources": list(change.physical_resources),
            }
            for change in changes
        ]
        observation_fingerprint = content_hash(observed)
        body = {
            "schema_version": PLAN_SCHEMA_VERSION,
            "manifest_id": changeset.manifest_id,
            "target": changeset.target.as_dict(),
            "observation_at": changeset.observation_at,
            "observation_fingerprint": observation_fingerprint,
            "changes": [change.as_dict() for change in changes],
            "selection": {
                "selected": list(selected),
                "excluded": list(excluded),
                "include_prune": include_prune,
            },
        }
        return cls(
            PLAN_SCHEMA_VERSION,
            content_hash(body),
            changeset.manifest_id,
            changeset.target,
            changeset.observation_at,
            observation_fingerprint,
            changes,
            selected,
            excluded,
            include_prune,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "manifest_id": self.manifest_id,
            "target": self.target.as_dict(),
            "observation_at": self.observation_at,
            "observation_fingerprint": self.observation_fingerprint,
            "changes": [change.as_dict() for change in self.changes],
            "selection": {
                "selected": list(self.selected),
                "excluded": list(self.excluded),
                "include_prune": self.include_prune,
            },
        }

    def matches(self, manifest_id: str, target: TargetIdentity) -> bool:
        return self.manifest_id == manifest_id and self.target.key == target.key


def _object_map(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("expected an object")
    return {str(key): item for key, item in value.items()}


def _string_map(value: object) -> dict[str, str]:
    return {key: str(item) for key, item in _object_map(value).items()}


def build_minimal_manifest(
    artifacts: Mapping[str, RenderedArtifact],
    *,
    project: Mapping[str, object],
    sources: Mapping[str, object],
    members: Mapping[str, object],
    files: Mapping[str, object],
    impact: ImpactIndex,
    diagnostics_summary: Mapping[str, int],
) -> Manifest:
    entries = {
        key: ArtifactEntry(
            type=artifact.artifact_type,
            name=artifact.target.name.folded.casefold(),
            fingerprint=artifact.fingerprint,
            source_files=(),
            member_keys=(),
            depends_on=artifact.depends_on,
            publish_target=artifact.target.sql,
            byte_length=len(artifact.ddl.encode("utf-8")),
            object_type=artifact.object_type,
            render_dialect=artifact.render_dialect,
            component_fingerprints=artifact.component_fingerprints,
            physical_resources=tuple((object_type, name.sql) for object_type, name in artifact.physical_resources),
        )
        for key, artifact in artifacts.items()
    }
    manifest = Manifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        manifest_id="",
        generator=MappingProxyType({"name": "sst", "version": SST_VERSION}),
        project=MappingProxyType(dict(project)),
        sources=MappingProxyType(dict(sources)),
        artifacts=MappingProxyType(entries),
        members=MappingProxyType(dict(members)),
        dbt_models=MappingProxyType({}),
        files=MappingProxyType(dict(files)),
        impact=impact,
        diagnostics_summary=MappingProxyType(dict(diagnostics_summary)),
    )
    return manifest.with_computed_id()
