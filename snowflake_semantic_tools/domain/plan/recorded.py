"""What a plan read of one target, recorded so a later plan can plan from it instead.

`sst plan` reads three things of its target before it decides: every object it observes,
the preflight facts about the target, and the state it plans against. A `RecordedObservation`
holds all three, and `sst plan --use-cached-state` plans from it without reading the target
again, so the plan it produces says what the target held when the record was taken.
`RecordedObservation.from_dict` reads back what `as_dict` wrote.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    GrantRow,
    ObservedArtifact,
    OwnershipMarker,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.state import State, StoredDocumentError

RECORDED_OBSERVATION_SCHEMA_VERSION = 1

_Scope = tuple[str, str]


@dataclass(frozen=True, slots=True)
class RecordedObservation:
    """One target as a plan read it: what it observed, the preflight facts, and the state.

    Attributes:
        target: The live target the plan read, with the account and role the session reported.
        observation: Every object the plan observed; its `fetched_at` is when.
        state: The authoritative state the plan read before it observed.
        preflight: The preflight facts; None when the plan made no preflight reads.
        declared_account: The profile's `account` as written when the record was taken, which
            a target resolved without a connection can be compared with; `target` holds the
            locator the session reported instead.
    """

    target: TargetIdentity
    observation: SnowflakeObservation
    state: State
    preflight: Preflight | None = None
    declared_account: str = ""

    @property
    def fetched_at(self) -> str:
        """When the observation was taken, as the plan's clock reported it."""
        return self.observation.fetched_at

    def foreign_to(self, target: TargetIdentity) -> str | None:
        """Describe the target the record was taken of when it is not this one; None when it is.

        `target` is resolved without a connection, so its account is the profile's as written,
        compared with `declared_account`; its name, database, and schema with the record's.
        The role is left out, because only a session reports it.
        """
        recorded = self.target
        if self.declared_account.upper() != target.account_locator.upper():
            return f"target {recorded.name} on account {self.declared_account or '(unset)'}"
        if (recorded.name, recorded.database.folded, recorded.schema.folded) != (
            target.name,
            target.database.folded,
            target.schema.folded,
        ):
            return f"target {recorded.name} for {recorded.scope.sql}"
        return None

    def as_dict(self) -> dict[str, object]:
        """Return the record as the document stores it, which `from_dict` reads back."""
        return {
            "schema_version": RECORDED_OBSERVATION_SCHEMA_VERSION,
            "target": self.target.as_dict(),
            "declared_account": self.declared_account,
            "fetched_at": self.observation.fetched_at,
            "artifacts": [_artifact_to_json(item) for _, item in sorted(self.observation.artifacts.items())],
            "preflight": _preflight_to_json(self.preflight) if self.preflight is not None else None,
            "state": self.state.as_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> RecordedObservation:
        """Read a record `as_dict` wrote.

        Raises:
            StoredDocumentError: SST-PRT009 when the document is not an object of this schema.
            ValueError: an artifact, the preflight, or the state is malformed.
            KeyError: an artifact lacks a required key.
            TypeError: a value has the wrong type to convert.
        """
        if not isinstance(value, dict):
            raise StoredDocumentError("SST-PRT009", "observation must be an object", detail="not an object")
        version = value.get("schema_version")
        if version != RECORDED_OBSERVATION_SCHEMA_VERSION:
            raise StoredDocumentError(
                "SST-PRT009", f"observation schema {version} is not supported", detail=f"schema {version}"
            )
        artifacts = [_artifact_from_json(item) for item in _list(value.get("artifacts"), "artifacts")]
        raw_preflight = value.get("preflight")
        return cls(
            target=TargetIdentity.from_dict(value.get("target")),
            observation=SnowflakeObservation(
                MappingProxyType({item.key: item for item in artifacts}), str(value.get("fetched_at") or "")
            ),
            state=State.from_dict(value.get("state")),
            preflight=_preflight_from_json(_object(raw_preflight, "preflight")) if raw_preflight is not None else None,
            declared_account=str(value.get("declared_account") or ""),
        )


def _artifact_to_json(item: ObservedArtifact) -> dict[str, object]:
    return {
        "key": item.key,
        "raw_name": item.raw_name,
        "qualified_name": item.qualified_name.sql,
        "object_type": item.object_type,
        "owner": item.owner,
        "created_on": item.created_on,
        "comment": item.comment,
        "marker": [item.marker.manifest_id, item.marker.fingerprint] if item.marker is not None else None,
        "grants": [_grant_to_json(grant) for grant in item.grants] if item.grants is not None else None,
        "definition": item.definition,
        "has_live_version": item.has_live_version,
        "routine_signature": list(item.routine_signature),
        "aliases": list(item.aliases),
        "tags": list(item.tags),
    }


def _artifact_from_json(value: object) -> ObservedArtifact:
    item = _object(value, "artifact")
    marker = item.get("marker")
    grants = item.get("grants")
    return ObservedArtifact(
        key=str(item["key"]),
        raw_name=str(item["raw_name"]),
        qualified_name=QualifiedName.parse(str(item["qualified_name"])),
        object_type=str(item["object_type"]),
        owner=str(item["owner"]),
        created_on=str(item["created_on"]),
        comment=_text(item.get("comment")),
        marker=OwnershipMarker(*(str(part) for part in _list(marker, "marker"))) if marker is not None else None,
        grants=tuple(_grant_from_json(grant) for grant in _list(grants, "grants")) if grants is not None else None,
        definition=_text(item.get("definition")),
        has_live_version=bool(item.get("has_live_version", False)),
        routine_signature=_texts(item.get("routine_signature")),
        aliases=_texts(item.get("aliases")),
        tags=_texts(item.get("tags")),
    )


def _grant_to_json(grant: GrantRow) -> dict[str, object]:
    return {
        "privilege": grant.privilege,
        "granted_to": grant.granted_to,
        "grantee_name": grant.grantee_name,
        "granted_by": grant.granted_by,
        "grant_option": grant.grant_option,
    }


def _grant_from_json(value: object) -> GrantRow:
    grant = _object(value, "grant")
    return GrantRow(
        str(grant["privilege"]),
        str(grant["granted_to"]),
        str(grant["grantee_name"]),
        str(grant.get("granted_by", "")),
        bool(grant.get("grant_option", False)),
    )


def _preflight_to_json(preflight: Preflight) -> dict[str, object]:
    return {
        "target_name": preflight.target_name,
        "role": preflight.role,
        "missing_databases": sorted(preflight.missing_databases),
        "missing_schemas": [list(scope) for scope in sorted(preflight.missing_schemas)],
        "missing_relations": _names_to_json(preflight.missing_relations),
        "missing_privileges": [
            [database, schema, list(privileges)]
            for (database, schema), privileges in sorted(preflight.missing_privileges.items())
        ],
        "occupied": sorted(preflight.occupied),
        "locked": [list(name) for name in sorted(preflight.locked)],
        "referenced": _names_to_json(preflight.referenced),
        "warehouse": preflight.warehouse,
        "warehouse_usable": preflight.warehouse_usable,
    }


def _preflight_from_json(value: Mapping[str, Any]) -> Preflight:
    privileges: dict[_Scope, tuple[str, ...]] = {}
    for entry in _list(value.get("missing_privileges"), "missing_privileges"):
        database, schema, held = _list(entry, "missing privilege")
        privileges[(str(database), str(schema))] = _texts(held)
    return Preflight(
        target_name=str(value["target_name"]),
        role=str(value["role"]),
        missing_databases=frozenset(_texts(value.get("missing_databases"))),
        missing_schemas=frozenset(_pair(scope) for scope in _list(value.get("missing_schemas"), "missing_schemas")),
        missing_relations=_names_from_json(value.get("missing_relations"), "missing_relations"),
        missing_privileges=MappingProxyType(privileges),
        occupied=frozenset(_texts(value.get("occupied"))),
        locked=frozenset(_triple(name) for name in _list(value.get("locked"), "locked")),
        referenced=_names_from_json(value.get("referenced"), "referenced"),
        warehouse=_text(value.get("warehouse")),
        warehouse_usable=bool(value.get("warehouse_usable", True)),
    )


def _names_to_json(names: Mapping[str, tuple[QualifiedName, ...]]) -> dict[str, list[str]]:
    return {key: [name.sql for name in found] for key, found in sorted(names.items())}


def _names_from_json(value: object, field: str) -> Mapping[str, tuple[QualifiedName, ...]]:
    return MappingProxyType(
        {
            str(key): tuple(QualifiedName.parse(str(name)) for name in _list(found, field))
            for key, found in _object(value, field).items()
        }
    )


def _object(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"observation {field} must be an object")
    return {str(key): item for key, item in value.items()}


def _list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"observation {field} must be a list")
    return value


def _pair(value: object) -> _Scope:
    database, schema = _list(value, "scope")
    return str(database), str(schema)


def _triple(value: object) -> tuple[str, str, str]:
    database, schema, name = _list(value, "name")
    return str(database), str(schema), str(name)


def _texts(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in _list(value if value is not None else [], "list"))


def _text(value: object) -> str | None:
    return None if value is None else str(value)
