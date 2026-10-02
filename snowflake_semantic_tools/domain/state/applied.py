"""What apply records per target: the state document, and the entry each published artifact leaves.

`State` is the local `state.<target>.json` document; the Snowflake state table holds the same
`AppliedEntry` values, one row per artifact. An entry is what plan trusts: an object whose
ownership marker matches its entry is SST's to update or prune. `State.from_dict` migrates a
schema-1 document in memory and refuses a newer one.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import cast

from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.state.codec import pairs_from_json, pairs_to_json
from snowflake_semantic_tools.domain.state.documents import StoredDocumentError

STATE_SCHEMA_VERSION = 2


class ResourceStatus(Enum):
    """What apply knows of a physical resource an entry records; the value is persisted.

    VERIFIED was read back after the write, RETAINED is kept from an earlier version that no
    longer uses it, and UNVERIFIED_AFTER_WRITE was written but not read back.
    """

    VERIFIED = "verified"
    RETAINED = "retained"
    UNVERIFIED_AFTER_WRITE = "unverified_after_write"


@dataclass(frozen=True, slots=True, order=True)
class AppliedResource:
    """One Snowflake object an applied artifact spans, and what apply knows of it.

    `object_type` is stored uppercase. Iterating yields `(object_type, qualified_name)`, so a
    resource unpacks like the pairs a rendered artifact lists.
    """

    object_type: str
    qualified_name: str
    status: ResourceStatus = ResourceStatus.VERIFIED

    def __post_init__(self) -> None:
        object.__setattr__(self, "object_type", self.object_type.upper())

    def as_dict(self) -> dict[str, str]:
        """Return the resource as an entry stores it under `physical_resources`."""
        return {
            "object_type": self.object_type,
            "qualified_name": self.qualified_name,
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, value: object) -> AppliedResource:
        """Read a resource as `as_dict` writes it; one without a `status` is VERIFIED.

        Raises:
            ValueError: the resource is not an object, or its status is not a `ResourceStatus`.
        """
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


# What an entry accepts as a resource: an `AppliedResource`, or an `(object_type,
# qualified_name)` pair, VERIFIED, or a triple that adds the status or its value.
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


# The outcome of a publish whose statements and checks all succeeded.
APPLIED = "applied"
# An executed prune that deactivates rather than drops leaves this outcome: the
# object is still SST's, so the entry is kept as a tombstone it can reactivate.
DEACTIVATED = "deactivated"
# A publish that wrote something and then failed; the next plan retries it.
FAILED_AFTER_WRITE = "failed_after_write"
# A publish whose statements stopped part way: some ran and a later one failed, so what is
# live is not what the entry's fingerprint describes, and the next plan updates it.
PARTIAL_WRITE = "partial_write"


@dataclass(frozen=True, slots=True)
class AppliedEntry:
    """What apply recorded for one published artifact; plan trusts the object it names.

    Component fingerprints are stored sorted, and resources given as pairs or triples are
    stored as `AppliedResource`s.

    Attributes:
        fingerprint: The fingerprint apply published, which the object's marker must repeat.
        qualified_name: The object the artifact was published to.
        applied_at: When apply recorded the entry, as the clock reported it.
        outcome: How the entry came to be, such as APPLIED or FAILED_AFTER_WRITE.
        ddl_sha256: The fingerprint as the publish recorded it; a prune requires the object's
            marker to repeat it.
        manifest_id: The manifest the artifact was published from, which the marker must repeat.
        git_sha: The commit apply ran from; empty when it is unknown.
        component_fingerprints: A composite artifact's published parts.
        physical_resources: The objects the artifact spans, with what apply knows of each.
    """

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
        """Return `physical_resources` as the `AppliedResource`s the entry stored them as."""
        return cast(tuple[AppliedResource, ...], self.physical_resources)

    def as_dict(self) -> dict[str, object]:
        """Return the entry as the state document stores it under `applied`."""
        return {
            "fingerprint": self.fingerprint,
            "qualified_name": self.qualified_name,
            "applied_at": self.applied_at,
            "run_id": self.run_id,
            "outcome": self.outcome,
            "ddl_sha256": self.ddl_sha256,
            "manifest_id": self.manifest_id,
            "git_sha": self.git_sha,
            "component_fingerprints": pairs_to_json(self.component_fingerprints),
            "physical_resources": [resource.as_dict() for resource in self.applied_resources],
        }

    @classmethod
    def from_dict(cls, value: object) -> AppliedEntry:
        """Read an entry as `as_dict` writes it; a key it lacks reads as empty.

        Raises:
            ValueError: the entry, its component metadata, or a resource is malformed.
        """
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
            component_fingerprints=pairs_from_json(components),
            physical_resources=parsed_resources,
        )


@dataclass(frozen=True, slots=True)
class LastRun:
    """The run that last wrote a state document.

    Attributes:
        started_at, finished_at: When the run started and finished, as the clock reported it.
        outcome: `ok`, or `partial` when a change failed.
        actor: Who ran it; empty when it is unknown.
    """

    run_id: str
    started_at: str
    finished_at: str
    sst_version: str
    command: str
    outcome: str
    actor: str = ""

    def as_dict(self) -> dict[str, str]:
        """Return the run as the state document stores it under `last_run`."""
        return {name: str(getattr(self, name)) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class State:
    """What apply recorded for one target: the manifest it last applied, and one entry per artifact.

    `as_dict` always writes `observed` and `lock` as null.

    Attributes:
        manifest_id: The manifest the last run applied; empty until a run records one.
        config_path: The project configuration the state belongs to.
        last_run: The run that wrote this state; None until one has.
    """

    schema_version: int
    target: TargetIdentity
    manifest_id: str
    config_path: str
    last_run: LastRun | None
    applied: Mapping[str, AppliedEntry]

    @classmethod
    def empty(cls, target: TargetIdentity, config_path: str = "sst_config.yml") -> State:
        """Return the state of a target nothing has been applied to."""
        return cls(STATE_SCHEMA_VERSION, target, "", config_path, None, MappingProxyType({}))

    def as_dict(self) -> dict[str, object]:
        """Return the state document, which `from_dict` reads back."""
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
        """Read a stored state document, migrating a schema-1 one in memory.

        Raises:
            StoredDocumentError: SST-MAN022 when the document is not an object, and SST-MAN023
                when its schema is not an integer, is newer, or has no migration.
            ValueError: `applied` or one of its entries is malformed.
        """
        if not isinstance(value, dict):
            raise StoredDocumentError("SST-MAN022", "state must be an object", detail="the document is not an object")
        version = value.get("schema_version")
        if not isinstance(version, int) or version > STATE_SCHEMA_VERSION:
            raise StoredDocumentError("SST-MAN023", f"state schema {version} is not supported", found=version)
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
    """Migrate a schema-1 state document to the current schema, leaving the input unchanged.

    Each entry gains empty component fingerprints and, when it names an object, that object
    as its one VERIFIED resource, with an unknown object type.

    Raises:
        StoredDocumentError: SST-MAN023 when the document is not schema 1.
        ValueError: `applied` or one of its entries is not an object.
    """
    version = value.get("schema_version")
    if version != 1:
        raise StoredDocumentError("SST-MAN023", f"state schema {version} has no migration", found=version)
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


def written_by_newer(writer_version: str, current_version: str) -> bool:
    """Report whether a document's writer is a later SST release than `current_version`.

    Releases compare by their leading dotted numbers, so a pre-release or development suffix
    never makes a writer newer, and a version with no leading number is never newer.
    """
    return _release(writer_version) > _release(current_version)


def _release(version: str) -> tuple[int, ...]:
    """The leading dotted numbers of a version, such as (1, 0, 2) for `1.0.2.dev3`."""
    numbers: list[int] = []
    for part in version.split("."):
        digits = ""
        for character in part:
            if not character.isdigit():
                break
            digits += character
        if not digits:
            break
        numbers.append(int(digits))
        if len(digits) != len(part):
            break
    return tuple(numbers)
