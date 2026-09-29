"""Immutable lifecycle values shared by plan, apply, and adapters."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from enum import Enum
from hashlib import sha256
from types import MappingProxyType
from typing import Mapping, TypeAlias

from .diagnostic import DiagnosticBag
from .identifier import Identifier, QualifiedName, TargetIdentity
from .registry import GrantPreservation

ArtifactKey: TypeAlias = str
_MARKER = re.compile(r"\[sst:([0-9a-f]{64}):([0-9a-f]{64})\]", re.IGNORECASE)


@dataclass(frozen=True, slots=True, order=True)
class OwnershipMarker:
    manifest_id: str
    fingerprint: str

    def __post_init__(self) -> None:
        for field_name, value in (("manifest_id", self.manifest_id), ("fingerprint", self.fingerprint)):
            if not re.fullmatch(r"[0-9a-f]{64}", value, re.IGNORECASE):
                raise ValueError(f"{field_name} must be a 64-character hexadecimal digest")
        object.__setattr__(self, "manifest_id", self.manifest_id.lower())
        object.__setattr__(self, "fingerprint", self.fingerprint.lower())

    @property
    def text(self) -> str:
        return f"[sst:{self.manifest_id.lower()}:{self.fingerprint.lower()}]"


def extract_marker(comment: str | None) -> OwnershipMarker | None:
    if comment is None:
        return None
    matches = tuple(_MARKER.finditer(comment))
    if not matches:
        return None
    match = matches[-1]
    return OwnershipMarker(match.group(1).lower(), match.group(2).lower())


@dataclass(frozen=True, slots=True, order=True)
class GrantRow:
    privilege: str
    granted_to: str
    grantee_name: str
    granted_by: str = ""
    grant_option: bool = False

    @property
    def is_explicit(self) -> bool:
        return self.granted_to.upper() in {"ROLE", "DATABASE_ROLE"} and self.privilege.upper() != "OWNERSHIP"

    @property
    def identity(self) -> tuple[str, str, str, bool]:
        return self.privilege.upper(), self.granted_to.upper(), self.grantee_name.upper(), self.grant_option


@dataclass(frozen=True, slots=True)
class ShowRow:
    name: str
    database_name: str
    schema_name: str
    owner: str
    created_on: str
    comment: str | None = None
    object_type: str = "SEMANTIC VIEW"

    @property
    def qualified_name(self) -> QualifiedName:
        return QualifiedName(
            Identifier.shown(self.database_name), Identifier.shown(self.schema_name), Identifier.shown(self.name)
        )


@dataclass(frozen=True, slots=True)
class ObservedArtifact:
    key: ArtifactKey
    raw_name: str
    qualified_name: QualifiedName
    object_type: str
    owner: str
    created_on: str
    comment: str | None
    marker: OwnershipMarker | None
    grants: tuple[GrantRow, ...] | None = None
    definition: str | None = None
    has_live_version: bool = False
    routine_signature: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()

    @property
    def explicit_grants(self) -> tuple[GrantRow, ...]:
        return tuple(grant for grant in (self.grants or ()) if grant.is_explicit)


@dataclass(frozen=True, slots=True)
class SnowflakeObservation:
    artifacts: Mapping[ArtifactKey, ObservedArtifact] = field(default_factory=lambda: MappingProxyType({}))
    fetched_at: str = ""


@dataclass(frozen=True, slots=True, order=True)
class PhysicalResource:
    object_type: str
    qualified_name: QualifiedName
    exists: bool


@dataclass(frozen=True, slots=True)
class CompositeObservation:
    key: ArtifactKey
    resources: tuple[PhysicalResource, ...] = ()
    stage_exists: bool = False
    stage_file_format: str | None = None
    config_path: str | None = None
    config_exists: bool = False
    diagnostics: DiagnosticBag = DiagnosticBag()
    config_size: int | None = None
    config_md5: str | None = None
    # Handler-specific observed facts. Each composite handler records what it
    # observed here, so a saved plan goes stale when any of them changes.
    details: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class CompositePlan:
    action: Action
    reason: ChangeReason
    observation: CompositeObservation
    diagnostics: DiagnosticBag = DiagnosticBag()


class ProbeKind(Enum):
    VIEW = "view"
    METRIC = "metric"
    VERIFIED_QUERY = "verified_query"
    DESCRIBE = "describe"


@dataclass(frozen=True, slots=True)
class SmokeProbe:
    key: str
    kind: ProbeKind
    sql: str


@dataclass(frozen=True, slots=True)
class RenderedArtifact:
    key: ArtifactKey
    artifact_type: str
    target: QualifiedName
    ddl: str
    statements: tuple[str, ...]
    fingerprint: str
    object_type: str = "SEMANTIC VIEW"
    render_dialect: str = "ddl"
    grant_preservation: GrantPreservation = GrantPreservation.CLAUSE
    upload_path: str | None = None
    upload_content: bytes | None = None
    temporary: bool = False
    routine_signature: tuple[str, ...] = ()
    create_statements: tuple[str, ...] = ()
    update_statements: tuple[str, ...] = ()
    update_live_statements: tuple[str, ...] = ()
    expected_marker: OwnershipMarker | None = None
    desired_alias: str | None = None
    desired_tags: tuple[str, ...] = ()
    depends_on: tuple[ArtifactKey, ...] = ()
    smoke: tuple[SmokeProbe, ...] = ()
    required_relations: tuple[QualifiedName, ...] = ()
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, QualifiedName], ...] = ()
    generic_apply_safe: bool = True

    @classmethod
    def create(
        cls,
        *,
        key: ArtifactKey,
        artifact_type: str,
        target: QualifiedName,
        ddl: str,
        object_type: str = "SEMANTIC VIEW",
        render_dialect: str = "ddl",
        grant_preservation: GrantPreservation = GrantPreservation.CLAUSE,
        upload_path: str | None = None,
        upload_content: bytes | None = None,
        temporary: bool = False,
        routine_signature: tuple[str, ...] = (),
        create_statements: tuple[str, ...] = (),
        update_statements: tuple[str, ...] = (),
        update_live_statements: tuple[str, ...] = (),
        expected_marker: OwnershipMarker | None = None,
        desired_alias: str | None = None,
        desired_tags: tuple[str, ...] = (),
        statements: tuple[str, ...] | None = None,
        depends_on: tuple[ArtifactKey, ...] = (),
        smoke: tuple[SmokeProbe, ...] = (),
        required_relations: tuple[QualifiedName, ...] = (),
        component_fingerprints: tuple[tuple[str, str], ...] = (),
        physical_resources: tuple[tuple[str, QualifiedName], ...] = (),
        generic_apply_safe: bool = True,
    ) -> RenderedArtifact:
        canonical = "\n".join(line.rstrip() for line in ddl.splitlines()).strip() + "\n"
        return cls(
            key=key,
            artifact_type=artifact_type,
            target=target,
            ddl=canonical,
            statements=(canonical.rstrip("\n"),) if statements is None else statements,
            fingerprint=sha256(canonical.encode("utf-8")).hexdigest(),
            object_type=object_type,
            render_dialect=render_dialect,
            grant_preservation=grant_preservation,
            upload_path=upload_path,
            upload_content=upload_content,
            temporary=temporary,
            routine_signature=routine_signature,
            create_statements=create_statements,
            update_statements=update_statements,
            update_live_statements=update_live_statements,
            expected_marker=expected_marker,
            desired_alias=desired_alias,
            desired_tags=desired_tags,
            depends_on=depends_on,
            smoke=smoke,
            required_relations=required_relations,
            component_fingerprints=component_fingerprints,
            physical_resources=physical_resources,
            generic_apply_safe=generic_apply_safe,
        )

    @property
    def content(self) -> str:
        return self.ddl

    def for_action(self, action: Action, observed: ObservedArtifact | None) -> RenderedArtifact:
        if action is Action.CREATE and self.create_statements:
            return replace(self, statements=self.create_statements)
        if action is Action.UPDATE:
            if observed is not None and observed.has_live_version and self.update_live_statements:
                statements = self.update_live_statements
            elif self.update_statements:
                statements = self.update_statements
            else:
                return self
            return replace(self, statements=(*statements, *_metadata_removals(self, observed)))
        return self


def _metadata_removals(
    artifact: RenderedArtifact,
    observed: ObservedArtifact | None,
) -> tuple[str, ...]:
    if observed is None or artifact.object_type != "AGENT":
        return ()
    statements: list[str] = []
    desired_alias = artifact.desired_alias.casefold() if artifact.desired_alias else None
    for alias in observed.aliases:
        if alias.casefold() == desired_alias:
            continue
        statements.append(f"ALTER AGENT {artifact.target.sql} MODIFY VERSION {_safe_identifier(alias)} UNSET ALIAS")
    stale_tags = tuple(
        tag for tag in observed.tags if tag.casefold() not in {value.casefold() for value in artifact.desired_tags}
    )
    if stale_tags:
        statements.append(
            f"ALTER AGENT {artifact.target.sql} UNSET TAG "
            + ", ".join(_safe_qualified_identifier(tag) for tag in stale_tags)
        )
    return tuple(statements)


def _safe_identifier(value: str) -> str:
    try:
        parsed = Identifier.parse(value)
    except ValueError:
        return Identifier(value, quoted=True).sql
    return parsed.sql if parsed.folded == value else Identifier(value, quoted=True).sql


def _safe_qualified_identifier(value: str) -> str:
    try:
        return QualifiedName.parse(value).sql
    except ValueError:
        return ".".join(_safe_identifier(part) for part in value.split("."))


class Action(Enum):
    CREATE = "create"
    UPDATE = "update"
    NOOP = "noop"
    PRUNE = "prune"
    BLOCKED = "blocked"


class ChangeReason(Enum):
    NOT_PRESENT = "not_present"
    FINGERPRINT_DIFFERS = "fingerprint_differs"
    NO_PRIOR_STATE = "no_prior_state"
    STATE_MANIFEST_MISMATCH = "state_manifest_mismatch"
    UNCHANGED = "unchanged"
    ORPHANED = "orphaned"
    POISONED_REFS = "poisoned_refs"
    VALIDATION_ERRORS = "validation_errors"
    DEPENDENCY_BLOCKED = "dependency_blocked"
    UNMANAGED_OBJECT = "unmanaged_object"
    TARGET_MOVED = "target_moved"


@dataclass(frozen=True, slots=True)
class Change:
    key: ArtifactKey
    artifact_type: str
    action: Action
    reason: ChangeReason
    rendered: RenderedArtifact | None
    observed: ObservedArtifact | None
    depends_on: tuple[ArtifactKey, ...]
    order: int
    diagnostics: DiagnosticBag = DiagnosticBag()
    composite_observation: CompositeObservation | None = None
    prune_executable: bool = True


@dataclass(frozen=True, slots=True)
class ChangeSet:
    manifest_id: str
    target: TargetIdentity
    changes: tuple[Change, ...]
    diagnostics: DiagnosticBag
    observation_at: str
    full: bool = True
    plan_id: str = ""

    @property
    def writes(self) -> tuple[Change, ...]:
        """What apply would execute. A report-only prune is listed, never executed."""
        return tuple(
            change
            for change in self.changes
            if change.action in (Action.CREATE, Action.UPDATE)
            or (change.action is Action.PRUNE and change.prune_executable)
        )

    @property
    def report_only(self) -> tuple[Change, ...]:
        return tuple(change for change in self.changes if change.action is Action.PRUNE and not change.prune_executable)

    @property
    def blocked(self) -> tuple[Change, ...]:
        return tuple(change for change in self.changes if change.action is Action.BLOCKED)


class FailurePolicy(Enum):
    STOP_ALL = "stop_all"
    STOP_DEPENDENTS = "stop_dependents"
    CONTINUE = "continue"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_ms: tuple[int, ...] = (1000, 2000)

    def delay_after(self, attempt: int) -> int:
        if attempt < 1 or attempt >= self.max_attempts:
            raise ValueError("retry delay requested outside retryable attempts")
        return self.backoff_ms[min(attempt - 1, len(self.backoff_ms) - 1)]


@dataclass(frozen=True, slots=True)
class ApplyOptions:
    parallelism: int = 4
    on_failure: FailurePolicy = FailurePolicy.STOP_DEPENDENTS
    retry: RetryPolicy = RetryPolicy()
    allow_prune: bool = False
    break_stale_lock: bool = False


class GrantCheck(Enum):
    NOT_APPLICABLE = "not_applicable"
    PRESERVED = "preserved"
    UNREADABLE = "unreadable"


class OutcomeStatus(Enum):
    APPLIED = "applied"
    SKIPPED = "skipped"
    FAILED = "failed"


class ErrorKind(Enum):
    PRIVILEGE = "privilege"
    NOT_FOUND = "not_found"
    SYNTAX = "syntax"
    TRANSIENT = "transient"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ClassifiedError:
    code: str
    message: str
    kind: ErrorKind
    retryable: bool = False
    sqlstate: str | None = None


@dataclass(frozen=True, slots=True)
class ApplyOutcome:
    key: ArtifactKey
    action: Action
    status: OutcomeStatus
    attempts: int
    duration_ms: int
    ddl: str
    error: ClassifiedError | None = None
    grants: GrantCheck = GrantCheck.NOT_APPLICABLE
    write_succeeded: bool = False
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class ApplyResult:
    outcomes: tuple[ApplyOutcome, ...]
    diagnostics: DiagnosticBag
    run_id: str
    started_at: str
    finished_at: str
    state_written: bool

    @property
    def success(self) -> bool:
        return not self.diagnostics.has_errors and all(
            outcome.status is not OutcomeStatus.FAILED for outcome in self.outcomes
        )


@dataclass(frozen=True, slots=True)
class QueryResult:
    columns: tuple[str, ...] = ()
    rows: tuple[tuple[object, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class ExecutionError:
    message: str
    sqlstate: str | None = None
    errno: int | None = None


@dataclass(frozen=True, slots=True)
class ExecResult:
    ok: bool
    query_ids: tuple[str, ...] = ()
    error: ExecutionError | None = None
    rows_affected: int | None = None
