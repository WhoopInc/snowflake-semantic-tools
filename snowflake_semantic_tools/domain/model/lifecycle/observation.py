"""What SST reads back from Snowflake: the port's transport values and the observations built on them.

`ShowRow` and `GrantRow` are what SHOW commands return; `QueryResult` and `ExecResult` are
what a query or a script returns. Plan assembles SHOW rows into a `SnowflakeObservation`,
one `ObservedArtifact` per object SST could own, and a composite lifecycle handler reports
what it found as a `CompositeObservation`. None of these values reads Snowflake itself.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TypeAlias

from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle.marker import OwnershipMarker

# The `<kind>:<name>` key every lifecycle value is indexed by; see `artifact_key`.
ArtifactKey: TypeAlias = str


@dataclass(frozen=True, slots=True, order=True)
class GrantRow:
    """One row of SHOW GRANTS ON an object.

    Attributes:
        granted_to: The grantee's kind as Snowflake shows it, such as ROLE or DATABASE_ROLE.
        granted_by: The role that made the grant; empty when the row does not say.
    """

    privilege: str
    granted_to: str
    grantee_name: str
    granted_by: str = ""
    grant_option: bool = False

    @property
    def is_explicit(self) -> bool:
        """Report whether apply must keep the grant across a replace: a role's privilege, not OWNERSHIP."""
        return self.granted_to.upper() in {"ROLE", "DATABASE_ROLE"} and self.privilege.upper() != "OWNERSHIP"

    @property
    def identity(self) -> tuple[str, str, str, bool]:
        """Return what identifies the grant when the grants before and after a write are compared."""
        return self.privilege.upper(), self.granted_to.upper(), self.grantee_name.upper(), self.grant_option


@dataclass(frozen=True, slots=True)
class ShowRow:
    """One object a SHOW command lists, with its names exactly as Snowflake shows them.

    Attributes:
        comment: The object's comment, where an ownership marker lives; None when it has none.
        object_type: The type SHOW listed the object under, such as SEMANTIC VIEW or AGENT.
    """

    name: str
    database_name: str
    schema_name: str
    owner: str
    created_on: str
    comment: str | None = None
    object_type: str = "SEMANTIC VIEW"

    @property
    def qualified_name(self) -> QualifiedName:
        """Name the object from its shown parts: each unquoted when it can be, else quoted exactly."""
        return QualifiedName(
            Identifier.shown(self.database_name), Identifier.shown(self.schema_name), Identifier.shown(self.name)
        )


@dataclass(frozen=True, slots=True)
class ObservedArtifact:
    """One object in Snowflake that an artifact key names, as plan observed it.

    Attributes:
        raw_name: The object's name exactly as SHOW printed it, to report a case mismatch.
        comment: The object's comment; None when it has none.
        marker: The ownership marker in the comment; None when the comment carries none.
        grants: Every grant on the object; None when plan did not read them, because the
            object is not one this plan may replace or the read failed.
        definition: The object's definition text; None when it was not read.
        has_live_version: The agent has a live version, which an update must commit first.
        routine_signature: The argument types of an observed procedure or function.
        aliases: The version aliases the agent shows; an update unsets all but the desired one.
        tags: The tags the agent shows; an update unsets those not desired.
    """

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
        """Return the grants a replace must keep; empty when the grants were not read."""
        return tuple(grant for grant in (self.grants or ()) if grant.is_explicit)


@dataclass(frozen=True, slots=True)
class SnowflakeObservation:
    """Every object plan observed in the target, keyed by the artifact key each would carry.

    Attributes:
        fetched_at: When the observation was taken, as the clock reported it; a saved plan
            records it, and the plan id covers it.
    """

    artifacts: Mapping[ArtifactKey, ObservedArtifact] = field(default_factory=lambda: MappingProxyType({}))
    fetched_at: str = ""


@dataclass(frozen=True, slots=True, order=True)
class PhysicalResource:
    """One Snowflake object a composite artifact spans, and whether it exists."""

    object_type: str
    qualified_name: QualifiedName
    exists: bool


@dataclass(frozen=True, slots=True)
class CompositeObservation:
    """What a composite lifecycle handler observed for one artifact.

    A saved plan hashes every field except `key` and `diagnostics` into the change's previous
    marker, so the plan goes stale when any observed fact changes.

    Attributes:
        stage_file_format: The file format the handler's stage declares; None when the stage
            is absent or declares none.
        config_path: The staged configuration file the handler reads; None when it has none.
        diagnostics: What the handler found wrong while observing.
        config_size: The staged configuration file's size in bytes; None when it is absent.
        config_md5: The staged configuration file's MD5 hex digest; None when it is absent.
        details: Handler-specific observed facts as `(name, value)` pairs. A saved plan hashes
            them only when there are some, so a handler that records none keeps its hash.
    """

    key: ArtifactKey
    resources: tuple[PhysicalResource, ...] = ()
    stage_exists: bool = False
    stage_file_format: str | None = None
    config_path: str | None = None
    config_exists: bool = False
    diagnostics: DiagnosticBag = DiagnosticBag()
    config_size: int | None = None
    config_md5: str | None = None
    details: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class QueryResult:
    """The rows a query returned, with its column names in order."""

    columns: tuple[str, ...] = ()
    rows: tuple[tuple[object, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class ExecutionError:
    """The error that stopped a script, as the driver reported it.

    Attributes:
        sqlstate: The SQLSTATE code; None when the driver gave none.
        errno: Snowflake's error number; None when the driver gave none.
    """

    message: str
    sqlstate: str | None = None
    errno: int | None = None


@dataclass(frozen=True, slots=True)
class ExecResult:
    """What running a script returned: whether every statement succeeded, and what ran.

    Attributes:
        query_ids: One id per statement that ran; on failure, the ids of the statements that
            ran before it, which the caller must treat as a partial write.
        error: What stopped the script; None when it succeeded.
        rows_affected: The rows the statements changed; None when the driver did not report it.
    """

    ok: bool
    query_ids: tuple[str, ...] = ()
    error: ExecutionError | None = None
    rows_affected: int | None = None
