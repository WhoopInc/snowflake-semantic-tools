"""The only interface through which application use cases reach Snowflake."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence

from ..model.diagnostic import Diagnostic
from ..model.identifier import QualifiedName, SchemaScope
from ..model.lifecycle import ExecResult, GrantRow, OwnershipMarker, QueryResult, ShowRow
from ..state.model import AppliedEntry, Manifest, SavedPlan, State


class SnowflakePortError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        sqlstate: str | None = None,
        errno: int | None = None,
        diagnostic: Diagnostic | None = None,
    ) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.errno = errno
        # What the command reports, when the adapter recognised the failure.
        self.diagnostic = diagnostic


@dataclass(frozen=True, slots=True)
class StageObservation:
    exists: bool
    file_format: str | None = None


@dataclass(frozen=True, slots=True)
class StagedFileMetadata:
    stage_path: str
    name: str
    size: int
    md5: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class ExtensionObservation:
    """One `SHOW CORTEX EXTENSIONS` row."""

    qualified_name: QualifiedName
    extension_type: str
    comment: str | None
    owner: str
    effective_version: str | None = None
    latest_certified_version: str | None = None


@dataclass(frozen=True, slots=True)
class ExtensionVersion:
    """One `SHOW VERSIONS IN CORTEX EXTENSION` row, addressed by its system name."""

    name: str
    alias: str | None
    location: str
    is_default: bool = False
    certification_status: str | None = None


class SnowflakePort(Protocol):
    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]: ...

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]: ...

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None: ...

    def query(self, sql: str, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult: ...

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: str,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult: ...

    def execute_script(self, statements: Sequence[str]) -> ExecResult: ...

    def try_execute(self, sql: str) -> ExecResult: ...

    def current_role(self) -> str: ...

    def current_account_locator(self) -> str: ...

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool: ...

    def dataset_exists(self, qualified_name: QualifiedName) -> bool: ...

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation: ...

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None: ...

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None: ...

    def read_staged_file(self, stage_path: str) -> bytes | None: ...

    def stage_file_exists(self, stage_path: str) -> bool: ...

    def upload(self, stage_path: str, content: bytes) -> None: ...

    def stage_type(self, qualified_name: QualifiedName) -> str | None: ...

    def list_location(self, location: str) -> tuple[str, ...]: ...

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None: ...

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]: ...

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None: ...

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None: ...

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None: ...

    def merge_profile_row(
        self,
        registry: QualifiedName,
        row: Mapping[str, object],
        *,
        expected_version: str | None,
    ) -> int: ...

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int: ...

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]: ...

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool: ...

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str: ...

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None: ...

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        applied: Mapping[str, AppliedEntry],
    ) -> None: ...

    def ensure_state_table(self, state_table: QualifiedName) -> None: ...

    def delete_state(self, state_table: QualifiedName, target_name: str, artifact_key: str) -> int: ...

    def upsert_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        artifact_key: str,
        entry: AppliedEntry,
    ) -> int: ...


class ClockPort(Protocol):
    def now_iso(self) -> str: ...

    def monotonic_ms(self) -> int: ...

    def sleep(self, milliseconds: int) -> None: ...

    def new_run_id(self) -> str: ...


class StateStore(Protocol):
    @property
    def config_path(self) -> str: ...

    def read_local(self) -> State | None: ...

    def write_local(self, value: State) -> None: ...

    def acquire_lock(self, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]: ...

    def release_lock(self, run_id: str) -> None: ...


class ManifestStore(Protocol):
    def read(self) -> Manifest | None: ...

    def write(self, value: Manifest) -> None: ...


class PlanStore(Protocol):
    def read(self) -> SavedPlan | None: ...

    def write(self, value: SavedPlan) -> None: ...
