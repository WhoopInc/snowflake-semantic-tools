"""In-memory doubles of the ports the application use cases take: Snowflake, the clock, and the state file.

They are real implementations that record what a use case did, not mocks. `InMemorySnowflake`
answers from the attributes a test sets (`rows`, `markers`, `remote_state`, the `*_error` hooks)
and records every query, script and upload.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import md5
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    ExecResult,
    ExecutionError,
    GrantRow,
    OwnershipMarker,
    QueryResult,
    ShowRow,
)
from snowflake_semantic_tools.domain.ports.snowflake import (
    ExtensionObservation,
    ExtensionVersion,
    StagedFileMetadata,
    StageObservation,
)
from snowflake_semantic_tools.domain.state import AppliedEntry, State


class InMemorySnowflake:
    def __init__(self) -> None:
        self.rows: tuple[ShowRow, ...] = ()
        self.grants: dict[str, tuple[GrantRow, ...]] = {}
        self.markers: dict[str, OwnershipMarker | None] = {}
        self.queries: list[tuple[str, object]] = []
        self.scripts: list[tuple[str, ...]] = []
        self.uploads: list[tuple[str, bytes]] = []
        self.live_agents: set[str] = set()
        self.agent_versions: dict[tuple[str, str], str] = {}
        self.existing: set[str] | None = None
        self.remote_state: Mapping[str, AppliedEntry] | None = MappingProxyType({})
        self.execute_results: list[ExecResult] = []
        self.query_error: Exception | None = None
        self.show_error: Exception | None = None
        self.grant_error: Exception | None = None
        self.marker_error: Exception | None = None
        self.stage_formats: dict[str, str] = {}
        self.stage_files: set[str] = set()
        self.staged_file_sizes: dict[str, int] = {}
        self.staged_file_md5s: dict[str, str | None] = {}
        self.staged_file_contents: dict[str, bytes] = {}
        self.table_row_counts: dict[str, int] = {}

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        del object_type, scope
        if self.show_error:
            raise self.show_error
        return self.rows

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]:
        del object_type, routine_signature
        if self.grant_error:
            raise self.grant_error
        return self.grants.get(qualified_name.sql, ())

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        del object_type
        if self.marker_error:
            raise self.marker_error
        return self.markers.get(qualified_name.sql)

    def query(self, sql: str, params: object = None) -> QueryResult:
        self.queries.append((sql, params))
        if self.query_error:
            raise self.query_error
        prefix = "SELECT COUNT(*) AS ROW_COUNT FROM "
        if sql.upper().startswith(prefix):
            table = sql[len(prefix) :]
            return QueryResult(("ROW_COUNT",), ((self.table_row_counts.get(table, 0),),))
        return QueryResult()

    def query_in_context(self, scope: SchemaScope, sql: str, params: object = None) -> QueryResult:
        del scope
        return self.query(sql, params)

    def execute_script(self, statements: Sequence[str]) -> ExecResult:
        self.scripts.append(tuple(statements))
        if self.execute_results:
            result = self.execute_results.pop(0)
            if not result.ok:
                return result
        for statement in statements:
            normalized = " ".join(statement.split())
            if normalized.upper().startswith("CREATE STAGE IF NOT EXISTS "):
                name = normalized.split()[5]
                marker = "FILE_FORMAT = ("
                index = normalized.upper().find(marker)
                self.stage_formats[name] = normalized[index + len(marker) :].rsplit(")", 1)[0]
                if self.existing is not None:
                    self.existing.add(name)
            elif normalized.upper().startswith("CREATE TABLE ") and self.existing is not None:
                table = normalized.split()[2]
                self.existing.add(table)
                self.table_row_counts.setdefault(table, 0)
            elif normalized.upper().startswith("INSERT INTO "):
                table = normalized.split()[2]
                self.table_row_counts[table] = normalized.upper().count("SELECT")
            elif "SYSTEM$CREATE_EVALUATION_DATASET" in normalized.upper() and self.existing is not None:
                values = normalized.split("'")
                if len(values) >= 6:
                    self.existing.add(values[5])
        return ExecResult(True)

    def try_execute(self, sql: str) -> ExecResult:
        return self.execute_script((sql,))

    def current_role(self) -> str:
        return "TEST_ROLE"

    def current_account_locator(self) -> str:
        return "TEST_ACCOUNT"

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        if object_type.upper() == "STAGE" and qualified_name.sql in self.stage_formats:
            return True
        if self.existing is None:
            return object_type == "TABLE OR VIEW"
        return qualified_name.sql in self.existing

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        return self.stage_formats.get(qualified_name.sql)

    def stage_file_exists(self, stage_path: str) -> bool:
        return stage_path in self.stage_files

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        if stage_path not in self.stage_files:
            return None
        return StagedFileMetadata(
            stage_path,
            stage_path[1:] if stage_path.startswith("@") else stage_path,
            self.staged_file_sizes.get(stage_path, 0),
            self.staged_file_md5s.get(stage_path),
        )

    def upload(self, stage_path: str, content: bytes) -> None:
        self.uploads.append((stage_path, content))
        self.stage_files.add(stage_path)
        self.staged_file_sizes[stage_path] = len(content)
        self.staged_file_md5s[stage_path] = md5(content, usedforsecurity=False).hexdigest()
        self.staged_file_contents[stage_path] = content

    def read_staged_file(self, stage_path: str) -> bytes | None:
        return self.staged_file_contents.get(stage_path)

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        return qualified_name.sql in self.live_agents

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        if selector.upper().startswith("VERSION$"):
            return selector.upper()
        return self.agent_versions.get((qualified_name.sql, selector.casefold()), "VERSION$1")

    # The rest of `SnowflakePort`, which no test of this double reaches: each refuses loudly, so a use
    # case that starts relying on one fails here rather than reading an invented answer. A test that
    # needs one subclasses this double and answers it.
    def dataset_exists(self, qualified_name: QualifiedName) -> bool:
        raise NotImplementedError(f"InMemorySnowflake does not model dataset_exists({qualified_name.sql})")

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        raise NotImplementedError(f"InMemorySnowflake does not model table_columns({qualified_name.sql})")

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation:
        raise NotImplementedError(f"InMemorySnowflake does not model observe_stage({qualified_name.sql})")

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        raise NotImplementedError(f"InMemorySnowflake does not model stage_type({qualified_name.sql})")

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        raise NotImplementedError(f"InMemorySnowflake does not model observe_extension({qualified_name.sql})")

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        raise NotImplementedError(f"InMemorySnowflake does not model extension_versions({qualified_name.sql})")

    def list_location(self, location: str) -> tuple[str, ...]:
        raise NotImplementedError(f"InMemorySnowflake does not model list_location({location})")

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        raise NotImplementedError(f"InMemorySnowflake does not model ensure_profile_registry({qualified_name.sql})")

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        raise NotImplementedError(f"InMemorySnowflake does not model read_profile_row({registry.sql}, {name})")

    def merge_profile_row(
        self, registry: QualifiedName, row: Mapping[str, object], *, expected_version: str | None
    ) -> int:
        raise NotImplementedError(f"InMemorySnowflake does not model merge_profile_row({registry.sql})")

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        raise NotImplementedError(f"InMemorySnowflake does not model deactivate_profile_row({registry.sql}, {name})")

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        raise NotImplementedError(f"InMemorySnowflake does not model desktop_profile_rows({registry.sql})")

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None:
        del state_table, target_name
        return self.remote_state

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        applied: Mapping[str, AppliedEntry],
    ) -> None:
        del state_table, target_name, manifest_id
        self.remote_state = MappingProxyType(dict(applied))

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        del state_table

    def delete_state(self, state_table: QualifiedName, target_name: str, artifact_key: str) -> int:
        del state_table, target_name
        current = dict(self.remote_state or {})
        existed = artifact_key in current
        current.pop(artifact_key, None)
        self.remote_state = MappingProxyType(current)
        return int(existed)

    def upsert_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        artifact_key: str,
        entry: AppliedEntry,
    ) -> int:
        del state_table, target_name
        current = dict(self.remote_state or {})
        current[artifact_key] = entry
        self.remote_state = MappingProxyType(current)
        return 1


class FixedClock:
    def __init__(self) -> None:
        self.current = 0
        self.sleeps: list[int] = []

    def now_iso(self) -> str:
        self.current += 1
        return f"2026-01-01T00:00:0{self.current}Z"

    def monotonic_ms(self) -> int:
        self.current += 1
        return self.current

    def sleep(self, milliseconds: int) -> None:
        self.sleeps.append(milliseconds)

    def new_run_id(self) -> str:
        return "run-1"


class InMemoryStateStore:
    def __init__(self, state: State | None = None) -> None:
        self.state = state
        self.locked = False
        self.holder: str | None = None
        self.stale = False
        self.writes: list[State] = []

    @property
    def config_path(self) -> str:
        return "sst_config.yml"

    def read_local(self) -> State | None:
        return self.state

    def write_local(self, value: State) -> None:
        self.state = value
        self.writes.append(value)

    def acquire_lock(self, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        if not self.locked:
            self.locked = True
            self.holder = run_id
            return True, None, False
        if self.stale and break_stale:
            old = self.holder
            self.holder = run_id
            return True, old, True
        return False, self.holder, False

    def release_lock(self, run_id: str) -> None:
        if self.holder == run_id:
            self.locked = False
            self.holder = None


def failed(message: str, sqlstate: str | None = None) -> ExecResult:
    return ExecResult(False, error=ExecutionError(message, sqlstate))
