"""Recorded, scripted, and read-only Snowflake adapters."""

from __future__ import annotations

from collections import deque
from hashlib import md5
from types import MappingProxyType
from typing import Mapping, Sequence

from ...domain.model.identifier import QualifiedName, SchemaScope
from ...domain.model.lifecycle import ExecResult, GrantRow, OwnershipMarker, QueryResult, ShowRow
from ...domain.ports.snowflake import SnowflakePort, SnowflakePortError, StagedFileMetadata, StageObservation
from ...domain.state.model import AppliedEntry


class RecordedSnowflake:
    def __init__(
        self,
        *,
        objects: Mapping[tuple[str, str], tuple[ShowRow, ...]] | None = None,
        grants: Mapping[str, tuple[GrantRow, ...]] | None = None,
        markers: Mapping[str, OwnershipMarker | None] | None = None,
        existing: tuple[str, ...] | None = None,
        state: Mapping[str, AppliedEntry] | None = None,
        role: str = "RECORDED_ROLE",
        account_locator: str = "RECORDED_ACCOUNT",
        stage_formats: Mapping[str, str] | None = None,
        stage_files: tuple[str, ...] = (),
        staged_file_metadata: Mapping[str, StagedFileMetadata] | None = None,
        staged_file_contents: Mapping[str, bytes] | None = None,
        agent_versions: Mapping[tuple[str, str], str] | None = None,
    ) -> None:
        self.objects = dict(objects or {})
        self.grants = dict(grants or {})
        self.markers = dict(markers or {})
        self.existing = set(existing) if existing is not None else None
        self.state = MappingProxyType(dict(state or {}))
        self.role = role
        self.account_locator = account_locator
        self.stage_formats = dict(stage_formats or {})
        self.stage_files = set(stage_files)
        self.staged_file_metadata = dict(staged_file_metadata or {})
        self.staged_file_contents = dict(staged_file_contents or {})
        self.agent_versions = {
            (name, selector.casefold()): version for (name, selector), version in (agent_versions or {}).items()
        }
        self.stage_files.update(self.staged_file_metadata)
        self.stage_files.update(self.staged_file_contents)
        self.queries: list[tuple[str, object]] = []
        self.scripts: list[tuple[str, ...]] = []
        self.uploads: list[tuple[str, bytes]] = []
        self.live_agents: set[str] = set()

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        rows: tuple[ShowRow, ...] = self.objects.get((object_type.upper(), scope.sql), ())
        return rows

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]:
        del object_type, routine_signature
        return self.grants.get(qualified_name.sql, ())

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        del object_type
        return self.markers.get(qualified_name.sql)

    def query(self, sql: str, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        self.queries.append((sql, params))
        return QueryResult()

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: str,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        del scope
        return self.query(sql, params)

    def execute_script(self, statements: Sequence[str]) -> ExecResult:
        self.scripts.append(tuple(statements))
        self._record_successful_statements(statements)
        return ExecResult(True)

    def try_execute(self, sql: str) -> ExecResult:
        return self.execute_script((sql,))

    def current_role(self) -> str:
        return self.role

    def current_account_locator(self) -> str:
        return self.account_locator

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        if object_type.upper() == "DATASET":
            return self.dataset_exists(qualified_name)
        if object_type.upper() == "STAGE" and qualified_name.sql in self.stage_formats:
            return True
        if self.existing is None:
            return object_type == "TABLE OR VIEW"
        return qualified_name.sql in self.existing

    def dataset_exists(self, qualified_name: QualifiedName) -> bool:
        return self.existing is not None and qualified_name.sql in self.existing

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation:
        exists = self.object_exists("STAGE", qualified_name)
        return StageObservation(exists, self.stage_formats.get(qualified_name.sql) if exists else None)

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        return self.stage_formats.get(qualified_name.sql)

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        if stage_path not in self.stage_files:
            return None
        return self.staged_file_metadata.get(stage_path, _default_staged_file_metadata(stage_path))

    def stage_file_exists(self, stage_path: str) -> bool:
        return self.observe_staged_file(stage_path) is not None

    def read_staged_file(self, stage_path: str) -> bytes | None:
        return self.staged_file_contents.get(stage_path)

    def upload(self, stage_path: str, content: bytes) -> None:
        self.uploads.append((stage_path, content))
        self.stage_files.add(stage_path)
        self.staged_file_contents[stage_path] = content
        self.staged_file_metadata[stage_path] = StagedFileMetadata(
            stage_path=stage_path,
            name=stage_path[1:] if stage_path.startswith("@") else stage_path,
            size=len(content),
            md5=md5(content, usedforsecurity=False).hexdigest(),
        )

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        return qualified_name.sql in self.live_agents

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        if selector.upper().startswith("VERSION$"):
            return selector.upper()
        try:
            return self.agent_versions[(qualified_name.sql, selector.casefold())]
        except KeyError as exc:
            raise SnowflakePortError(
                f"agent {qualified_name.sql} selector {selector!r} does not resolve to a committed version"
            ) from exc

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None:
        del state_table, target_name
        return self.state

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        applied: Mapping[str, AppliedEntry],
    ) -> None:
        del state_table, target_name, manifest_id
        self.state = MappingProxyType(dict(applied))

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        del state_table

    def delete_state(self, state_table: QualifiedName, target_name: str, artifact_key: str) -> int:
        del state_table, target_name
        existed = artifact_key in self.state
        self.state = MappingProxyType({key: value for key, value in self.state.items() if key != artifact_key})
        return int(existed)

    def upsert_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        artifact_key: str,
        entry: AppliedEntry,
    ) -> int:
        del state_table, target_name
        self.state = MappingProxyType({**self.state, artifact_key: entry})
        return 1

    def _record_successful_statements(self, statements: Sequence[str]) -> None:
        for statement in statements:
            normalized = " ".join(statement.split())
            if normalized.upper().startswith("CREATE STAGE IF NOT EXISTS "):
                name = normalized.split()[5]
                self.stage_formats[name] = _stage_format_from_statement(normalized)
                if self.existing is not None:
                    self.existing.add(name)
            elif normalized.upper().startswith("CREATE TABLE "):
                name = normalized.split()[2]
                if self.existing is not None:
                    self.existing.add(name)
            elif "SYSTEM$CREATE_EVALUATION_DATASET" in normalized.upper():
                quoted = _quoted_sql_arguments(normalized)
                if len(quoted) >= 3 and self.existing is not None:
                    self.existing.add(quoted[2])


class ScriptedSnowflake(RecordedSnowflake):
    def __init__(
        self,
        results: Sequence[ExecResult] = (),
        query_results: Sequence[QueryResult] = (),
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.results = deque(results)
        self.query_results = deque(query_results)
        self.query_failures: deque[SnowflakePortError] = deque()

    def execute_script(self, statements: Sequence[str]) -> ExecResult:
        self.scripts.append(tuple(statements))
        result = self.results.popleft() if self.results else ExecResult(True)
        if result.ok:
            self._record_successful_statements(statements)
        return result

    def query(self, sql: str, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        self.queries.append((sql, params))
        if self.query_failures:
            raise self.query_failures.popleft()
        if self.query_results:
            return self.query_results.popleft()
        return QueryResult()


class ReadOnlySnowflake:
    def __init__(self, delegate: SnowflakePort) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str) -> object:
        return getattr(self._delegate, name)

    def execute_script(self, statements: Sequence[str]) -> ExecResult:
        raise SnowflakePortError(f"read-only Snowflake adapter refused {len(statements)} statement(s)")

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: str,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        return self._delegate.query_in_context(scope, sql, params)

    def upload(self, stage_path: str, content: bytes) -> None:
        del stage_path, content
        raise SnowflakePortError("read-only Snowflake adapter refused upload")

    def read_staged_file(self, stage_path: str) -> bytes | None:
        return self._delegate.read_staged_file(stage_path)

    def try_execute(self, sql: str) -> ExecResult:
        raise SnowflakePortError(f"read-only Snowflake adapter refused statement: {sql[:40]}")

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        return self._delegate.resolve_agent_version(qualified_name, selector)

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        applied: Mapping[str, AppliedEntry],
    ) -> None:
        del state_table, target_name, manifest_id, applied
        raise SnowflakePortError("read-only Snowflake adapter refused state write")

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        del state_table
        raise SnowflakePortError("read-only Snowflake adapter refused state table creation")

    def delete_state(self, state_table: QualifiedName, target_name: str, artifact_key: str) -> int:
        del state_table, target_name, artifact_key
        raise SnowflakePortError("read-only Snowflake adapter refused state delete")

    def upsert_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        artifact_key: str,
        entry: AppliedEntry,
    ) -> int:
        del state_table, target_name, artifact_key, entry
        raise SnowflakePortError("read-only Snowflake adapter refused state upsert")


def _stage_format_from_statement(statement: str) -> str:
    marker = "FILE_FORMAT = ("
    index = statement.upper().find(marker)
    return statement[index + len(marker) :].rsplit(")", 1)[0] if index >= 0 else ""


def _quoted_sql_arguments(statement: str) -> tuple[str, ...]:
    values: list[str] = []
    current: list[str] = []
    quoted = False
    index = 0
    while index < len(statement):
        character = statement[index]
        if character == "'":
            if quoted and index + 1 < len(statement) and statement[index + 1] == "'":
                current.append("'")
                index += 1
            else:
                quoted = not quoted
                if not quoted:
                    values.append("".join(current))
                    current = []
        elif quoted:
            current.append(character)
        index += 1
    return tuple(values)


def _default_staged_file_metadata(stage_path: str) -> StagedFileMetadata:
    return StagedFileMetadata(
        stage_path=stage_path,
        name=stage_path[1:] if stage_path.startswith("@") else stage_path,
        size=0,
    )
