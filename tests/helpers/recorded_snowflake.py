"""Recorded, scripted, and read-only Snowflake test doubles.

These implement `SnowflakePort` in memory for the unit and contract tests and for the
`run_recorded_*` scripts beside this module, which the reference project runs to plan and
apply offline. They are test support: nothing in the package imports them, so they do not
ship in the wheel.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from hashlib import md5
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    ExecResult,
    ExecutionError,
    GrantRow,
    OwnershipMarker,
    QueryResult,
    ShowRow,
)
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import (
    ExtensionObservation,
    ExtensionVersion,
    StageObservation,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, StateWrite
from tests.helpers.preflight import PreflightAnswers, PreflightDouble
from tests.helpers.run_locks import InMemoryRunLocks


class RecordedSnowflake(PreflightDouble):
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
        definitions: Mapping[str, str] | None = None,
    ) -> None:
        self.objects = dict(objects or {})
        self.grants = dict(grants or {})
        # GET_DDL's answer by qualified name; an object missing here has no readable definition.
        self.definitions = dict(definitions or {})
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
        self.stage_types: dict[str, str] = {}
        self.extensions: dict[str, dict[str, object]] = {}
        self.tables: dict[str, tuple[tuple[str, str], ...]] = {}
        self.profile_rows: dict[str, dict[str, dict[str, object]]] = {}
        # Statements containing any of these fragments fail, to rehearse refusals.
        self.refused: tuple[str, ...] = ()
        self.state_manifest: str | None = None
        self.run_locks = InMemoryRunLocks()
        self.preflight = PreflightAnswers()
        # The versions of each dataset, by qualified name; ADD VERSION appends to them.
        self.dataset_version_names: dict[str, list[str]] = {}

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        return self.tables.get(qualified_name.sql)

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        if "CREATE TABLE" in " ".join(self.refused):
            raise SnowflakePortError("recorded refusal: CREATE TABLE")
        self.tables.setdefault(qualified_name.sql, PROFILE_REGISTRY_SHAPE)
        self.profile_rows.setdefault(qualified_name.sql, {})

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        row = self.profile_rows.get(registry.sql, {}).get(name)
        return dict(row) if row is not None else None

    def merge_profile_row(
        self,
        registry: QualifiedName,
        row: Mapping[str, object],
        *,
        expected_version: str | None,
    ) -> int:
        if "MERGE" in self.refused:
            raise SnowflakePortError("recorded refusal: MERGE")
        rows = self.profile_rows.setdefault(registry.sql, {})
        name = str(row["CONFIG_NAME"])
        current = rows.get(name)
        if current is not None and (expected_version is None or current.get("VERSION") != expected_version):
            return 0
        stored = {**(current or {}), **{key: _variant_text(key, value) for key, value in row.items()}, "ACTIVE": True}
        rows[name] = stored
        return 1

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        row = self.profile_rows.get(registry.sql, {}).get(name)
        if row is None or row.get("VERSION") != expected_version or row.get("ACTIVE") is not True:
            return 0
        row["ACTIVE"] = False
        return 1

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        rows = self.profile_rows.get(registry.sql, {})
        return tuple(dict(rows[name]) for name in sorted(rows) if rows[name].get("ACTIVE") is True)

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        if qualified_name.sql in self.stage_types:
            return self.stage_types[qualified_name.sql]
        return "INTERNAL NO CSE" if self.object_exists("STAGE", qualified_name) else None

    def list_location(self, location: str) -> tuple[str, ...]:
        if location.startswith("snow://"):
            for extension in self.extensions.values():
                for version in _versions(extension):
                    if str(version["location"]).casefold() == location.casefold():
                        return tuple(sorted(_files(version)))
            return ()
        return tuple(sorted(path[len(location) :] for path in self.stage_files if path.startswith(location)))

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        extension = self.extensions.get(qualified_name.sql)
        if extension is None:
            return None
        versions = _versions(extension)
        default = next((version for version in versions if version["is_default"]), None)
        # MEASURED 2026-09-29: once a version is certified, the extension serves the
        # latest certified version instead of the default.
        certified = next((version for version in reversed(versions) if version["certification"] == "CERTIFIED"), None)
        effective = certified or default
        return ExtensionObservation(
            qualified_name=qualified_name,
            extension_type=str(extension["type"]),
            comment=extension["comment"] if isinstance(extension["comment"], str) else None,
            owner=self.role,
            effective_version=str(effective["name"]) if effective is not None else None,
            latest_certified_version=str(certified["name"]) if certified is not None else None,
        )

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        extension = self.extensions.get(qualified_name.sql)
        if extension is None:
            raise SnowflakePortError(f"Cortex extension {qualified_name.sql} does not exist")
        return tuple(
            ExtensionVersion(
                name=str(version["name"]),
                alias=version["alias"] if isinstance(version["alias"], str) else None,
                location=str(version["location"]),
                is_default=bool(version["is_default"]),
                certification_status=(version["certification"] if isinstance(version["certification"], str) else None),
            )
            for version in _versions(extension)
        )

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

    def get_ddl(self, object_type: str, qualified_name: QualifiedName) -> str:
        if qualified_name.sql not in self.definitions:
            raise SnowflakePortError(f"GET_DDL refused for {object_type} {qualified_name.sql}")
        return self.definitions[qualified_name.sql]

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        del object_type
        return self.markers.get(qualified_name.sql)

    def query(self, sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        self.queries.append((str(sql), params))
        return QueryResult()

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: Sql,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        del scope
        return self.query(sql, params)

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        texts = tuple(str(statement) for statement in statements)
        self.scripts.append(texts)
        refused = next(
            (statement for statement in texts if any(fragment in statement for fragment in self.refused)),
            None,
        )
        if refused is not None:
            return ExecResult(False, error=ExecutionError(f"recorded refusal: {refused[:60]}"))
        self._record_successful_statements(texts)
        return ExecResult(True)

    def try_execute(self, sql: Sql) -> ExecResult:
        return self.execute_script((sql,))

    def current_role(self) -> str:
        return self.role

    def current_account_locator(self) -> str:
        return self.account_locator

    # A recorded session records no SHOW rows, DESCRIBE properties, or parameters: each reads
    # as absent, so a connected check that needs one finds nothing to report.
    def show_row(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        del object_type, qualified_name
        return None

    def describe_properties(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        del object_type, qualified_name
        return None

    def object_parameter(self, object_type: str, name: str, parameter: str) -> str | None:
        del object_type, name, parameter
        return None

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

    def dataset_versions(self, qualified_name: QualifiedName) -> tuple[str, ...]:
        return tuple(self.dataset_version_names.get(qualified_name.sql, ()))

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
        if stage_path.startswith("snow://"):
            for extension in self.extensions.values():
                live = extension.get("live")
                if isinstance(live, dict) and stage_path.startswith(str(live["location"])):
                    _files(live).append(stage_path[len(str(live["location"])) :])
                    return
            raise SnowflakePortError(f"no open live version accepts {stage_path}")
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

    def read_state_manifest(self, state_table: QualifiedName, target_name: str) -> str | None:
        del state_table, target_name
        return self.state_manifest

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        write: StateWrite,
    ) -> None:
        del state_table, target_name
        current = {**self.state, **write.upserts}
        for key in write.deletes:
            current.pop(key, None)
        self.state = MappingProxyType(current)
        self.state_manifest = manifest_id

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        del state_table

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        return self.run_locks.acquire_run_lock(state_table, target_name, claim, break_stale=break_stale)

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, claim: LockClaim) -> bool:
        return self.run_locks.extend_run_lock(state_table, target_name, claim)

    def release_run_lock(self, state_table: QualifiedName, target_name: str, run_id: str) -> None:
        self.run_locks.release_run_lock(state_table, target_name, run_id)

    def _record_successful_statements(self, statements: Sequence[str]) -> None:
        for statement in statements:
            normalized = " ".join(statement.split())
            if normalized.upper().startswith("CREATE STAGE IF NOT EXISTS "):
                name = normalized.split()[5]
                self.stage_formats[name] = _stage_format_from_statement(normalized)
                if "SNOWFLAKE_SSE" in normalized.upper():
                    self.stage_types[name] = "INTERNAL NO CSE"
                if self.existing is not None:
                    self.existing.add(name)
            elif normalized.upper().startswith(("CREATE CORTEX EXTENSION", "ALTER CORTEX EXTENSION")):
                self._record_extension_statement(normalized, statement)
            elif normalized.upper().startswith("CREATE TABLE "):
                name = normalized.split()[2]
                if self.existing is not None:
                    self.existing.add(name)
            elif normalized.upper().startswith("ALTER DATASET ") and " ADD VERSION " in normalized.upper():
                tokens = normalized.split()
                self.dataset_version_names.setdefault(tokens[2], []).append(tokens[5].strip("'"))
            elif "SYSTEM$CREATE_EVALUATION_DATASET" in normalized.upper():
                quoted = _quoted_sql_arguments(normalized)
                if len(quoted) >= 3 and self.existing is not None:
                    self.existing.add(quoted[2])
                    # The role that creates a dataset owns it.
                    self.grants.setdefault(quoted[2], (GrantRow("OWNERSHIP", "ROLE", self.current_role()),))

    def _record_extension_statement(self, normalized: str, original: str | None = None) -> None:
        tokens = normalized.split()
        upper = [token.upper() for token in tokens]
        quoted = tuple(value.replace("\\\\", "\\") for value in _quoted_sql_arguments(original or normalized))
        if upper[0] == "CREATE":
            name = tokens[6]
            if name not in self.extensions:
                location = f"snow://cortex_extension/{name}/versions/version$1/"
                self.extensions[name] = {
                    "type": quoted[0].upper(),
                    "comment": quoted[1] if len(quoted) > 1 else None,
                    "versions": [
                        {
                            "name": "VERSION$1",
                            "alias": None,
                            "location": location,
                            "files": [],
                            "is_default": False,
                            "certification": None,
                        }
                    ],
                    "live": None,
                }
            return
        name = tokens[3]
        extension = self.extensions[name]
        clause = " ".join(upper[4:])
        if clause.startswith("ADD VERSION ") and " FROM " in clause:
            alias = tokens[6]
            source = tokens[8]
            files = [path[len(source) :] for path in sorted(self.stage_files) if path.startswith(source)]
            self._add_version(name, extension, alias, files)
        elif clause.startswith("ADD LIVE VERSION "):
            extension["live"] = {
                "alias": tokens[7],
                "location": f"snow://cortex_extension/{name}/versions/live/",
                "files": [],
            }
        elif clause == "COMMIT":
            live = extension["live"]
            if not isinstance(live, dict):
                raise SnowflakePortError(f"Cortex extension {name} has no open live version")
            self._add_version(name, extension, str(live["alias"]), _files(live))
            extension["live"] = None
        elif clause == "ABORT":
            extension["live"] = None
        elif clause.startswith("SET COMMENT"):
            extension["comment"] = quoted[0]
        elif clause.startswith("VERSION ") and "SET TAG" in clause:
            label = tokens[5].casefold()
            for version in _versions(extension):
                if label in (str(version["alias"]).casefold(), str(version["name"]).casefold()):
                    version["certification"] = quoted[0]

    @staticmethod
    def _add_version(name: str, extension: dict[str, object], alias: str, files: list[str]) -> None:
        versions = _versions(extension)
        if any(str(version["alias"]).casefold() == alias.casefold() for version in versions):
            raise SnowflakePortError(f"version alias {alias} already exists in {name}")
        system = f"VERSION${len(versions) + 1}"
        for version in versions:
            version["is_default"] = False
        versions.append(
            {
                "name": system,
                "alias": alias,
                "location": f"snow://cortex_extension/{name}/versions/{system.lower()}/",
                "files": list(files),
                "is_default": True,
                "certification": None,
            }
        )


def _versions(extension: Mapping[str, object]) -> list[dict[str, object]]:
    versions = extension["versions"]
    assert isinstance(versions, list)
    return versions


PROFILE_REGISTRY_SHAPE: tuple[tuple[str, str], ...] = (
    ("CONFIG_NAME", "VARCHAR(16777216)"),
    ("DESCRIPTION", "VARCHAR(16777216)"),
    ("OWNER_TEAM", "VARCHAR(16777216)"),
    ("SKILL_REPOS", "VARIANT"),
    ("MCP_SERVERS", "VARIANT"),
    ("COMMAND_REPOS", "VARIANT"),
    ("ENV_VARS", "VARIANT"),
    ("SETTINGS_OVERRIDES", "VARIANT"),
    ("VERSION", "VARCHAR(16777216)"),
    ("ACTIVE", "BOOLEAN"),
    ("CREATED_AT", "TIMESTAMP_NTZ(9)"),
    ("UPDATED_AT", "TIMESTAMP_NTZ(9)"),
    ("SYSTEM_PROMPT_REPO", "VARIANT"),
    ("HOOKS", "VARIANT"),
    ("SCRIPTS", "VARIANT"),
    ("PLUGINS", "VARIANT"),
    ("ALLOWED_ROLES", "VARIANT"),
    ("PERMISSIONS", "VARIANT"),
)
_VARIANT_COLUMNS = frozenset(name for name, kind in PROFILE_REGISTRY_SHAPE if kind == "VARIANT")


def _variant_text(column: str, value: object) -> object:
    """The driver returns VARIANT columns as JSON text, so the recording does too."""
    if column in _VARIANT_COLUMNS and value is not None:
        import json

        return json.dumps(value, sort_keys=True)
    return value


def _files(version: Mapping[str, object]) -> list[str]:
    files = version["files"]
    assert isinstance(files, list)
    return files


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

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        texts = tuple(str(statement) for statement in statements)
        self.scripts.append(texts)
        result = self.results.popleft() if self.results else ExecResult(True)
        if result.ok:
            self._record_successful_statements(texts)
        return result

    def query(self, sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        self.queries.append((str(sql), params))
        if self.query_failures:
            raise self.query_failures.popleft()
        if self.query_results:
            return self.query_results.popleft()
        return QueryResult()


class ReadOnlySnowflake:
    def __init__(self, delegate: SnowflakePort) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        raise SnowflakePortError(f"read-only Snowflake adapter refused {len(statements)} statement(s)")

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: Sql,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        return self._delegate.query_in_context(scope, sql, params)

    def upload(self, stage_path: str, content: bytes) -> None:
        del stage_path, content
        raise SnowflakePortError("read-only Snowflake adapter refused upload")

    def read_staged_file(self, stage_path: str) -> bytes | None:
        return self._delegate.read_staged_file(stage_path)

    def try_execute(self, sql: Sql) -> ExecResult:
        raise SnowflakePortError(f"read-only Snowflake adapter refused statement: {str(sql)[:40]}")

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        return self._delegate.resolve_agent_version(qualified_name, selector)

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        write: StateWrite,
    ) -> None:
        del state_table, target_name, manifest_id, write
        raise SnowflakePortError("read-only Snowflake adapter refused state write")

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        del state_table
        raise SnowflakePortError("read-only Snowflake adapter refused state table creation")

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        del state_table, target_name, claim, break_stale
        raise SnowflakePortError("read-only Snowflake adapter refused the run lock")


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
