"""Production Snowflake connector adapter."""

from __future__ import annotations

import json
from pathlib import PurePosixPath
from threading import RLock
from types import MappingProxyType
from typing import Any, Mapping, Sequence, cast

import snowflake.connector
from snowflake.connector import DictCursor

from ...domain.model.identifier import QualifiedName, SchemaScope
from ...domain.model.lifecycle import (
    ExecResult,
    ExecutionError,
    GrantRow,
    OwnershipMarker,
    QueryResult,
    ShowRow,
    extract_marker,
)
from ...domain.ports.snowflake import SnowflakePortError, StagedFileMetadata, StageObservation
from ...domain.state.model import AppliedEntry, AppliedResource

STATE_COLUMNS = (
    "TARGET_NAME",
    "ARTIFACT_KEY",
    "FINGERPRINT",
    "QUALIFIED_NAME",
    "MANIFEST_ID",
    "GIT_SHA",
    "APPLIED_AT",
    "RUN_ID",
    "OUTCOME",
    "DDL_SHA256",
    "COMPONENT_FINGERPRINTS",
    "PHYSICAL_RESOURCES",
)
OBJECT_TYPES = frozenset(
    (
        "SEMANTIC VIEW",
        "CORTEX SEARCH SERVICE",
        "PROCEDURE",
        "FUNCTION",
        "STAGE",
        "AGENT",
        "DATASET",
        "TABLE",
        "VIEW",
    )
)


class SnowflakeConnector:
    def __init__(self, connection_params: Mapping[str, object]) -> None:
        try:
            self._connection = snowflake.connector.connect(**dict(connection_params))
            self._lock = RLock()
        except Exception as exc:
            raise _port_error(exc) from exc

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        normalized_type = _object_type(object_type)
        sql = f"SHOW {normalized_type}S IN SCHEMA {scope.sql}"
        rows = self._dict_rows(sql)
        return tuple(
            sorted(
                (
                    ShowRow(
                        name=str(row["name"]),
                        database_name=str(row.get("database_name") or scope.database.folded),
                        schema_name=str(row.get("schema_name") or scope.schema.folded),
                        owner=str(row.get("owner") or ""),
                        created_on=str(row.get("created_on") or ""),
                        comment=str(row["comment"]) if row.get("comment") is not None else None,
                        object_type=normalized_type,
                    )
                    for row in rows
                ),
                key=lambda item: item.qualified_name.folded,
            )
        )

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]:
        object_name = qualified_name.sql
        if object_type.upper() in {"PROCEDURE", "FUNCTION"}:
            object_name += f"({', '.join(routine_signature)})"
        rows = self._dict_rows(f"SHOW GRANTS ON {_object_type(object_type)} {object_name}")
        return tuple(
            sorted(
                GrantRow(
                    privilege=str(row.get("privilege") or ""),
                    granted_to=str(row.get("granted_to") or ""),
                    grantee_name=str(row.get("grantee_name") or ""),
                    granted_by=str(row.get("granted_by") or ""),
                    grant_option=str(row.get("grant_option") or "").lower() in {"true", "yes"},
                )
                for row in rows
            )
        )

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        pattern = qualified_name.name.folded.replace("'", "''")
        rows = self._dict_rows(
            f"SHOW {_object_type(object_type)}S LIKE '{pattern}' "
            f"IN SCHEMA {qualified_name.database.sql}.{qualified_name.schema.sql}"
        )
        for row in rows:
            if str(row.get("name") or "").upper() == qualified_name.name.folded.upper():
                return extract_marker(str(row["comment"]) if row.get("comment") is not None else None)
        return None

    def query(self, sql: str, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        try:
            with self._lock:
                cursor = self._connection.cursor()
                try:
                    connector_params = cast(Sequence[Any] | dict[Any, Any] | None, params)
                    cursor.execute(sql, connector_params)
                    columns = tuple(item[0] for item in (cursor.description or ()))
                    rows = tuple(tuple(row) for row in cursor.fetchall()) if cursor.description else ()
                    return QueryResult(columns, rows)
                finally:
                    cursor.close()
        except Exception as exc:
            raise _port_error(exc) from exc

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: str,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        try:
            with self._lock:
                cursor = self._connection.cursor()
                try:
                    cursor.execute(f"USE DATABASE {scope.database.sql}")
                    cursor.execute(f"USE SCHEMA {scope.sql}")
                    connector_params = cast(Sequence[Any] | dict[Any, Any] | None, params)
                    cursor.execute(sql, connector_params)
                    columns = tuple(item[0] for item in (cursor.description or ()))
                    rows = tuple(tuple(row) for row in cursor.fetchall()) if cursor.description else ()
                    return QueryResult(columns, rows)
                finally:
                    cursor.close()
        except Exception as exc:
            raise _port_error(exc) from exc

    def execute_script(self, statements: Sequence[str]) -> ExecResult:
        query_ids: list[str] = []
        try:
            with self._lock:
                cursor = self._connection.cursor()
                try:
                    for statement in statements:
                        cursor.execute(statement)
                        query_ids.append(str(cursor.sfqid or ""))
                finally:
                    cursor.close()
            return ExecResult(True, tuple(query_ids), rows_affected=0)
        except Exception as exc:
            write_succeeded = bool(query_ids)
            return ExecResult(
                False,
                tuple(query_ids),
                ExecutionError(
                    str(exc),
                    getattr(exc, "sqlstate", None),
                    getattr(exc, "errno", None),
                ),
                rows_affected=(1 if write_succeeded else 0),
            )

    def try_execute(self, sql: str) -> ExecResult:
        return self.execute_script((sql,))

    def current_role(self) -> str:
        return str(self.query("SELECT CURRENT_ROLE()").rows[0][0])

    def current_account_locator(self) -> str:
        return str(self.query("SELECT CURRENT_ACCOUNT()").rows[0][0])

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        normalized_input = " ".join(object_type.upper().split())
        if normalized_input == "DATASET":
            return self.dataset_exists(qualified_name)
        if normalized_input in {"TABLE OR VIEW", "TABLE"}:
            type_filter = "" if normalized_input == "TABLE OR VIEW" else " AND TABLE_TYPE = 'BASE TABLE'"
            result = self.query(
                f"SELECT COUNT(*) FROM {qualified_name.database.sql}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s{type_filter}",
                (qualified_name.schema.folded, qualified_name.name.folded),
            )
            count = result.rows[0][0] if result.rows else 0
            return isinstance(count, (int, float, str)) and int(count) > 0
        normalized_type = _object_type(normalized_input)
        pattern = qualified_name.name.folded.replace("'", "''")
        rows = self._dict_rows(
            f"SHOW {normalized_type}S LIKE '{pattern}' IN SCHEMA "
            f"{qualified_name.database.sql}.{qualified_name.schema.sql}"
        )
        return any(
            str(row.get("name") or "").upper() == qualified_name.name.folded.upper()
            and str(row.get("database_name") or qualified_name.database.folded).upper()
            == qualified_name.database.folded.upper()
            and str(row.get("schema_name") or qualified_name.schema.folded).upper()
            == qualified_name.schema.folded.upper()
            for row in rows
        )

    def dataset_exists(self, qualified_name: QualifiedName) -> bool:
        pattern = qualified_name.name.folded.replace("'", "''")
        rows = self._dict_rows(
            f"SHOW DATASETS LIKE '{pattern}' IN SCHEMA {qualified_name.database.sql}.{qualified_name.schema.sql}"
        )
        return any(
            _identifier_matches(row.get("name"), qualified_name.name.folded)
            and _identifier_matches(row.get("database_name"), qualified_name.database.folded)
            and _identifier_matches(row.get("schema_name"), qualified_name.schema.folded)
            for row in rows
        )

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation:
        if not self.object_exists("STAGE", qualified_name):
            return StageObservation(False)
        return StageObservation(True, self.describe_stage_file_format(qualified_name))

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        rows = self._dict_rows(f"DESCRIBE STAGE {qualified_name.sql}")
        properties = {
            str(row.get("property") or row.get("name") or "").upper(): row.get("property_value", row.get("value"))
            for row in rows
        }
        file_format = properties.get("FILE_FORMAT")
        if file_format is not None:
            return str(file_format)
        required = (
            "TYPE",
            "FIELD_DELIMITER",
            "RECORD_DELIMITER",
            "SKIP_HEADER",
            "FIELD_OPTIONALLY_ENCLOSED_BY",
            "ESCAPE_UNENCLOSED_FIELD",
        )
        if not any(key in properties for key in required):
            return None
        return " ".join(f"{key}={properties.get(key)}" for key in required)

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        stage_path = _validated_stage_path(stage_path)
        rows = self._dict_rows(f"LIST '{stage_path}'")
        expected_name = stage_path[1:]
        matches = tuple(row for row in rows if _staged_file_name_matches(row.get("name"), stage_path))
        if len(matches) > 1:
            raise SnowflakePortError(f"stage path returned duplicate files: {stage_path}")
        if not matches:
            return None
        row = matches[0]
        try:
            size = int(row.get("size") or 0)
        except (TypeError, ValueError) as exc:
            raise SnowflakePortError(f"stage file {stage_path} returned invalid size metadata") from exc
        return StagedFileMetadata(
            stage_path=stage_path,
            name=expected_name,
            size=size,
            md5=str(row["md5"]) if row.get("md5") is not None else None,
            last_modified=str(row["last_modified"]) if row.get("last_modified") is not None else None,
        )

    def stage_file_exists(self, stage_path: str) -> bool:
        return self.observe_staged_file(stage_path) is not None

    def read_staged_file(self, stage_path: str) -> bytes | None:
        stage_path = _validated_stage_path(stage_path)
        import os
        import tempfile

        temp_dir = tempfile.mkdtemp(prefix="sst-stage-read-")
        try:
            result = self.execute_script((f"GET '{stage_path}' 'file://{temp_dir}'",))
            if not result.ok:
                raise SnowflakePortError(result.error.message if result.error else "stage download failed")
            local_path = os.path.join(temp_dir, PurePosixPath(stage_path).name)
            try:
                with open(local_path, "rb") as handle:
                    return handle.read()
            except FileNotFoundError:
                return None
        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def upload(self, stage_path: str, content: bytes) -> None:
        stage_path = _validated_stage_path(stage_path)
        import os
        import tempfile

        basename = PurePosixPath(stage_path).name
        temp_dir = tempfile.mkdtemp(prefix="sst-upload-")
        local_path = os.path.join(temp_dir, basename)
        try:
            with open(local_path, "wb") as handle:
                handle.write(content)
            destination = stage_path.rsplit("/", 1)[0] + "/"
            result = self.execute_script(
                (f"PUT 'file://{local_path}' {destination} " "OVERWRITE=TRUE AUTO_COMPRESS=FALSE",)
            )
            if not result.ok:
                raise SnowflakePortError(result.error.message if result.error else "stage upload failed")
        finally:
            try:
                os.unlink(local_path)
                os.rmdir(temp_dir)
            except OSError:
                pass

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        rows = self._dict_rows(f"SHOW VERSIONS IN AGENT {qualified_name.sql}")
        return any(row.get("name") is None for row in rows)

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        if selector.upper().startswith("VERSION$"):
            return selector.upper()
        rows = self._dict_rows(f"DESCRIBE AGENT {qualified_name.sql}")
        properties = {
            str(row.get("name") or row.get("property") or "").casefold(): row.get("value", row.get("property_value"))
            for row in rows
        }
        aliases = _variant_value(properties.get("aliases"), {})
        if not isinstance(aliases, dict):
            raise SnowflakePortError(f"agent {qualified_name.sql} returned invalid aliases metadata")
        key = "LAST" if selector == "committed" else selector.removeprefix("alias:")
        resolved = next((value for alias, value in aliases.items() if str(alias).casefold() == key.casefold()), None)
        if not isinstance(resolved, str) or not resolved.upper().startswith("VERSION$"):
            raise SnowflakePortError(
                f"agent {qualified_name.sql} selector {selector!r} does not resolve to a committed version"
            )
        return resolved.upper()

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None:
        if not self.object_exists("TABLE", state_table):
            return MappingProxyType({})
        try:
            columns = {
                str(row.get("name") or "").upper() for row in self._dict_rows(f"DESCRIBE TABLE {state_table.sql}")
            }
            component_select = (
                "COMPONENT_FINGERPRINTS, PHYSICAL_RESOURCES"
                if {"COMPONENT_FINGERPRINTS", "PHYSICAL_RESOURCES"}.issubset(columns)
                else "NULL COMPONENT_FINGERPRINTS, NULL PHYSICAL_RESOURCES"
            )
            result = self.query(
                f"SELECT ARTIFACT_KEY, FINGERPRINT, QUALIFIED_NAME, MANIFEST_ID, GIT_SHA, "
                f"APPLIED_AT, RUN_ID, OUTCOME, DDL_SHA256, {component_select} "
                f"FROM {state_table.sql} "
                "WHERE TARGET_NAME = %s",
                (target_name,),
            )
        except SnowflakePortError:
            return None
        return MappingProxyType(
            {
                str(row[0]): AppliedEntry(
                    fingerprint=str(row[1]),
                    qualified_name=str(row[2]),
                    manifest_id=str(row[3]),
                    git_sha=str(row[4] or ""),
                    applied_at=str(row[5]),
                    run_id=str(row[6]),
                    outcome=str(row[7]),
                    ddl_sha256=str(row[8]),
                    component_fingerprints=_component_fingerprints(row[9]),
                    physical_resources=_physical_resources(row[10]),
                )
                for row in result.rows
            }
        )

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        create = (
            f"CREATE TABLE IF NOT EXISTS {state_table.sql} ("
            "TARGET_NAME VARCHAR NOT NULL, ARTIFACT_KEY VARCHAR NOT NULL, "
            "FINGERPRINT VARCHAR(64) NOT NULL, QUALIFIED_NAME VARCHAR NOT NULL, "
            "MANIFEST_ID VARCHAR(64) NOT NULL, GIT_SHA VARCHAR, APPLIED_AT TIMESTAMP_TZ, "
            "RUN_ID VARCHAR, OUTCOME VARCHAR, DDL_SHA256 VARCHAR(64), "
            "COMPONENT_FINGERPRINTS OBJECT, PHYSICAL_RESOURCES ARRAY, "
            "PRIMARY KEY (TARGET_NAME, ARTIFACT_KEY))"
        )
        created = self.execute_script((create,))
        if not created.ok:
            raise SnowflakePortError(created.error.message if created.error else "state table creation failed")
        self._ensure_state_columns(state_table)

    def _ensure_state_columns(self, state_table: QualifiedName) -> None:
        rows = self._dict_rows(f"DESCRIBE TABLE {state_table.sql}")
        columns = {str(row.get("name") or "").upper() for row in rows}
        additions = []
        if "COMPONENT_FINGERPRINTS" not in columns:
            additions.append("ADD COLUMN COMPONENT_FINGERPRINTS OBJECT")
        if "PHYSICAL_RESOURCES" not in columns:
            additions.append("ADD COLUMN PHYSICAL_RESOURCES ARRAY")
        if additions:
            result = self.execute_script(tuple(f"ALTER TABLE {state_table.sql} {clause}" for clause in additions))
            if not result.ok:
                raise SnowflakePortError(result.error.message if result.error else "state table migration failed")

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        applied: Mapping[str, AppliedEntry],
    ) -> None:
        del manifest_id
        self.ensure_state_table(state_table)
        try:
            with self._lock:
                cursor = self._connection.cursor()
                try:
                    cursor.execute("BEGIN")
                    cursor.execute(
                        f"DELETE FROM {state_table.sql} WHERE TARGET_NAME = %s",
                        (target_name,),
                    )
                    for key, entry in sorted(applied.items()):
                        cursor.execute(
                            f"INSERT INTO {state_table.sql} ({', '.join(STATE_COLUMNS)}) "
                            "SELECT %s, %s, %s, %s, %s, %s, TO_TIMESTAMP_TZ(%s), %s, %s, %s, "
                            "PARSE_JSON(%s), PARSE_JSON(%s)",
                            (
                                target_name,
                                key,
                                entry.fingerprint,
                                entry.qualified_name,
                                entry.manifest_id,
                                entry.git_sha,
                                entry.applied_at,
                                entry.run_id,
                                entry.outcome,
                                entry.ddl_sha256,
                                _json_components(entry),
                                _json_resources(entry),
                            ),
                        )
                    cursor.execute("COMMIT")
                except Exception:
                    cursor.execute("ROLLBACK")
                    raise
                finally:
                    cursor.close()
        except Exception as exc:
            raise _port_error(exc) from exc

    def delete_state(self, state_table: QualifiedName, target_name: str, artifact_key: str) -> int:
        try:
            with self._lock:
                cursor = self._connection.cursor()
                try:
                    cursor.execute(
                        f"DELETE FROM {state_table.sql} WHERE TARGET_NAME = %s AND ARTIFACT_KEY = %s",
                        (target_name, artifact_key),
                    )
                    return max(cursor.rowcount or 0, 0)
                finally:
                    cursor.close()
        except Exception as exc:
            raise _port_error(exc) from exc

    def upsert_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        artifact_key: str,
        entry: AppliedEntry,
    ) -> int:
        sql = (
            f"MERGE INTO {state_table.sql} AS target USING (SELECT %s TARGET_NAME, %s ARTIFACT_KEY, "
            "%s FINGERPRINT, %s QUALIFIED_NAME, %s MANIFEST_ID, %s GIT_SHA, "
            "TO_TIMESTAMP_TZ(%s) APPLIED_AT, %s RUN_ID, %s OUTCOME, %s DDL_SHA256, "
            "PARSE_JSON(%s) COMPONENT_FINGERPRINTS, PARSE_JSON(%s) PHYSICAL_RESOURCES) AS source "
            "ON target.TARGET_NAME = source.TARGET_NAME AND target.ARTIFACT_KEY = source.ARTIFACT_KEY "
            "WHEN MATCHED THEN UPDATE SET FINGERPRINT=source.FINGERPRINT, QUALIFIED_NAME=source.QUALIFIED_NAME, "
            "MANIFEST_ID=source.MANIFEST_ID, GIT_SHA=source.GIT_SHA, APPLIED_AT=source.APPLIED_AT, "
            "RUN_ID=source.RUN_ID, OUTCOME=source.OUTCOME, DDL_SHA256=source.DDL_SHA256, "
            "COMPONENT_FINGERPRINTS=source.COMPONENT_FINGERPRINTS, PHYSICAL_RESOURCES=source.PHYSICAL_RESOURCES "
            "WHEN NOT MATCHED THEN INSERT (TARGET_NAME, ARTIFACT_KEY, FINGERPRINT, QUALIFIED_NAME, MANIFEST_ID, "
            "GIT_SHA, APPLIED_AT, RUN_ID, OUTCOME, DDL_SHA256, COMPONENT_FINGERPRINTS, PHYSICAL_RESOURCES) "
            "VALUES (source.TARGET_NAME, source.ARTIFACT_KEY, "
            "source.FINGERPRINT, source.QUALIFIED_NAME, source.MANIFEST_ID, source.GIT_SHA, source.APPLIED_AT, "
            "source.RUN_ID, source.OUTCOME, source.DDL_SHA256, source.COMPONENT_FINGERPRINTS, "
            "source.PHYSICAL_RESOURCES)"
        )
        try:
            with self._lock:
                cursor = self._connection.cursor()
                try:
                    cursor.execute(
                        sql,
                        (
                            target_name,
                            artifact_key,
                            entry.fingerprint,
                            entry.qualified_name,
                            entry.manifest_id,
                            entry.git_sha,
                            entry.applied_at,
                            entry.run_id,
                            entry.outcome,
                            entry.ddl_sha256,
                            _json_components(entry),
                            _json_resources(entry),
                        ),
                    )
                    return max(cursor.rowcount or 0, 0)
                finally:
                    cursor.close()
        except Exception as exc:
            raise _port_error(exc) from exc

    def _dict_rows(self, sql: str) -> tuple[dict[str, Any], ...]:
        try:
            with self._lock:
                cursor = self._connection.cursor(DictCursor)
                try:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    if any(not isinstance(row, dict) for row in rows):
                        raise SnowflakePortError("dictionary cursor returned a positional row")
                    dictionaries = cast(tuple[dict[Any, Any], ...], rows)
                    return tuple({str(key).lower(): value for key, value in row.items()} for row in dictionaries)
                finally:
                    cursor.close()
        except Exception as exc:
            raise _port_error(exc) from exc


def _port_error(exc: Exception) -> SnowflakePortError:
    return SnowflakePortError(
        str(exc),
        sqlstate=getattr(exc, "sqlstate", None),
        errno=getattr(exc, "errno", None),
    )


def _object_type(value: str) -> str:
    normalized = " ".join(value.upper().split())
    if normalized not in OBJECT_TYPES:
        raise SnowflakePortError(f"unsupported Snowflake object type {value!r}")
    return normalized


def _identifier_matches(value: object, expected: str) -> bool:
    return str(value or "").casefold() == expected.casefold()


def _staged_file_name_matches(value: object, stage_path: str) -> bool:
    expected_stage, expected_relative = stage_path[1:].split("/", 1)
    returned = str(value or "")
    if "/" not in returned:
        return False
    returned_stage, returned_relative = returned.split("/", 1)
    if returned_relative != expected_relative:
        return False
    stage_name = QualifiedName.parse(expected_stage).name.folded
    return returned_stage.casefold() in {expected_stage.casefold(), stage_name.casefold()}


def _validated_stage_path(value: str) -> str:
    message = "stage path must use safe segments and a safe basename under @<db>.<schema>.<stage>"
    if not value.startswith("@") or any(character.isspace() or ord(character) < 32 for character in value):
        raise SnowflakePortError(message)
    if any(character in value for character in ("\\", "'", '"', "*", "?", "[", "]", "{", "}")):
        raise SnowflakePortError(message)
    try:
        stage, path = value[1:].split("/", 1)
        QualifiedName.parse(stage)
    except ValueError:
        raise SnowflakePortError(message) from None
    parts = path.split("/")
    if not parts or any(not part or part in (".", "..") for part in parts):
        raise SnowflakePortError(message)
    safe = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-$")
    if any(any(character not in safe for character in part) for part in parts):
        raise SnowflakePortError(message)
    if PurePosixPath(path).is_absolute():
        raise SnowflakePortError(message)
    return value


def _json_components(entry: AppliedEntry) -> str:
    return json.dumps(dict(entry.component_fingerprints), sort_keys=True, separators=(",", ":"))


def _json_resources(entry: AppliedEntry) -> str:
    return json.dumps(
        [
            {
                "object_type": resource.object_type,
                "qualified_name": resource.qualified_name,
                "status": resource.status.value,
            }
            for resource in entry.applied_resources
        ],
        sort_keys=True,
        separators=(",", ":"),
    )


def _component_fingerprints(value: object) -> tuple[tuple[str, str], ...]:
    parsed = _variant_value(value, {})
    if not isinstance(parsed, dict):
        raise SnowflakePortError("state COMPONENT_FINGERPRINTS must be an object")
    return tuple(sorted((str(key), str(item)) for key, item in parsed.items()))


def _physical_resources(value: object) -> tuple[AppliedResource, ...]:
    parsed = _variant_value(value, [])
    if not isinstance(parsed, list):
        raise SnowflakePortError("state PHYSICAL_RESOURCES must be an array")
    resources: list[AppliedResource] = []
    for item in parsed:
        if not isinstance(item, dict):
            raise SnowflakePortError("state physical resource must be an object")
        resources.append(AppliedResource.from_dict(item))
    return tuple(resources)


def _variant_value(value: object, default: object) -> object:
    if value is None:
        return default
    if isinstance(value, str):
        return json.loads(value)
    return value
