"""Production Snowflake connector adapter."""

from __future__ import annotations

import contextlib
import json
import re
import sys
from datetime import datetime, timezone
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
from ...domain.ports.snowflake import (
    ExtensionObservation,
    ExtensionVersion,
    SnowflakePortError,
    StagedFileMetadata,
    StageObservation,
)
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
        "CORTEX EXTENSION",
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
            # Browser SSO prints its prompts to stdout, which `--output json` reserves
            # for exactly one envelope; the prompts still reach the user on stderr.
            with contextlib.redirect_stdout(sys.stderr):
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
                        # SHOW FUNCTIONS and SHOW PROCEDURES name the database `catalog_name`.
                        database_name=str(row.get("database_name") or row.get("catalog_name") or scope.database.folded),
                        schema_name=str(row.get("schema_name") or scope.schema.folded),
                        owner=str(row.get("owner") or ""),
                        created_on=str(row.get("created_on") or ""),
                        comment=_show_comment(row),
                        object_type=normalized_type,
                    )
                    for row in rows
                    # Both also list every built-in routine in every schema; none is an artifact.
                    if str(row.get("is_builtin") or "").upper() != "Y"
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
                return extract_marker(_show_comment(row))
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
        stage_path = _validated_upload_target(stage_path)
        import os
        import tempfile

        basename = PurePosixPath(stage_path).name
        temp_dir = tempfile.mkdtemp(prefix="sst-upload-")
        local_path = os.path.join(temp_dir, basename)
        try:
            with open(local_path, "wb") as handle:
                handle.write(content)
            # K204: the PUT target is always a directory, so it ends in a separator.
            destination = stage_path.rsplit("/", 1)[0] + "/"
            if destination.startswith("snow://"):
                destination = f"'{destination}'"
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

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        pattern = qualified_name.name.folded.replace("'", "''")
        rows = self._dict_rows(
            f"SHOW STAGES LIKE '{pattern}' IN SCHEMA {qualified_name.database.sql}.{qualified_name.schema.sql}"
        )
        match = next(
            (row for row in rows if _identifier_matches(row.get("name"), qualified_name.name.folded)),
            None,
        )
        return str(match.get("type") or "") if match is not None else None

    def list_location(self, location: str) -> tuple[str, ...]:
        """File paths below a stage prefix or an extension version, relative to it."""
        location = _validated_location(location)
        rows = self._dict_rows(f"LIST '{location}'")
        if location.startswith("snow://"):
            marker = location[location.index("/versions/") :]
            names = (str(row.get("name") or "") for row in rows)
            return tuple(sorted(name[len(marker) :] for name in names if name.casefold().startswith(marker.casefold())))
        prefix = location[1:].split("/", 1)[1]
        relative: list[str] = []
        for row in rows:
            returned = str(row.get("name") or "")
            if "/" not in returned:
                continue
            path = returned.split("/", 1)[1]
            if path.startswith(prefix):
                relative.append(path[len(prefix) :])
        return tuple(sorted(relative))

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        pattern = qualified_name.name.folded.replace("'", "''")
        rows = self._dict_rows(
            f"SHOW CORTEX EXTENSIONS LIKE '{pattern}' IN SCHEMA "
            f"{qualified_name.database.sql}.{qualified_name.schema.sql}"
        )
        match = next(
            (row for row in rows if _identifier_matches(row.get("name"), qualified_name.name.folded)),
            None,
        )
        if match is None:
            return None
        return ExtensionObservation(
            qualified_name=qualified_name,
            extension_type=str(match.get("type") or "").upper(),
            comment=str(match["comment"]) if match.get("comment") is not None else None,
            owner=str(match.get("owner") or ""),
            effective_version=_optional_text(match.get("effective_version")),
            latest_certified_version=_optional_text(match.get("latest_certified_version")),
        )

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        rows = self._dict_rows(f"SHOW VERSIONS IN CORTEX EXTENSION {qualified_name.sql}")
        return tuple(
            ExtensionVersion(
                name=str(row.get("name") or "").upper(),
                alias=_optional_text(row.get("alias")),
                location=str(row.get("location_uri") or ""),
                is_default=str(row.get("is_default") or "").casefold() == "true",
                certification_status=_optional_text(row.get("certification_status")),
            )
            for row in rows
            if row.get("name")
        )

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        if not self.object_exists("TABLE", qualified_name):
            return None
        rows = self._dict_rows(f"DESCRIBE TABLE {qualified_name.sql}")
        return tuple((str(row.get("name") or "").upper(), str(row.get("type") or "").upper()) for row in rows)

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        result = self.execute_script((f"CREATE TABLE IF NOT EXISTS {qualified_name.sql} ({PROFILE_REGISTRY_COLUMNS})",))
        if not result.ok:
            raise SnowflakePortError(result.error.message if result.error else "profile registry creation failed")

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        result = self.query(f"SELECT * FROM {registry.sql} WHERE CONFIG_NAME = %s", (name,))
        rows = [dict(zip((column.upper() for column in result.columns), row)) for row in result.rows]
        if len(rows) > 1:
            raise SnowflakePortError(f"profile registry {registry.sql} holds {len(rows)} rows named {name!r}")
        return rows[0] if rows else None

    def merge_profile_row(
        self,
        registry: QualifiedName,
        row: Mapping[str, object],
        *,
        expected_version: str | None,
    ) -> int:
        """One MERGE on CONFIG_NAME, guarded by the VERSION the plan observed."""
        params: dict[str, object] = {
            "name": row["CONFIG_NAME"],
            "description": row.get("DESCRIPTION"),
            "owner_team": row.get("OWNER_TEAM"),
            "version": row["VERSION"],
            "expected": expected_version,
            **{column.lower(): _variant_parameter(row.get(column)) for column in PROFILE_VARIANT_COLUMNS},
        }
        updates = ", ".join(f"{column} = PARSE_JSON(%({column.lower()})s)" for column in PROFILE_VARIANT_COLUMNS)
        inserted = ", ".join(PROFILE_VARIANT_COLUMNS)
        values = ", ".join(f"PARSE_JSON(%({column.lower()})s)" for column in PROFILE_VARIANT_COLUMNS)
        result = self.query(
            f"MERGE INTO {registry.sql} AS t USING (SELECT %(name)s AS CONFIG_NAME) AS s "
            "ON t.CONFIG_NAME = s.CONFIG_NAME "
            "WHEN MATCHED AND t.VERSION = %(expected)s THEN UPDATE SET "
            f"DESCRIPTION = %(description)s, OWNER_TEAM = %(owner_team)s, VERSION = %(version)s, {updates}, "
            "ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP() "
            f"WHEN NOT MATCHED THEN INSERT (CONFIG_NAME, DESCRIPTION, OWNER_TEAM, VERSION, {inserted}, ACTIVE) "
            f"VALUES (%(name)s, %(description)s, %(owner_team)s, %(version)s, {values}, TRUE)",
            params,
        )
        return _affected(result)

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        result = self.query(
            f"UPDATE {registry.sql} SET ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() "
            "WHERE CONFIG_NAME = %s AND VERSION = %s AND ACTIVE = TRUE",
            (name, expected_version),
        )
        return _affected(result)

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        """The exact query CoCo Desktop runs against its registry."""
        result = self.query(f"SELECT * FROM {registry.sql} WHERE active = TRUE ORDER BY config_name")
        return tuple(dict(zip((column.upper() for column in result.columns), row)) for row in result.rows)

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
                    applied_at=_state_timestamp(row[5]),
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


def _validated_stage_path(value: str, *, directory: bool = False) -> str:
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
    if directory:
        if not path.endswith("/"):
            raise SnowflakePortError(message)
        path = path[:-1]
    parts = path.split("/")
    if not parts or any(not part or part in (".", "..") for part in parts):
        raise SnowflakePortError(message)
    if any(any(character not in _SAFE_SEGMENT for character in part) for part in parts):
        raise SnowflakePortError(message)
    if PurePosixPath(path).is_absolute():
        raise SnowflakePortError(message)
    return value


_SAFE_SEGMENT = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-$")
_EXTENSION_URI = re.compile(
    r"snow://cortex_extension/(?P<name>[A-Za-z0-9_$.]+)/versions/(?P<version>version\$[0-9]+|live)/(?P<path>.*)",
    re.IGNORECASE,
)


def _extension_uri(value: str, *, directory: bool) -> str:
    message = "extension path must be snow://cortex_extension/<db>.<schema>.<name>/versions/<version>/<safe path>"
    match = _EXTENSION_URI.fullmatch(value)
    if match is None:
        raise SnowflakePortError(message)
    try:
        QualifiedName.parse(match.group("name"))
    except ValueError:
        raise SnowflakePortError(message) from None
    path = match.group("path")
    if directory:
        if path:
            raise SnowflakePortError(message)
        return value
    parts = path.split("/")
    if any(
        not part or part in (".", "..") or any(character not in _SAFE_SEGMENT for character in part) for part in parts
    ):
        raise SnowflakePortError(message)
    return value


def _validated_upload_target(value: str) -> str:
    if value.startswith("snow://"):
        return _extension_uri(value, directory=False)
    return _validated_stage_path(value)


def _validated_location(value: str) -> str:
    if value.startswith("snow://"):
        return _extension_uri(value, directory=True)
    return _validated_stage_path(value, directory=True)


def _show_comment(row: Mapping[str, object]) -> str | None:
    """An object's COMMENT from a SHOW row; routines report it as `description`."""
    value = row["comment"] if "comment" in row else row.get("description")
    return str(value) if value is not None else None


def _state_timestamp(value: object) -> str:
    """APPLIED_AT in the clock's own form, so a written entry reads back equal.

    The column is TIMESTAMP_TZ and the driver returns a datetime, whose `str()`
    (`2026-09-29 11:56:32.077209+00:00`) never equals the ISO text SST wrote
    (`2026-09-29T11:56:32.077209Z`); the cache then disagreed after every apply.
    """
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _optional_text(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


# The 18 columns of CoCo Desktop's profile registry, as the production table
# declares them. Desktop reads 12; the rest belong to other writers and are
# never written by SST on update.
PROFILE_REGISTRY_COLUMNS = (
    "CONFIG_NAME VARCHAR NOT NULL PRIMARY KEY, DESCRIPTION VARCHAR, OWNER_TEAM VARCHAR, "
    "SKILL_REPOS VARIANT, MCP_SERVERS VARIANT, COMMAND_REPOS VARIANT, ENV_VARS VARIANT, "
    "SETTINGS_OVERRIDES VARIANT, VERSION VARCHAR DEFAULT '1.0', ACTIVE BOOLEAN DEFAULT TRUE, "
    "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), "
    "SYSTEM_PROMPT_REPO VARIANT, HOOKS VARIANT, SCRIPTS VARIANT, PLUGINS VARIANT, ALLOWED_ROLES VARIANT, "
    "PERMISSIONS VARIANT"
)
PROFILE_VARIANT_COLUMNS = (
    "SKILL_REPOS",
    "SYSTEM_PROMPT_REPO",
    "MCP_SERVERS",
    "HOOKS",
    "PLUGINS",
    "COMMAND_REPOS",
    "ENV_VARS",
    "SETTINGS_OVERRIDES",
)


def _variant_parameter(value: object) -> str | None:
    """JSON text for PARSE_JSON; None binds SQL NULL rather than a JSON null."""
    return None if value is None else json.dumps(value, sort_keys=True, separators=(",", ":"))


def _affected(result: QueryResult) -> int:
    return sum(
        int(value) for row in result.rows for value in row if isinstance(value, int) and not isinstance(value, bool)
    )


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
