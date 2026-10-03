"""The state table: one row per target and artifact key, recording what apply published there.

One column table drives every statement the table sees: the CREATE, the ALTERs that migrate
an older table, the SELECT that reads a target's entries, and the MERGE that writes one.
COMPONENT_FINGERPRINTS, PHYSICAL_RESOURCES and STATE_MANIFEST_ID came later, so a table
created before them reads the first two as NULL and records no manifest until
`ensure_state_table` adds them with ADD COLUMN IF NOT EXISTS, which is safe to repeat.

A run writes only the entries it changed: one MERGE per changed entry and one DELETE per
retired key, then one UPDATE that stamps the run's manifest on every row of the target, all
in one transaction on one cursor with every value bound. That transaction is a lock
transaction: it first takes the lock table's mutex, as `run_lock` explains, and writes only
if the writer's fence still holds the target's lock, so a run whose lock was broken can never
overwrite what the run that broke it recorded.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import NamedTuple

from snowflake_semantic_tools.adapters.snowflake.connector.lock_table import (
    TABLE_KIND,
    LockTableMethods,
    fence_holds,
    lock_table_sql,
)
from snowflake_semantic_tools.adapters.snowflake.connector.session import (
    _json_text,
    _require_ok,
    _variant_value,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.sql import Sql, ident, join, qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry, AppliedResource, pairs_from_json, pairs_to_json
from snowflake_semantic_tools.domain.state.lock import LockFence, StateWrite


class _StateColumn(NamedTuple):
    """One state-table column: its DDL type, and the expression a write binds its value through."""

    name: str
    ddl: Sql
    bind: Sql = sql("%s")
    key: bool = False
    added: bool = False

    @property
    def identifier(self) -> Sql:
        """The column's name as SQL."""
        return ident(Identifier(self.name))


# In table order, which is the order a write binds values in and a read decodes them in.
_COLUMNS = (
    _StateColumn("TARGET_NAME", sql("VARCHAR NOT NULL"), key=True),
    _StateColumn("ARTIFACT_KEY", sql("VARCHAR NOT NULL"), key=True),
    _StateColumn("FINGERPRINT", sql("VARCHAR(64) NOT NULL")),
    _StateColumn("QUALIFIED_NAME", sql("VARCHAR NOT NULL")),
    _StateColumn("MANIFEST_ID", sql("VARCHAR(64) NOT NULL")),
    _StateColumn("GIT_SHA", sql("VARCHAR")),
    _StateColumn("APPLIED_AT", sql("TIMESTAMP_TZ"), sql("TO_TIMESTAMP_TZ(%s)")),
    _StateColumn("RUN_ID", sql("VARCHAR")),
    _StateColumn("OUTCOME", sql("VARCHAR")),
    _StateColumn("DDL_SHA256", sql("VARCHAR(64)")),
    _StateColumn("COMPONENT_FINGERPRINTS", sql("OBJECT"), sql("PARSE_JSON(%s)"), added=True),
    _StateColumn("PHYSICAL_RESOURCES", sql("ARRAY"), sql("PARSE_JSON(%s)"), added=True),
)
STATE_COLUMNS = tuple(column.name for column in _COLUMNS)
# The manifest of the run that last wrote the target, on every one of its rows; not part of an entry.
_STATE_MANIFEST = _StateColumn("STATE_MANIFEST_ID", sql("VARCHAR(64)"), added=True)


class StateTableMethods(LockTableMethods, StatePort, CatalogPort):
    """Keep the state table: reads never change it, and every write creates and migrates it first.

    `CatalogPort` is a base for the one lookup a read starts with, whether the table exists,
    which the assembled connector's catalog role answers.
    """

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None:
        if not self.object_exists("TABLE", state_table):
            return MappingProxyType({})
        try:
            result = self.query(_select_sql(state_table, self._column_names(state_table)), (target_name,))
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

    def read_state_manifest(self, state_table: QualifiedName, target_name: str) -> str | None:
        if not self.object_exists("TABLE", state_table):
            return None
        if _STATE_MANIFEST.name not in self._column_names(state_table):
            return None
        result = self.query(
            sql(
                "SELECT DISTINCT {column} FROM {table} WHERE TARGET_NAME = %s",
                column=_STATE_MANIFEST.identifier,
                table=qname(state_table),
            ),
            (target_name,),
        )
        recorded = {str(row[0]) for row in result.rows if row[0]}
        return next(iter(recorded)) if len(recorded) == 1 else None

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        additions = tuple(
            sql(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ddl}",
                table=qname(state_table),
                column=column.identifier,
                ddl=column.ddl,
            )
            for column in (*_COLUMNS, _STATE_MANIFEST)
            if column.added
        )
        _require_ok(self.execute_script((_create_sql(state_table), *additions)), "state table creation failed")

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        write: StateWrite,
        fence: LockFence,
    ) -> bool:
        self.ensure_state_table(state_table)
        table = qname(state_table)
        merge = _merge_sql(state_table)
        delete = sql("DELETE FROM {table} WHERE TARGET_NAME = %s AND ARTIFACT_KEY = %s", table=table)
        stamp = sql(
            "UPDATE {table} SET {column} = %s WHERE TARGET_NAME = %s", table=table, column=_STATE_MANIFEST.identifier
        )
        lock = lock_table_sql(state_table)
        with self._lock_transaction(lock) as transaction:
            if not fence_holds(transaction, lock, target_name, fence):
                transaction.rollback()
                return False
            for key, entry in sorted(write.upserts.items()):
                transaction.run(merge, _state_values(target_name, key, entry))
            for key in write.deletes:
                transaction.run(delete, (target_name, key))
            transaction.run(stamp, (manifest_id, target_name))
        return True

    def _column_names(self, table: QualifiedName) -> set[str]:
        """Return the names of the columns DESCRIBE TABLE lists for a table, uppercased."""
        return {
            str(row.get("name") or "").upper()
            for row in self._dict_rows(sql("DESCRIBE TABLE {table}", table=qname(table)))
        }


def _create_sql(table: QualifiedName) -> Sql:
    columns = join(
        ", ", (sql("{name} {ddl}", name=column.identifier, ddl=column.ddl) for column in (*_COLUMNS, _STATE_MANIFEST))
    )
    key = join(", ", (column.identifier for column in _COLUMNS if column.key))
    return sql(
        "CREATE {kind} IF NOT EXISTS {table} ({columns}, PRIMARY KEY ({key}))",
        kind=TABLE_KIND,
        table=qname(table),
        columns=columns,
        key=key,
    )


def _select_sql(table: QualifiedName, present: set[str]) -> Sql:
    """Select a target's entries, every column but TARGET_NAME in table order.

    A table that lacks either added column selects NULL for both, so a half-migrated table
    reads as an unmigrated one.
    """
    migrated = all(column.name in present for column in _COLUMNS if column.added)
    selected = join(
        ", ",
        (
            column.identifier if migrated or not column.added else sql("NULL {name}", name=column.identifier)
            for column in _COLUMNS
            if column.name != "TARGET_NAME"
        ),
    )
    return sql("SELECT {selected} FROM {table} WHERE TARGET_NAME = %s", selected=selected, table=qname(table))


def _column_list() -> Sql:
    return join(", ", (column.identifier for column in _COLUMNS))


def _merge_sql(table: QualifiedName) -> Sql:
    """Insert or replace one entry: matched on the key columns, every other column updated."""
    source = join(", ", (sql("{bind} {name}", bind=column.bind, name=column.identifier) for column in _COLUMNS))
    matched = join(
        " AND ",
        (sql("target.{name} = source.{name}", name=column.identifier) for column in _COLUMNS if column.key),
    )
    updates = join(
        ", ",
        (sql("{name}=source.{name}", name=column.identifier) for column in _COLUMNS if not column.key),
    )
    values = join(", ", (sql("source.{name}", name=column.identifier) for column in _COLUMNS))
    return sql(
        "MERGE INTO {table} AS target USING (SELECT {source}) AS source ON {matched} "
        "WHEN MATCHED THEN UPDATE SET {updates} "
        "WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})",
        table=qname(table),
        source=source,
        matched=matched,
        updates=updates,
        columns=_column_list(),
        values=values,
    )


def _state_values(target_name: str, artifact_key: str, entry: AppliedEntry) -> tuple[object, ...]:
    """Return the values a write binds for one entry, in `STATE_COLUMNS` order."""
    return (
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
    )


def _state_timestamp(value: object) -> str:
    """APPLIED_AT in the clock's own form, so a written entry reads back equal.

    The column is TIMESTAMP_TZ and the driver returns a datetime, whose `str()`
    (`2026-09-29 11:56:32.077209+00:00`) never equals the ISO text SST wrote
    (`2026-09-29T11:56:32.077209Z`); the cache then disagreed after every apply.
    """
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(value)


def _json_components(entry: AppliedEntry) -> str:
    return _json_text(pairs_to_json(entry.component_fingerprints))


def _json_resources(entry: AppliedEntry) -> str:
    return _json_text([resource.as_dict() for resource in entry.applied_resources])


def _component_fingerprints(value: object) -> tuple[tuple[str, str], ...]:
    parsed = _variant_value(value, {})
    if not isinstance(parsed, dict):
        raise SnowflakePortError("state COMPONENT_FINGERPRINTS must be an object")
    return pairs_from_json(parsed)


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
