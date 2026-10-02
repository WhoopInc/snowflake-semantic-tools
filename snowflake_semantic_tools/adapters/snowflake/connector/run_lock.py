"""The run lock: one row per target in the lock table beside the state table.

A claim reads the target's row, then claims it with one compare-and-set MERGE: a free lock is
inserted, and an expired one replaced only while the row still names the run that was read.
The target's rows are then read back, and the claim holds the lock only if its row is the
only one: a standard table does not enforce the key, so two racing inserts could both land,
and then both withdraw. Expiry is computed and judged by Snowflake's CURRENT_TIMESTAMP, so
the clocks of the machines that contend never matter. Every value is bound.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.adapters.snowflake.connector.session import (
    Session,
    _as_port_errors,
    _execute,
    _require_ok,
)
from snowflake_semantic_tools.adapters.snowflake.connector.state_table import TABLE_KIND, _state_timestamp
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.sql import Sql, qname, sql
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, RunLock, lock_table_for


def _create_sql(table: Sql) -> Sql:
    return sql(
        "CREATE {kind} IF NOT EXISTS {table} (TARGET_NAME VARCHAR NOT NULL, RUN_ID VARCHAR NOT NULL, "
        "OWNER VARCHAR, HOST VARCHAR, ACQUIRED_AT TIMESTAMP_TZ NOT NULL, EXPIRES_AT TIMESTAMP_TZ NOT NULL, "
        "PRIMARY KEY (TARGET_NAME))",
        kind=TABLE_KIND,
        table=table,
    )


def _read_sql(table: Sql) -> Sql:
    return sql(
        "SELECT RUN_ID, OWNER, HOST, ACQUIRED_AT, EXPIRES_AT, EXPIRES_AT <= CURRENT_TIMESTAMP() AS EXPIRED "
        "FROM {table} WHERE TARGET_NAME = %s ORDER BY ACQUIRED_AT, RUN_ID",
        table=table,
    )


def _claim_sql(table: Sql) -> Sql:
    """Insert a free lock, or replace an expired one still held by the expected run.

    Binds the target, run id, owner, host, ttl seconds, and the run the claim expects to replace:
    NULL for a free lock, which then never matches a row.
    """
    return sql(
        "MERGE INTO {table} AS held USING (SELECT %s AS TARGET_NAME, %s AS RUN_ID, %s AS OWNER, %s AS HOST, "
        "%s AS TTL_SECONDS, %s AS EXPECTED_RUN_ID) AS claim ON held.TARGET_NAME = claim.TARGET_NAME "
        "WHEN MATCHED AND held.RUN_ID = claim.EXPECTED_RUN_ID AND held.EXPIRES_AT <= CURRENT_TIMESTAMP() "
        "THEN UPDATE SET RUN_ID = claim.RUN_ID, OWNER = claim.OWNER, HOST = claim.HOST, "
        "ACQUIRED_AT = CURRENT_TIMESTAMP(), EXPIRES_AT = DATEADD(SECOND, claim.TTL_SECONDS, CURRENT_TIMESTAMP()) "
        "WHEN NOT MATCHED THEN INSERT (TARGET_NAME, RUN_ID, OWNER, HOST, ACQUIRED_AT, EXPIRES_AT) VALUES "
        "(claim.TARGET_NAME, claim.RUN_ID, claim.OWNER, claim.HOST, CURRENT_TIMESTAMP(), "
        "DATEADD(SECOND, claim.TTL_SECONDS, CURRENT_TIMESTAMP()))",
        table=table,
    )


def _extend_sql(table: Sql) -> Sql:
    return sql(
        "UPDATE {table} SET EXPIRES_AT = DATEADD(SECOND, %s, CURRENT_TIMESTAMP()) "
        "WHERE TARGET_NAME = %s AND RUN_ID = %s",
        table=table,
    )


def _release_sql(table: Sql) -> Sql:
    return sql("DELETE FROM {table} WHERE TARGET_NAME = %s AND RUN_ID = %s", table=table)


class RunLockMethods(Session, StatePort):
    """Keep the run lock table: taking the lock creates it first, so a first apply finds it."""

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        table = self._ensure_lock_table(state_table)
        held = self._lock_rows(table, target_name)
        current = held[0] if held else None
        if current is not None and not (current.expired and break_stale):
            return LockAcquisition(False, current)
        self._rowcount(
            _claim_sql(table),
            (
                target_name,
                claim.run_id,
                claim.owner,
                claim.host,
                claim.ttl_seconds,
                current.run_id if current is not None else None,
            ),
        )
        after = self._lock_rows(table, target_name)
        if [row.run_id for row in after] == [claim.run_id]:
            return LockAcquisition(True, current, broke_stale=current is not None)
        if any(row.run_id == claim.run_id for row in after):
            # Two claims inserted a row each; neither holds the lock, so this one withdraws.
            self._rowcount(_release_sql(table), (target_name, claim.run_id))
        rival = next((row for row in after if row.run_id != claim.run_id), current)
        return LockAcquisition(False, rival)

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, claim: LockClaim) -> bool:
        table = qname(lock_table_for(state_table))
        return self._rowcount(_extend_sql(table), (claim.ttl_seconds, target_name, claim.run_id)) > 0

    def release_run_lock(self, state_table: QualifiedName, target_name: str, run_id: str) -> None:
        self._rowcount(_release_sql(qname(lock_table_for(state_table))), (target_name, run_id))

    def _ensure_lock_table(self, state_table: QualifiedName) -> Sql:
        """Create the lock table beside `state_table` unless it exists; return its name as SQL."""
        table = qname(lock_table_for(state_table))
        _require_ok(self.execute_script((_create_sql(table),)), "lock table creation failed")
        return table

    def _lock_rows(self, table: Sql, target_name: str) -> tuple[RunLock, ...]:
        """Read the target's lock rows, oldest first: one when the lock is held, none when it is free."""
        result = self.query(_read_sql(table), (target_name,))
        return tuple(
            RunLock(
                run_id=str(row[0]),
                owner=str(row[1] or ""),
                host=str(row[2] or ""),
                acquired_at=_state_timestamp(row[3]),
                expires_at=_state_timestamp(row[4]),
                expired=bool(row[5]),
            )
            for row in result.rows
        )

    def _rowcount(self, statement: Sql, params: Sequence[object]) -> int:
        """Run one statement with `params` bound, and return the rows it changed."""
        with _as_port_errors(), self._cursor() as cursor:
            _execute(cursor, statement, params)
            return max(cursor.rowcount or 0, 0)
