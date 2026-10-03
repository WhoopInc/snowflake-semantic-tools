"""The run lock: one row per target in the lock table beside the state table, taken atomically.

Snowflake semantics this relies on, for a standard table (Snowflake documentation,
"Transactions": "Resource locking" and "READ COMMITTED isolation level"):

1. UPDATE, DELETE and MERGE take locks that keep other UPDATE, DELETE and MERGE statements on
   the table from running in parallel; a blocked statement waits for the lock, up to
   LOCK_TIMEOUT, rather than failing.
2. Locks a statement takes are held until its transaction commits or rolls back.
3. Under READ COMMITTED, each statement sees the data committed before *it* began, plus its
   own transaction's earlier writes; INSERT is not blocked by those locks.

`SELECT ... FOR UPDATE`, which would lock the rows read, exists for hybrid tables only, and a
compare-and-set MERGE is correct only if a MERGE that waited for a lock re-evaluates its
condition afterwards, which the documentation does not promise. So every operation on the
lock table instead runs as one explicit transaction, on one session, whose FIRST statement
writes the mutex row unconditionally (`_lock_transaction`): it either waits for a rival's
transaction to end, by (1) and (2), or makes the rival wait for this one. Its own result
depends on nothing read, so whether it re-evaluates after waiting does not matter. Every
statement after it begins once no rival transaction is open, so by (3) it reads what the last
one committed, and nothing can change under it before this transaction commits: a rival's
INSERT, which (1) does not block, only ever runs inside such a transaction too. Claim, extend,
release and the fenced state write all serialise this way, and none waits for anything
else, so they cannot deadlock: the state write takes the lock table before the state table.

A claim then reads the target's row and decides: a free lock is inserted, an expired one is
replaced only with `break_stale`, and a live one is refused with a ROLLBACK that wrote
nothing. A winning claim is issued the next generation, recorded on its row and on the mutex
row, and returned as its `LockFence`. Expiry is computed and judged by Snowflake's
CURRENT_TIMESTAMP, so the clocks of the machines that contend never matter. Every value is
bound.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.snowflake.connector.lock_table import (
    MUTEX_TARGET,
    LockTableMethods,
    generation,
    lock_table_sql,
)
from snowflake_semantic_tools.adapters.snowflake.connector.session import Transaction
from snowflake_semantic_tools.adapters.snowflake.connector.state_table import _state_timestamp
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.sql import Sql, sql
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, LockFence, RunLock


def _read_sql(table: Sql) -> Sql:
    return sql(
        "SELECT RUN_ID, OWNER, HOST, ACQUIRED_AT, EXPIRES_AT, EXPIRES_AT <= CURRENT_TIMESTAMP() AS EXPIRED "
        "FROM {table} WHERE TARGET_NAME = %s ORDER BY ACQUIRED_AT, RUN_ID",
        table=table,
    )


def _last_generation_sql(table: Sql) -> Sql:
    return sql("SELECT MAX(GENERATION) FROM {table}", table=table)


def _clear_sql(table: Sql) -> Sql:
    return sql("DELETE FROM {table} WHERE TARGET_NAME = %s", table=table)


def _insert_sql(table: Sql) -> Sql:
    """Insert a claim's row; binds the target, run id, owner, host, ttl seconds, and generation."""
    return sql(
        "INSERT INTO {table} (TARGET_NAME, RUN_ID, OWNER, HOST, ACQUIRED_AT, EXPIRES_AT, GENERATION) "
        "VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP(), DATEADD(SECOND, %s, CURRENT_TIMESTAMP()), %s)",
        table=table,
    )


def _issued_sql(table: Sql) -> Sql:
    """Record on the mutex row the generation just issued, so the next claim issues a later one."""
    return sql("UPDATE {table} SET GENERATION = %s WHERE TARGET_NAME = %s", table=table)


def _extend_sql(table: Sql) -> Sql:
    return sql(
        "UPDATE {table} SET EXPIRES_AT = DATEADD(SECOND, %s, CURRENT_TIMESTAMP()) "
        "WHERE TARGET_NAME = %s AND RUN_ID = %s AND COALESCE(GENERATION, 0) = %s",
        table=table,
    )


def _release_sql(table: Sql) -> Sql:
    return sql(
        "DELETE FROM {table} WHERE TARGET_NAME = %s AND RUN_ID = %s AND COALESCE(GENERATION, 0) = %s",
        table=table,
    )


class RunLockMethods(LockTableMethods, StatePort):
    """Keep the run lock table: taking the lock creates it first, so a first apply finds it."""

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        if target_name == MUTEX_TARGET:
            raise ValueError("a run lock needs a target name")
        table = self._ensure_lock_table(state_table)
        with self._lock_transaction(table) as transaction:
            held = _lock_rows(transaction, table, target_name)
            live = next((row for row in held if not row.expired), None)
            current = held[0] if held else None
            if live is not None or (current is not None and not break_stale):
                transaction.rollback()
                return LockAcquisition(False, live or current)
            issued = generation(transaction.rows(_last_generation_sql(table))[0][0]) + 1
            if held:
                transaction.run(_clear_sql(table), (target_name,))
            transaction.run(
                _insert_sql(table),
                (target_name, claim.run_id, claim.owner, claim.host, claim.ttl_seconds, issued),
            )
            transaction.run(_issued_sql(table), (issued, MUTEX_TARGET))
        fence = LockFence(claim.run_id, issued)
        return LockAcquisition(True, current, broke_stale=current is not None, fence=fence)

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence, ttl_seconds: int) -> bool:
        table = lock_table_sql(state_table)
        with self._lock_transaction(table) as transaction:
            return transaction.run(_extend_sql(table), (ttl_seconds, target_name, fence.run_id, fence.generation)) > 0

    def release_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence) -> None:
        table = lock_table_sql(state_table)
        with self._lock_transaction(table) as transaction:
            transaction.run(_release_sql(table), (target_name, fence.run_id, fence.generation))


def _lock_rows(transaction: Transaction, table: Sql, target_name: str) -> tuple[RunLock, ...]:
    """Read the target's lock rows, oldest first: one when the lock is held, none when it is free.

    A table written before this protocol may hold two rows for one target; each is judged.
    """
    return tuple(
        RunLock(
            run_id=str(row[0]),
            owner=str(row[1] or ""),
            host=str(row[2] or ""),
            acquired_at=_state_timestamp(row[3]),
            expires_at=_state_timestamp(row[4]),
            expired=bool(row[5]),
        )
        for row in transaction.rows(_read_sql(table), (target_name,))
    )
