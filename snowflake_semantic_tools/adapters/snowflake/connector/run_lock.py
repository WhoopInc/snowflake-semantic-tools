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
release and the fenced state write all serialize this way, and none waits for anything
else, so they cannot deadlock: the state write takes the lock table before the state table.

A claim then reads the target's row and decides: a free lock is inserted, an expired one is
replaced only with `break_stale`, and a live one is refused with a ROLLBACK that wrote
nothing. A winning claim is issued the next generation, recorded on its row and on the mutex
row, and returned as its `LockFence`. Expiry is computed and judged by Snowflake's
CURRENT_TIMESTAMP, so the clocks of the machines that contend never matter. Every value is
bound. A verification, which a run makes before each write, only reads the target's row, in no
transaction: it writes nothing, so it never queues behind a claim.

A hybrid table (`state.+table_kind: hybrid`) has row locks and `SELECT ... FOR UPDATE`, so
it needs no mutex row. Each target's row is seeded once, free, by an autocommitted INSERT
before any transaction -- measured, an INSERT of a key a rival transaction holds uncommitted
waits until the statement timeout, so no transaction ever inserts it. Every operation on a
target's lock is then one transaction whose first statement is `SELECT ... FOR UPDATE` of that
row (`_row_transaction`): it either locks the row, so no rival transaction on the target can
change it before this one commits, or fails at once because a rival holds it, and is asked
again a moment later. What it read is therefore current, and the claim, extension or release
that follows is a plain UPDATE of the locked row. A release marks the row free instead of
deleting it, so the target's GENERATION only ever grows: the row is the fence's whole history,
and a claim issues the row's generation plus one. Transactions on different targets lock
different rows and never wait on each other.

Contention on a standard table is per table, not per target. Every lock transaction writes the one mutex row, so
claims for different targets that share a lock table queue behind each other by design; and
(1) is coarser than a row lock even without it: measured on a standard table, an UPDATE of
one existing row waits for an uncommitted UPDATE of a different row until that transaction
commits (an INSERT does not wait). What keeps the queue short is that each transaction is
only its few bound statements, committed as its block exits: nothing slow -- no client-side
work, no statement outside the lock table and, for the state write, the state table -- runs
while it holds the lock.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.snowflake.connector.lock_table import (
    FREE,
    MUTEX_TARGET,
    LockTableMethods,
    expired_sql,
    fence_sql,
    generation,
    lock_table_sql,
    records_fence,
)
from snowflake_semantic_tools.adapters.snowflake.connector.session import Transaction
from snowflake_semantic_tools.adapters.snowflake.connector.state_table import _state_timestamp
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.sql import Sql, sql
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, LockFence, RunLock, lock_table_for


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


def _take_sql(table: Sql) -> Sql:
    """Make a locked hybrid row a claim's; binds run id, owner, host, ttl seconds, generation, target."""
    return sql(
        "UPDATE {table} SET RUN_ID = %s, OWNER = %s, HOST = %s, ACQUIRED_AT = CURRENT_TIMESTAMP(), "
        "EXPIRES_AT = DATEADD(SECOND, %s, CURRENT_TIMESTAMP()), GENERATION = %s WHERE TARGET_NAME = %s",
        table=table,
    )


def _free_sql(table: Sql) -> Sql:
    """Mark a hybrid row free, keeping its generation, while the fence holds it.

    Binds the target, the run id and the generation.
    """
    return sql(
        "UPDATE {table} SET RUN_ID = '', OWNER = NULL, HOST = NULL, EXPIRES_AT = CURRENT_TIMESTAMP() "
        "WHERE TARGET_NAME = %s AND RUN_ID = %s AND COALESCE(GENERATION, 0) = %s",
        table=table,
    )


class RunLockMethods(LockTableMethods, StatePort):
    """Keep the run lock table: taking the lock creates it when absent, so a first apply finds it."""

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
        if self._hybrid:
            return self._acquire_row(state_table, target_name, claim, break_stale=break_stale)
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

    def _acquire_row(
        self, state_table: QualifiedName, target_name: str, claim: LockClaim, *, break_stale: bool
    ) -> LockAcquisition:
        """Claim a hybrid table's lock: lock the target's seeded row, then take it if it is free.

        A free row, or an expired one with `break_stale`, is taken with the row's generation
        plus one; a live one, or an expired one without `break_stale`, is refused with a
        ROLLBACK that wrote nothing.
        """
        table = self._ensure_lock_table(state_table, target_name)
        with self._row_transaction(table, target_name) as (transaction, row):
            if row is None:
                raise SnowflakePortError(f"the run lock table has no row for target {target_name!r}")
            # The row is locked, so this read is current: no rival can change it before COMMIT.
            expired = bool(transaction.rows(expired_sql(table), (target_name,))[0][0])
            current = _held(row, expired=expired)
            if current is not None and (not current.expired or not break_stale):
                transaction.rollback()
                return LockAcquisition(False, current)
            issued = generation(row[5]) + 1
            transaction.run(
                _take_sql(table),
                (claim.run_id, claim.owner, claim.host, claim.ttl_seconds, issued, target_name),
            )
        fence = LockFence(claim.run_id, issued)
        return LockAcquisition(True, current, broke_stale=current is not None, fence=fence)

    def extend_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence, ttl_seconds: int) -> bool:
        table = lock_table_sql(state_table)
        if self._hybrid:
            with self._row_transaction(table, target_name) as (transaction, _row):
                return (
                    transaction.run(_extend_sql(table), (ttl_seconds, target_name, fence.run_id, fence.generation)) > 0
                )
        with self._lock_transaction(table) as transaction:
            return transaction.run(_extend_sql(table), (ttl_seconds, target_name, fence.run_id, fence.generation)) > 0

    def verify_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence) -> bool:
        # A plain read, outside any lock transaction: it must not queue behind a rival's claim,
        # and a takeover's DELETE and INSERT commit together, so it never sees a lock between.
        rows = self.query(fence_sql(lock_table_sql(state_table)), (target_name,)).rows
        return records_fence(rows, fence)

    def release_run_lock(self, state_table: QualifiedName, target_name: str, fence: LockFence) -> None:
        table = lock_table_sql(state_table)
        if self._hybrid:
            with self._row_transaction(table, target_name) as (transaction, _row):
                transaction.run(_free_sql(table), (target_name, fence.run_id, fence.generation))
            return
        with self._lock_transaction(table) as transaction:
            transaction.run(_release_sql(table), (target_name, fence.run_id, fence.generation))

    def force_release_run_lock(
        self, state_table: QualifiedName, target_name: str, run_id: str, *, host: str | None = None
    ) -> RunLock | None:
        lock_table = lock_table_for(state_table)
        if self.kind_of(lock_table) is None:
            return None
        self.check_table_kind(lock_table)
        table = lock_table_sql(state_table)
        if self._hybrid:
            with self._row_transaction(table, target_name) as (transaction, row):
                expired = row is not None and bool(transaction.rows(expired_sql(table), (target_name,))[0][0])
                current = None if row is None else _held(row, expired=expired)
                if row is None or not _releases(current, run_id, host):
                    transaction.rollback()
                    return None
                transaction.run(_free_sql(table), (target_name, run_id, generation(row[5])))
            return current
        with self._lock_transaction(table) as transaction:
            current = next(
                (lock for lock in _lock_rows(transaction, table, target_name) if lock.run_id == run_id), None
            )
            issued = [
                generation(row[1]) for row in transaction.rows(fence_sql(table), (target_name,)) if row[0] == run_id
            ]
            if not _releases(current, run_id, host) or not issued:
                transaction.rollback()
                return None
            transaction.run(_release_sql(table), (target_name, run_id, issued[0]))
        return current


def _releases(lock: RunLock | None, run_id: str, host: str | None) -> bool:
    """Whether a forced release may free `lock`: `run_id` holds it, on `host` when one is named."""
    return lock is not None and lock.run_id == run_id and (host is None or lock.host == host)


def _held(row: tuple[object, ...], *, expired: bool) -> RunLock | None:
    """Return the lock a hybrid row records, as `row_lock_sql` reads it; None when the row is free."""
    if str(row[0] or "") == FREE:
        return None
    return RunLock(
        run_id=str(row[0]),
        owner=str(row[1] or ""),
        host=str(row[2] or ""),
        acquired_at=_state_timestamp(row[3]),
        expires_at=_state_timestamp(row[4]),
        expired=expired,
    )


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
