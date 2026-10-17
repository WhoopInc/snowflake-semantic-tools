"""The lock table's shared ground: its creation, the lock every transaction takes, the fence.

`run_lock` claims, extends and releases the lock; `state_table` writes state only while the
writer's fence holds it. Both open their transaction through `_lock_transaction`, and
`run_lock` explains why that makes them serialise. What the transaction locks first depends
on the kind of table `state.+table_kind` names:

- A standard table's transaction first writes the table's mutex row, which has the empty
  target name, which no target has, and records in GENERATION the last generation the table
  issued. Every transaction on the table queues behind it, whatever its target.
- A hybrid table has no mutex row. Each target's row is seeded once, free (RUN_ID ''), outside
  any transaction, and the transaction first locks that row with `SELECT ... FOR UPDATE`, so
  only transactions on the same target queue; the row keeps the target's own generation.

Every value is bound.
"""

from __future__ import annotations

import contextlib
import re
import time
from collections.abc import Iterator, Sequence

from snowflake_semantic_tools.adapters.snowflake.connector.session import (
    Session,
    Transaction,
    _driver_errors,
    _require_ok,
)
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.diagnostics.signatures import SessionFailure, session_failure
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, ident, qname, sql
from snowflake_semantic_tools.domain.state.lock import HYBRID_TABLES, STANDARD_TABLES, LockFence, lock_table_for

# The target name of the mutex row; a target is always named, so no lock row has it. A free
# hybrid row has it as its RUN_ID.
MUTEX_TARGET = ""
FREE = ""
# Snowflake's numbers for a hybrid table's key: inserted twice (measured: "A primary key already
# exists."), and locked by another transaction ("has locked a key and this lock has not yet
# been released"), which a `SELECT ... FOR UPDATE` reports at once rather than waiting.
DUPLICATE_KEY = 200001
KEY_LOCKED = 200010
# A hybrid table cannot be created in a transient database or schema.
_TRANSIENT_HYBRID = 391412
# How often, and how long apart, a lock transaction asks again for a row a rival holds: a
# rival's transaction is a few bound statements, so a few seconds in all is ample.
_ROW_LOCK_ATTEMPTS = 20
_ROW_LOCK_PAUSE_SECONDS = (0.05, 0.1, 0.2, 0.4)
# Snowflake's number, and its wording, for a column the statement names that the table lacks.
_INVALID_IDENTIFIER = 904
_NO_SUCH_COLUMN = re.compile(r"\binvalid identifier\b", re.IGNORECASE)


def lock_table_sql(state_table: QualifiedName) -> Sql:
    """Return the lock table beside `state_table` as SQL."""
    return qname(lock_table_for(state_table))


def table_kind_sql(kind: str) -> Sql:
    """Return the CREATE keyword a table of `kind` takes: `HYBRID TABLE` for hybrid, else `TABLE`."""
    return sql("HYBRID TABLE") if kind == HYBRID_TABLES else sql("TABLE")


def _create_sql(table: Sql, kind: str) -> Sql:
    return sql(
        "CREATE {kind} IF NOT EXISTS {table} (TARGET_NAME VARCHAR NOT NULL, RUN_ID VARCHAR NOT NULL, "
        "OWNER VARCHAR, HOST VARCHAR, ACQUIRED_AT TIMESTAMP_TZ NOT NULL, EXPIRES_AT TIMESTAMP_TZ NOT NULL, "
        "GENERATION NUMBER, PRIMARY KEY (TARGET_NAME))",
        kind=table_kind_sql(kind),
        table=table,
    )


def _migrate_sql(table: Sql) -> Sql:
    """Add GENERATION to a lock table created before fences; safe to repeat."""
    return sql("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS GENERATION NUMBER", table=table)


def _probe_sql(table: Sql) -> Sql:
    """Read the newest column of one row: it fails unless the table exists with every column."""
    return sql("SELECT GENERATION FROM {table} LIMIT 1", table=table)


def _seed_sql(table: Sql) -> Sql:
    """Insert the mutex row unless it exists; binds the mutex target name twice.

    Two seeds racing may both insert one, which is harmless: the mutex statement writes every
    mutex row, and the generation is read across all rows.
    """
    return sql(
        "MERGE INTO {table} AS held USING (SELECT %s AS TARGET_NAME) AS seed ON held.TARGET_NAME = seed.TARGET_NAME "
        "WHEN NOT MATCHED THEN INSERT (TARGET_NAME, RUN_ID, ACQUIRED_AT, EXPIRES_AT, GENERATION) "
        "VALUES (%s, '', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0)",
        table=table,
    )


def _mutex_sql(table: Sql) -> Sql:
    """Write the mutex row unconditionally: what it writes depends on nothing any statement read."""
    return sql("UPDATE {table} SET ACQUIRED_AT = CURRENT_TIMESTAMP() WHERE TARGET_NAME = %s", table=table)


def _row_seed_sql(table: Sql) -> Sql:
    """Insert a hybrid table's free row for a target; binds the target. A second insert fails."""
    return sql(
        "INSERT INTO {table} (TARGET_NAME, RUN_ID, ACQUIRED_AT, EXPIRES_AT, GENERATION) "
        "VALUES (%s, '', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0)",
        table=table,
    )


def row_lock_sql(table: Sql) -> Sql:
    """Lock a hybrid table's row for a target and read it; binds the target.

    Reads RUN_ID, OWNER, HOST, ACQUIRED_AT, EXPIRES_AT and GENERATION: plain columns only, since
    a FOR UPDATE whose select list calls CURRENT_TIMESTAMP() fails with an internal error
    (000603, measured); `expired_sql` reads whether the row has expired once it is locked.
    """
    return sql(
        "SELECT RUN_ID, OWNER, HOST, ACQUIRED_AT, EXPIRES_AT, GENERATION FROM {table} "
        "WHERE TARGET_NAME = %s FOR UPDATE",
        table=table,
    )


def expired_sql(table: Sql) -> Sql:
    """Read whether a target's lock row has expired, by Snowflake's clock; binds the target."""
    return sql("SELECT EXPIRES_AT <= CURRENT_TIMESTAMP() FROM {table} WHERE TARGET_NAME = %s", table=table)


def kind_sql(table: QualifiedName) -> Sql:
    """Read whether a table is hybrid: `YES` or `NO`, no row when there is no such table."""
    return sql(
        "SELECT IS_HYBRID FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        database=ident(table.database),
    )


def kind_in(rows: Sequence[Sequence[object]]) -> str | None:
    """Return the kind `kind_sql`'s rows say a table is, `hybrid` or `standard`; None for no row."""
    if not rows:
        return None
    return HYBRID_TABLES if str(rows[0][0]).strip().upper() in ("YES", "Y", "TRUE") else STANDARD_TABLES


def fence_sql(table: Sql) -> Sql:
    """Select a target's lock rows as a fence compares them: run id and generation; binds the target."""
    return sql("SELECT RUN_ID, GENERATION FROM {table} WHERE TARGET_NAME = %s", table=table)


def generation(value: object) -> int:
    """Read a GENERATION value: NULL, from a row written before fences, is generation 0."""
    return int(str(value)) if value is not None else 0


class LockTableMethods(Session):
    """Create the lock table, and run transactions on it that serialise with every rival one."""

    # The tables this session found to be of the kind it was configured with; checked once each.
    _kinds_checked: frozenset[str] = frozenset()

    @property
    def _hybrid(self) -> bool:
        """Whether the state and lock tables are hybrid tables, as `state.+table_kind` says."""
        return self._state_table_kind == HYBRID_TABLES

    def _ensure_lock_table(self, state_table: QualifiedName, target_name: str = MUTEX_TARGET) -> Sql:
        """Create the lock table beside `state_table` unless it exists, and seed it; return its name.

        The table is probed first, with a read: Snowflake checks the CREATE TABLE privilege for
        CREATE TABLE IF NOT EXISTS even when the table exists, so a role that may only read and
        write SST's tables could never take the lock. It is created only when the probe cannot
        see it, and migrated only when it lacks a column. A standard table's mutex row is seeded
        with a MERGE, which needs only the privilege to write it; a hybrid table's row for
        `target_name`, when one is named, with an INSERT that a rival's seed may beat. DDL and
        seeds run before any lock transaction, as autocommitted statements: DDL inside a
        transaction would commit it, and an insert of a key a rival transaction holds waits.

        Raises:
            SnowflakePortError: probing, creating, migrating, or seeding the table failed; or
                the table is of the other kind (SST-APL031), or a hybrid one cannot be created
                where it is configured (SST-APL030).
        """
        table = lock_table_sql(state_table)
        missing = self._lock_table_ddl(table)
        if missing:
            self._create(missing, lock_table_for(state_table), "lock table creation failed")
        self.check_table_kind(lock_table_for(state_table))
        if not self._hybrid:
            self.query(_seed_sql(table), (MUTEX_TARGET, MUTEX_TARGET))
        elif target_name != MUTEX_TARGET:
            try:
                self.query(_row_seed_sql(table), (target_name,))
            except SnowflakePortError as exc:
                if exc.errno != DUPLICATE_KEY:
                    raise
        return table

    def _create(self, statements: tuple[Sql, ...], table: QualifiedName, failure: str) -> None:
        """Run the DDL that creates or migrates `table`, naming a hybrid table a transient schema refuses.

        Raises:
            SnowflakePortError: the DDL failed; a hybrid table in a transient database or schema
                carries SST-APL030.
        """
        result = self.execute_script(statements)
        if not result.ok and result.error is not None and result.error.errno == _TRANSIENT_HYBRID:
            diagnostic = D("SST-APL030", value=table.sql)
            raise SnowflakePortError(diagnostic.message, errno=_TRANSIENT_HYBRID, diagnostic=diagnostic)
        _require_ok(result, failure)

    def check_table_kind(self, table: QualifiedName) -> None:
        """Refuse a state or lock table of the other kind than `state.+table_kind` names.

        CREATE ... IF NOT EXISTS succeeds on a table of either kind, so a kind changed in the
        configuration would otherwise run the other kind's protocol on the table that exists.
        A table checked once is not checked again on this session; one that does not exist passes.

        Raises:
            SnowflakePortError: the table is of the other kind (SST-APL031), or the read failed.
        """
        if table.sql in self._kinds_checked:
            return
        found, configured = self.kind_of(table), HYBRID_TABLES if self._hybrid else STANDARD_TABLES
        if found is not None and found != configured:
            diagnostic = D("SST-APL031", value=table.sql, found=found, expected=configured)
            raise SnowflakePortError(diagnostic.message, diagnostic=diagnostic)
        self._kinds_checked = self._kinds_checked | {table.sql}

    def kind_of(self, table: QualifiedName) -> str | None:
        """Return whether `table` is a `hybrid` or `standard` table; None when it does not exist.

        Raises:
            SnowflakePortError: the read failed.
        """
        return kind_in(self.query(kind_sql(table), (table.schema.folded, table.name.folded)).rows)

    def _lock_table_ddl(self, table: Sql) -> tuple[Sql, ...]:
        """Return the DDL the lock table still needs: none when it exists with every column.

        Raises:
            SnowflakePortError: the probe failed for another reason than that the table or its
                newest column is missing.
        """
        try:
            self.query(_probe_sql(table))
        except SnowflakePortError as exc:
            if session_failure(str(exc), errno=exc.errno, sqlstate=exc.sqlstate) is SessionFailure.NOT_VISIBLE:
                return (_create_sql(table, self._state_table_kind),)
            if exc.errno == _INVALID_IDENTIFIER or _NO_SUCH_COLUMN.search(str(exc)):
                return (_migrate_sql(table),)
            raise
        return ()

    @contextlib.contextmanager
    def _lock_transaction(self, table: Sql) -> Iterator[Transaction]:
        """Hold a transaction on a standard lock table whose first statement wrote the mutex row.

        Raises:
            SnowflakePortError: the table has no mutex row, a statement failed, or the commit
                failed; the transaction was rolled back.
        """
        with self._transaction_block() as transaction:
            if transaction.run(_mutex_sql(table), (MUTEX_TARGET,)) == 0:
                raise SnowflakePortError("the run lock table has no mutex row; take the lock to create it")
            yield transaction

    @contextlib.contextmanager
    def _row_transaction(self, table: Sql, target_name: str) -> Iterator[tuple[Transaction, tuple[object, ...] | None]]:
        """Hold a transaction on a hybrid lock table whose first statement locked the target's row.

        Yields the transaction and the row as `row_lock_sql` reads it; None when the target has
        no row, which no claim has seeded. A row a rival transaction holds is asked for again,
        a moment later, up to `_ROW_LOCK_ATTEMPTS` times in the same transaction: Snowflake
        refuses it at once rather than waiting, and the transaction stays usable.

        Raises:
            SnowflakePortError: the row stayed locked, a statement failed, or the commit failed;
                the transaction was rolled back.
        """
        with self._transaction_block() as transaction:
            for attempt in range(_ROW_LOCK_ATTEMPTS):
                try:
                    rows = transaction.rows(row_lock_sql(table), (target_name,))
                    break
                except _driver_errors() as exc:
                    if getattr(exc, "errno", None) != KEY_LOCKED or attempt == _ROW_LOCK_ATTEMPTS - 1:
                        raise
                    time.sleep(_ROW_LOCK_PAUSE_SECONDS[min(attempt, len(_ROW_LOCK_PAUSE_SECONDS) - 1)])
            yield transaction, (rows[0] if rows else None)


def fence_holds(transaction: Transaction, table: Sql, target_name: str, fence: LockFence) -> bool:
    """Report whether the target's one lock row records `fence`, read inside a lock transaction."""
    return records_fence(transaction.rows(fence_sql(table), (target_name,)), fence)


def records_fence(rows: tuple[tuple[object, ...], ...], fence: LockFence) -> bool:
    """Report whether a target's lock rows, as `fence_sql` reads them, are one row recording `fence`."""
    return [(str(row[0]), generation(row[1])) for row in rows] == [(fence.run_id, fence.generation)]
