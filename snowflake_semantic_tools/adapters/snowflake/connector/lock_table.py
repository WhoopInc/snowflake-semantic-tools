"""The lock table's shared ground: its creation, the mutex every transaction takes, the fence.

`run_lock` claims, extends and releases the lock; `state_table` writes state only while the
writer's fence holds it. Both open their transaction through `_lock_transaction`, whose first
statement writes the table's mutex row; `run_lock` explains why that makes them serialise.

The mutex row has the empty target name, which no target has. It records, in GENERATION, the
last generation the table issued. Every value is bound.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session, Transaction, _require_ok
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, qname, sql
from snowflake_semantic_tools.domain.state.lock import LockFence, lock_table_for

# The kind of table the state and lock tables are created as. A standard table, not a hybrid
# one: "Concurrent applies and the run lock" in docs/concepts.md compares the two.
TABLE_KIND = sql("TABLE")
# The target name of the mutex row; a target is always named, so no lock row has it.
MUTEX_TARGET = ""


def lock_table_sql(state_table: QualifiedName) -> Sql:
    """Return the lock table beside `state_table` as SQL."""
    return qname(lock_table_for(state_table))


def _create_sql(table: Sql) -> Sql:
    return sql(
        "CREATE {kind} IF NOT EXISTS {table} (TARGET_NAME VARCHAR NOT NULL, RUN_ID VARCHAR NOT NULL, "
        "OWNER VARCHAR, HOST VARCHAR, ACQUIRED_AT TIMESTAMP_TZ NOT NULL, EXPIRES_AT TIMESTAMP_TZ NOT NULL, "
        "GENERATION NUMBER, PRIMARY KEY (TARGET_NAME))",
        kind=TABLE_KIND,
        table=table,
    )


def _migrate_sql(table: Sql) -> Sql:
    """Add GENERATION to a lock table created before fences; safe to repeat."""
    return sql("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS GENERATION NUMBER", table=table)


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


def _fence_sql(table: Sql) -> Sql:
    return sql("SELECT RUN_ID, GENERATION FROM {table} WHERE TARGET_NAME = %s", table=table)


def generation(value: object) -> int:
    """Read a GENERATION value: NULL, from a row written before fences, is generation 0."""
    return int(str(value)) if value is not None else 0


class LockTableMethods(Session):
    """Create the lock table, and run transactions on it that serialise with every other one."""

    def _ensure_lock_table(self, state_table: QualifiedName) -> Sql:
        """Create the lock table beside `state_table` unless it exists, with its mutex row; return its name.

        Runs before any lock transaction, as autocommitted statements: DDL inside a transaction
        would commit it.

        Raises:
            SnowflakePortError: creating, migrating, or seeding the table failed.
        """
        table = lock_table_sql(state_table)
        _require_ok(self.execute_script((_create_sql(table), _migrate_sql(table))), "lock table creation failed")
        self.query(_seed_sql(table), (MUTEX_TARGET, MUTEX_TARGET))
        return table

    @contextlib.contextmanager
    def _lock_transaction(self, table: Sql) -> Iterator[Transaction]:
        """Hold a transaction on the lock table whose first statement wrote the mutex row.

        Raises:
            SnowflakePortError: the table has no mutex row, a statement failed, or the commit
                failed; the transaction was rolled back.
        """
        with self._transaction_block() as transaction:
            if transaction.run(_mutex_sql(table), (MUTEX_TARGET,)) == 0:
                raise SnowflakePortError("the run lock table has no mutex row; take the lock to create it")
            yield transaction


def fence_holds(transaction: Transaction, table: Sql, target_name: str, fence: LockFence) -> bool:
    """Report whether the target's one lock row records `fence`, read inside a lock transaction."""
    rows = transaction.rows(_fence_sql(table), (target_name,))
    return [(str(row[0]), generation(row[1])) for row in rows] == [(fence.run_id, fence.generation)]
