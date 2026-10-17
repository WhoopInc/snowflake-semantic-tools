"""Rebuild the state and lock tables as the other kind of table, keeping the old ones as backups.

`StateConvertMethods.convert_state_tables` is what `sst state convert` does. Snowflake swaps a
hybrid table only with another hybrid table (measured: 391439), so the state table is copied:
a table of the new kind is created under a temporary name and filled with INSERT ... SELECT,
then the old table is renamed to its backup, `<name>_<KIND>_BAK`, and the new one into its
place. The lock table holds no state worth keeping once no lock is live, so it is renamed to
its backup and created again, empty, in the new kind; the first claim seeds it.

A conversion runs while no apply does. It is refused while any target's lock is live, and,
since DDL cannot sit in a transaction with the copy, it checks again after the copy that no
lock was taken and that the state table still holds as many rows, dropping its copy and
refusing otherwise. A refusal before the renames changes nothing; the backups are never
overwritten, so a conversion whose backup name is taken is refused.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NoReturn

from snowflake_semantic_tools.adapters.snowflake.connector import lock_table as locks
from snowflake_semantic_tools.adapters.snowflake.connector import state_table as states
from snowflake_semantic_tools.adapters.snowflake.connector.lock_table import lock_table_sql
from snowflake_semantic_tools.adapters.snowflake.connector.session import _require_ok
from snowflake_semantic_tools.adapters.snowflake.connector.state_table import StateTableMethods
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, join, qname, sql
from snowflake_semantic_tools.domain.state.lock import HYBRID_TABLES, STANDARD_TABLES, lock_table_for


@dataclass(frozen=True, slots=True)
class Conversion:
    """What a conversion did: the kinds, the rows it copied, and where the old tables went."""

    from_kind: str
    to_kind: str
    rows: int
    state_backup: QualifiedName
    lock_backup: QualifiedName | None


def _renamed(table: QualifiedName, suffix: str) -> QualifiedName:
    name = table.name
    return QualifiedName(table.database, table.schema, Identifier(f"{name.value}_{suffix}", quoted=name.quoted))


def _live_locks_sql(lock: Sql) -> Sql:
    """Select every target whose lock is held and unexpired, by Snowflake's clock."""
    return sql(
        "SELECT TARGET_NAME, RUN_ID FROM {table} WHERE TARGET_NAME <> '' AND RUN_ID <> '' "
        "AND EXPIRES_AT > CURRENT_TIMESTAMP() ORDER BY TARGET_NAME",
        table=lock,
    )


def _rename_sql(table: QualifiedName, to: QualifiedName) -> Sql:
    return sql("ALTER TABLE {table} RENAME TO {to}", table=qname(table), to=qname(to))


class StateConvertMethods(StateTableMethods):
    """Convert the state and lock tables between standard and hybrid tables."""

    def convert_state_tables(self, state_table: QualifiedName, to_kind: str) -> Conversion:
        """Rebuild the state and lock tables beside `state_table` as `to_kind` tables.

        Raises:
            SnowflakePortError: the conversion is refused (SST-APL034), with nothing changed,
                or a statement failed.
        """
        if to_kind not in (STANDARD_TABLES, HYBRID_TABLES):
            raise ValueError(f"no table kind {to_kind!r}")
        from_kind = self.kind_of(state_table)
        if from_kind is None:
            _refuse(state_table, "it does not exist; set state.+table_kind, and apply creates it of that kind")
        if from_kind == to_kind:
            _refuse(state_table, f"it is already a {to_kind} table")
        lock = lock_table_for(state_table)
        lock_kind = self.kind_of(lock)
        suffix = f"{from_kind.upper()}_BAK"
        state_backup, lock_backup = _renamed(state_table, suffix), _renamed(lock, suffix)
        for backup in (state_backup, lock_backup if lock_kind is not None else None):
            if backup is not None and self.kind_of(backup) is not None:
                _refuse(state_table, f"its backup {backup.sql} already exists; drop or rename it first")
        self._refuse_live_locks(state_table, lock_kind)
        before = self._count(state_table)
        copy = _renamed(state_table, "SST_CONVERT")
        self._copy(state_table, copy, to_kind)
        if self._count(copy) != before or self._count(state_table) != before:
            self._drop(copy)
            _refuse(state_table, "its rows changed while they were copied; run it again once no apply runs")
        try:
            self._refuse_live_locks(state_table, lock_kind)
        except SnowflakePortError:
            self._drop(copy)
            raise
        renames = [_rename_sql(state_table, state_backup), _rename_sql(copy, state_table)]
        if lock_kind is not None:
            renames.append(_rename_sql(lock, lock_backup))
        renames.append(locks._create_sql(lock_table_sql(state_table), to_kind))
        _require_ok(self.execute_script(tuple(renames)), "state table conversion failed")
        self._kinds_checked = frozenset()
        return Conversion(from_kind, to_kind, before, state_backup, lock_backup if lock_kind is not None else None)

    def _refuse_live_locks(self, state_table: QualifiedName, lock_kind: str | None) -> None:
        if lock_kind is None:
            return
        held = self.query(_live_locks_sql(lock_table_sql(state_table))).rows
        if held:
            named = ", ".join(f"{row[0]} (run {row[1]})" for row in held)
            _refuse(state_table, f"a run holds the lock on {named}; wait for it, or let the lock expire")

    def _count(self, table: QualifiedName) -> int:
        rows = self.query(sql("SELECT COUNT(*) FROM {table}", table=qname(table))).rows
        return int(str(rows[0][0])) if rows else 0

    def _copy(self, state_table: QualifiedName, copy: QualifiedName, to_kind: str) -> None:
        """Create `copy` as a `to_kind` state table and fill it with every row of `state_table`.

        Only the columns the old table holds are copied; one it lacks, from before a migration,
        is left NULL, as reading it would read it.
        """
        present = self._column_names(state_table)
        columns = join(
            ", ",
            (column.identifier for column in (*states._COLUMNS, states._STATE_MANIFEST) if column.name in present),
        )
        insert = sql(
            "INSERT INTO {copy} ({columns}) SELECT {columns} FROM {table}",
            copy=qname(copy),
            columns=columns,
            table=qname(state_table),
        )
        statements = (states._create_sql(copy, to_kind), insert)
        result = self.execute_script(statements)
        if not result.ok:
            self._drop(copy)
            _require_ok(result, "state table copy failed")

    def _drop(self, table: QualifiedName) -> None:
        self.execute_script((sql("DROP TABLE IF EXISTS {table}", table=qname(table)),))


def _refuse(state_table: QualifiedName, detail: str) -> NoReturn:
    diagnostic = D("SST-APL034", value=state_table.sql, detail=detail)
    raise SnowflakePortError(diagnostic.message, diagnostic=diagnostic)
