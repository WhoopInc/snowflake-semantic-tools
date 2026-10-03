"""A driver double for the run lock and state tables with Snowflake's transactional locking.

`LockTables` answers the connector's lock-table and state-table statements, recognised by
their text and binds, with the semantics the run lock depends on ("Transactions" in the
Snowflake documentation), and no more:

- READ COMMITTED: each statement reads what was committed before it began, plus its own
  transaction's earlier writes, which no other connection sees before COMMIT.
- UPDATE, DELETE and MERGE take the table's lock, waiting while another transaction holds it;
  INSERT does not. A lock is held until its transaction commits or rolls back.
- Pessimistically, a statement that waits for a lock judges its WHERE clause against what it
  read when it began, before waiting, which is the re-evaluation the documentation does not
  promise; a protocol that is correct here does not depend on it.

A statement outside BEGIN commits on its own. Expiry is judged by `now`, as Snowflake's
CURRENT_TIMESTAMP is. `on` is called with ("ran", statement, connection) after each statement
and ("waiting", statement, connection) when one blocks on a lock, so a test can stage a race
deterministically.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import RLock

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector

LOCK = "DB.S.SST_STATE_LOCK"
STATE = "DB.S.SST_STATE"
_EPOCH = datetime(2026, 10, 1, tzinfo=UTC)


@dataclass
class LockRow:
    target: str
    run_id: str
    owner: str | None
    host: str | None
    acquired_at: float
    expires_at: float
    generation: int | None


@dataclass
class Tables:
    """The committed contents: the lock table's rows, and the state table's by (target, key)."""

    locks: list[LockRow] = field(default_factory=list)
    state: dict[tuple[str, str], tuple[object, ...]] = field(default_factory=dict)
    manifests: dict[str, object] = field(default_factory=dict)


_Write = Callable[[Tables], object]


@dataclass
class _Transaction:
    writes: list[_Write] = field(default_factory=list)
    locked: set[str] = field(default_factory=set)


class LockTables:
    def __init__(self) -> None:
        self.now = 0.0
        self.committed = Tables()
        self.statements: list[tuple[int, str]] = []
        self.on: Callable[[str, str, int], object] = lambda event, statement, connection: None
        self._condition = threading.Condition()
        self._holders: dict[str, int] = {}
        self._open: dict[int, _Transaction] = {}

    @property
    def rows(self) -> list[LockRow]:
        """The committed lock rows of every target, the mutex row excluded."""
        return [row for row in self.committed.locks if row.target]

    def executed(self, connection: int | None = None) -> list[str]:
        return [statement for who, statement in self.statements if connection is None or who == connection]

    def execute(
        self, connection: int, statement: str, params: Sequence[object]
    ) -> tuple[list[tuple[object, ...]] | None, int]:
        with self._condition:
            self.statements.append((connection, statement))
        if statement in ("BEGIN", "COMMIT", "ROLLBACK"):
            self._control(connection, statement)
            return None, 0
        result = self._statement(connection, statement, tuple(params))
        self.on("ran", statement, connection)
        return result

    def _control(self, connection: int, statement: str) -> None:
        with self._condition:
            if statement == "BEGIN":
                self._open[connection] = _Transaction()
                return
            transaction = self._open.pop(connection)
            if statement == "COMMIT":
                for write in transaction.writes:
                    write(self.committed)
            self._release(transaction)

    def _statement(
        self, connection: int, statement: str, params: tuple[object, ...]
    ) -> tuple[list[tuple[object, ...]] | None, int]:
        autocommit = connection not in self._open
        if autocommit:
            with self._condition:
                self._open[connection] = _Transaction()
        try:
            found, count = self._run(connection, statement, params)
        except BaseException:
            if autocommit:
                with self._condition:
                    self._release(self._open.pop(connection))
            raise
        if autocommit:
            self._control(connection, "COMMIT")
        return found, count

    def _run(
        self, connection: int, statement: str, params: tuple[object, ...]
    ) -> tuple[list[tuple[object, ...]] | None, int]:
        if statement.startswith(("CREATE TABLE IF NOT EXISTS", "ALTER TABLE")):
            return None, 0
        seen = self._view(connection)
        if statement.startswith("SELECT RUN_ID, OWNER, HOST"):
            held = sorted(
                (row for row in seen.locks if row.target == params[0]), key=lambda row: (row.acquired_at, row.run_id)
            )
            return [self._shown(row) for row in held], 0
        if statement.startswith("SELECT RUN_ID, GENERATION"):
            return [(row.run_id, row.generation) for row in seen.locks if row.target == params[0]], 0
        if statement.startswith("SELECT MAX(GENERATION)"):
            generations = [row.generation for row in seen.locks if row.generation is not None]
            return [(max(generations) if generations else None,)], 0
        if statement.startswith(f"INSERT INTO {LOCK}"):
            target, run_id, owner, host, ttl, generation = params
            row = LockRow(
                str(target),
                str(run_id),
                str(owner),
                str(host),
                self.now,
                self.now + float(str(ttl)),
                int(str(generation)),
            )
            # INSERT takes no lock that UPDATE, DELETE or MERGE wait for.
            return None, self._write(connection, None, lambda tables: tables.locks.append(copy.copy(row)), 1)
        return None, self._dml(connection, statement, params, seen)

    def _dml(self, connection: int, statement: str, params: tuple[object, ...], seen: Tables) -> int:
        """Run an UPDATE, DELETE or MERGE: its rows are judged against `seen`, read before any wait."""
        if statement.startswith(f"MERGE INTO {LOCK} AS held USING (SELECT %s AS TARGET_NAME) AS seed"):
            if any(row.target == params[0] for row in seen.locks):
                return self._write(connection, LOCK, lambda tables: None, 0)
            seed = LockRow(str(params[1]), "", None, None, self.now, self.now, 0)
            return self._write(connection, LOCK, lambda tables: tables.locks.append(copy.copy(seed)), 1)
        if statement.startswith(f"UPDATE {LOCK} SET ACQUIRED_AT"):
            matched = _judged(seen, lambda row: row.target == params[0])
            return self._write(connection, LOCK, _update(matched, acquired_at=self.now), len(matched))
        if statement.startswith(f"UPDATE {LOCK} SET GENERATION"):
            matched = _judged(seen, lambda row: row.target == params[1])
            return self._write(connection, LOCK, _update(matched, generation=int(str(params[0]))), len(matched))
        if statement.startswith(f"UPDATE {LOCK} SET EXPIRES_AT"):
            ttl, target, run_id, generation = params
            matched = _judged(seen, lambda row: _fence(row) == (target, run_id, generation))
            expires_at = self.now + float(str(ttl))
            return self._write(connection, LOCK, _update(matched, expires_at=expires_at), len(matched))
        if statement.startswith(f"DELETE FROM {LOCK}"):
            if len(params) == 1:
                matched = _judged(seen, lambda row: row.target == params[0])
            else:
                matched = _judged(seen, lambda row: _fence(row) == params)
            return self._write(connection, LOCK, _delete(matched), len(matched))
        if statement.startswith(f"MERGE INTO {STATE} AS target"):
            key, values = (str(params[0]), str(params[1])), params
            return self._write(connection, STATE, lambda tables: tables.state.__setitem__(key, values), 1)
        if statement.startswith(f"DELETE FROM {STATE}"):
            gone = (str(params[0]), str(params[1]))
            return self._write(connection, STATE, lambda tables: tables.state.pop(gone, None), int(gone in seen.state))
        if statement.startswith(f"UPDATE {STATE} SET STATE_MANIFEST_ID"):
            manifest, target = params
            return self._write(connection, STATE, lambda tables: tables.manifests.__setitem__(str(target), manifest), 1)
        raise AssertionError(f"unexpected statement: {statement}")

    def _write(self, connection: int, table: str | None, write: _Write, count: int) -> int:
        """Take `table`'s lock for the connection's transaction, then record `write` in it."""
        if table is not None:
            self._lock(connection, table)
        self._open[connection].writes.append(write)
        return count

    def _lock(self, connection: int, table: str) -> None:
        with self._condition:
            holder = self._holders.get(table)
            if holder not in (None, connection):
                self.on("waiting", table, connection)
                self._condition.wait_for(lambda: self._holders.get(table) in (None, connection), timeout=5)
                assert self._holders.get(table) in (None, connection), f"{table} stayed locked"
            self._holders[table] = connection
            self._open[connection].locked.add(table)

    def _release(self, transaction: _Transaction) -> None:
        for table in transaction.locked:
            self._holders.pop(table, None)
        self._condition.notify_all()

    def _view(self, connection: int) -> Tables:
        """What a statement of `connection` reads: committed data plus its transaction's own writes."""
        with self._condition:
            seen = copy.deepcopy(self.committed)
            transaction = self._open.get(connection)
            writes = list(transaction.writes) if transaction is not None else []
        for write in writes:
            write(seen)
        return seen

    def _shown(self, row: LockRow) -> tuple[object, ...]:
        return (
            row.run_id,
            row.owner,
            row.host,
            _EPOCH + timedelta(seconds=row.acquired_at),
            _EPOCH + timedelta(seconds=row.expires_at),
            row.expires_at <= self.now,
        )


_Key = tuple[str, str, int | None, float]


def _key(row: LockRow) -> _Key:
    return (row.target, row.run_id, row.generation, row.acquired_at)


def _fence(row: LockRow) -> tuple[object, ...]:
    return (row.target, row.run_id, row.generation or 0)


def _judged(seen: Tables, matches: Callable[[LockRow], bool]) -> set[_Key]:
    """The rows a statement's WHERE clause matched in what it read when it began."""
    return {_key(row) for row in seen.locks if matches(row)}


def _update(matched: set[_Key], **values: object) -> _Write:
    """Set `values` on exactly the rows judged matched, whatever was committed since."""

    def write(tables: Tables) -> None:
        for row in [row for row in tables.locks if _key(row) in matched]:
            for name, value in values.items():
                setattr(row, name, value)

    return write


def _delete(matched: set[_Key]) -> _Write:
    def write(tables: Tables) -> None:
        tables.locks = [row for row in tables.locks if _key(row) not in matched]

    return write


class _Cursor:
    def __init__(self, tables: LockTables, connection: int) -> None:
        self._tables = tables
        self._connection = connection
        self.description: tuple[tuple[str], ...] | None = None
        self.rowcount = 0
        self.sfqid = "query-id"
        self._rows: list[tuple[object, ...]] = []

    def execute(self, statement: str, params: Sequence[object] | None = None) -> None:
        rows, self.rowcount = self._tables.execute(self._connection, statement, tuple(params or ()))
        self._rows = rows or []
        self.description = (("RUN_ID",),) if rows is not None else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self, tables: LockTables, connection: int) -> None:
        self._tables = tables
        self._number = connection

    def cursor(self, *args: object) -> _Cursor:
        return _Cursor(self._tables, self._number)


class LockTablesConnector(SnowflakeConnector):
    """A connector on its own connection to `tables`: two of them are two Snowflake sessions."""

    _count = 0

    def __init__(self, tables: LockTables) -> None:
        LockTablesConnector._count += 1
        self.number = LockTablesConnector._count
        self._lock = RLock()
        self._scoped_guard = threading.Lock()
        self._connection = _Connection(tables, self.number)  # type: ignore[assignment]  # a double, not a driver

    def object_exists(self, object_type: str, qualified_name: object) -> bool:
        return True

    def _dict_rows(self, statement: object) -> tuple[dict[str, object], ...]:
        names = ("COMPONENT_FINGERPRINTS", "PHYSICAL_RESOURCES", "STATE_MANIFEST_ID")
        return tuple({"name": name} for name in names)
