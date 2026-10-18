"""A driver double for hybrid run-lock and state tables, with the row locking Snowflake measured.

`HybridTables` answers the connector's hybrid lock-table and state-table statements, recognised
by their text and binds, as a live Snowflake account answered them when measured, and no more:

- `SELECT ... FOR UPDATE` locks the row it reads until the transaction ends, and fails at once
  with 200010 when another transaction holds it; the transaction stays usable.
- An INSERT of a key that is committed fails with 200001. One of a key another transaction
  holds, and an UPDATE or DELETE of a row another transaction holds, would wait until the
  statement timeout: here they fail the test, since the protocol must never issue them.
- READ COMMITTED: a statement reads what was committed, plus its own transaction's writes.

A statement outside BEGIN commits on its own. Expiry is judged by `now`. `on` is called with
("ran", statement, connection) after each statement and ("refused", statement, connection)
when a FOR UPDATE meets a row another transaction holds, so a test can stage a race.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta

from snowflake.connector.errors import ProgrammingError

from snowflake_semantic_tools.domain.state.lock import HYBRID_TABLES
from tests.helpers.lock_tables import _EPOCH, LOCK, STATE, LockRow, Tables
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession, Rows

# A connection number no connector has: a test holds a row with it, as a rival session would.
RIVAL = -1


@dataclass
class _Open:
    writes: list[Callable[[Tables], object]] = field(default_factory=list)
    rows: set[str] = field(default_factory=set)


class HybridTables:
    def __init__(self, *, hybrid: bool = True) -> None:
        self.now = 0.0
        self.committed = Tables()
        self.statements: list[tuple[int, str]] = []
        self.on: Callable[[str, str, int], object] = lambda event, statement, connection: None
        # What INFORMATION_SCHEMA.TABLES says the tables are; a test sets False for standard ones.
        self.hybrid = hybrid
        # False when INFORMATION_SCHEMA lists no such table; `kind_refused` fails the read instead.
        self.kind_listed = True
        self.kind_refused = False
        self.lock_table_exists = True
        # The errno a CREATE fails with, as a transient schema's 391412, and its message; None
        # creates it.
        self.create_fails: int | None = None
        self.create_message = "Cannot create hybrid table within transient database or schema."
        self._guard = threading.RLock()
        self._held: dict[str, int] = {}
        self._open: dict[int, _Open] = {}

    @property
    def rows(self) -> dict[str, LockRow]:
        """The committed lock rows, by target."""
        return {row.target: row for row in self.committed.locks}

    def executed(self, connection: int | None = None) -> list[str]:
        return [statement for who, statement in self.statements if connection is None or who == connection]

    def hold(self, target: str, connection: int = RIVAL) -> None:
        """Hold `target`'s row for `connection`, as a rival transaction's FOR UPDATE would."""
        with self._guard:
            self._open.setdefault(connection, _Open()).rows.add(target)
            self._held[target] = connection

    def let_go(self, connection: int = RIVAL) -> None:
        """End `connection`'s transaction, releasing every row it holds and keeping nothing."""
        with self._guard:
            self._end(connection, commit=False)

    def execute(self, connection: int, statement: str, params: Sequence[object]) -> tuple[Rows | None, int]:
        with self._guard:
            self.statements.append((connection, statement))
            if statement in ("BEGIN", "COMMIT", "ROLLBACK"):
                if statement == "BEGIN":
                    self._open[connection] = _Open()
                else:
                    self._end(connection, commit=statement == "COMMIT")
                return None, 0
            autocommit = connection not in self._open
            if autocommit:
                self._open[connection] = _Open()
            try:
                result = self._run(connection, statement, tuple(params))
            except BaseException:
                if autocommit:
                    self._end(connection, commit=False)
                raise
            if autocommit:
                self._end(connection, commit=True)
        self.on("ran", statement, connection)
        return result

    def _end(self, connection: int, *, commit: bool) -> None:
        transaction = self._open.pop(connection, None)
        if transaction is None:
            return
        if commit:
            for write in transaction.writes:
                write(self.committed)
        for target in transaction.rows:
            if self._held.get(target) == connection:
                del self._held[target]

    def _view(self, connection: int) -> Tables:
        seen = copy.deepcopy(self.committed)
        for write in self._open[connection].writes:
            write(seen)
        return seen

    def _require_row(self, connection: int, target: object, statement: str) -> None:
        """Fail the test unless the connection holds `target`'s row: anything else would wait."""
        holder = self._held.get(str(target))
        assert holder == connection, f"would wait until the statement timeout: {statement} (row held by {holder})"

    def _run(self, connection: int, statement: str, params: tuple[object, ...]) -> tuple[Rows | None, int]:
        if statement.startswith("SELECT IS_HYBRID"):
            if self.kind_refused:
                raise ProgrammingError(msg="Insufficient privileges to operate on schema 'S'", errno=3001)
            return ([("YES" if self.hybrid else "NO",)] if self.kind_listed else []), 0
        if statement.startswith(("CREATE HYBRID TABLE IF NOT EXISTS", "CREATE TABLE IF NOT")):
            if self.create_fails is not None:
                raise ProgrammingError(msg=self.create_message, errno=self.create_fails)
            if LOCK in statement:
                self.lock_table_exists = True
            return None, 0
        if statement.startswith("ALTER TABLE"):
            return None, 0
        if statement.startswith(f"SELECT GENERATION FROM {LOCK}"):
            if not self.lock_table_exists:
                raise ProgrammingError(
                    msg=f"Object '{LOCK}' does not exist or not authorized.", errno=2003, sqlstate="02000"
                )
            return [], 0
        seen = self._view(connection)
        rows = {row.target: row for row in seen.locks}
        if statement.startswith(f"INSERT INTO {LOCK}"):
            target = str(params[0])
            assert target not in self._held or self._held[target] == connection, f"would wait: {statement}"
            if target in rows:
                raise ProgrammingError(msg="A primary key already exists.", errno=200001, sqlstate="22000")
            row = LockRow(target, "", None, None, self.now, self.now, 0)
            self._open[connection].writes.append(lambda tables: tables.locks.append(copy.copy(row)))
            return None, 1
        if statement.startswith("SELECT EXPIRES_AT <= CURRENT_TIMESTAMP()"):
            return [(row.expires_at <= self.now,) for row in seen.locks if row.target == params[0]], 0
        if statement.startswith("SELECT RUN_ID, OWNER, HOST, ACQUIRED_AT, EXPIRES_AT") and statement.endswith(
            "FOR UPDATE"
        ):
            # Snowflake fails a FOR UPDATE whose select list calls CURRENT_TIMESTAMP() (000603).
            assert "CURRENT_TIMESTAMP" not in statement, statement
            target = str(params[0])
            holder = self._held.get(target)
            if holder not in (None, connection):
                self.on("refused", statement, connection)
                raise ProgrammingError(
                    msg="Transaction has locked a key and this lock has not yet been released.",
                    errno=200010,
                    sqlstate="22000",
                )
            if target not in rows:
                return [], 0
            self._held[target] = connection
            self._open[connection].rows.add(target)
            shown = rows[target]
            return [(shown.run_id, shown.owner, shown.host, *self._shown(shown)[3:5], shown.generation)], 0
        if statement.startswith("SELECT RUN_ID, GENERATION"):
            return [(row.run_id, row.generation) for row in seen.locks if row.target == params[0]], 0
        return self._dml(connection, statement, params, rows)

    def _dml(
        self, connection: int, statement: str, params: tuple[object, ...], rows: dict[str, LockRow]
    ) -> tuple[Rows | None, int]:
        if statement.startswith(f"UPDATE {LOCK} SET RUN_ID = %s"):
            run_id, owner, host, ttl, generation, target = params
            self._require_row(connection, target, statement)
            values = {
                "run_id": str(run_id),
                "owner": owner,
                "host": host,
                "acquired_at": self.now,
                "expires_at": self.now + float(str(ttl)),
                "generation": int(str(generation)),
            }
            return None, self._update(connection, str(target), values, rows)
        if statement.startswith(f"UPDATE {LOCK} SET RUN_ID = ''"):
            target, run_id, generation = params
            self._require_row(connection, target, statement)
            if not _fenced(rows.get(str(target)), run_id, generation):
                return None, 0
            values = {"run_id": "", "owner": None, "host": None, "expires_at": self.now}
            return None, self._update(connection, str(target), values, rows)
        if statement.startswith(f"UPDATE {LOCK} SET EXPIRES_AT"):
            ttl, target, run_id, generation = params
            self._require_row(connection, target, statement)
            if not _fenced(rows.get(str(target)), run_id, generation):
                return None, 0
            return None, self._update(connection, str(target), {"expires_at": self.now + float(str(ttl))}, rows)
        if statement.startswith(f"DELETE FROM {STATE} WHERE TARGET_NAME = %s AND ARTIFACT_KEY IN"):
            target, *keys = params
            self._require_row(connection, target, statement)
            gone = {(str(target), str(key)) for key in keys}
            self._open[connection].writes.append(lambda tables: [tables.state.pop(key, None) for key in gone])
            return None, len(gone)
        if statement.startswith(f"INSERT INTO {STATE}"):
            self._require_row(connection, params[0], statement)
            width = len(params) // statement.count("SELECT")
            entries = [params[start : start + width] for start in range(0, len(params), width)]

            def insert(tables: Tables) -> None:
                for entry in entries:
                    key = (str(entry[0]), str(entry[1]))
                    assert key not in tables.state, f"duplicate key {key}"
                    tables.state[key] = entry

            self._open[connection].writes.append(insert)
            return None, len(entries)
        if statement.startswith(f"UPDATE {STATE} SET STATE_MANIFEST_ID"):
            manifest, target = params
            self._require_row(connection, target, statement)
            self._open[connection].writes.append(lambda tables: tables.manifests.__setitem__(str(target), manifest))
            return None, 1
        raise AssertionError(f"unexpected statement: {statement}")

    def _update(self, connection: int, target: str, values: dict[str, object], rows: dict[str, LockRow]) -> int:
        if target not in rows:
            return 0

        def write(tables: Tables) -> None:
            for row in tables.locks:
                if row.target == target:
                    for name, value in values.items():
                        setattr(row, name, value)

        self._open[connection].writes.append(write)
        return 1

    def _shown(self, row: LockRow) -> tuple[object, ...]:
        return (
            row.run_id,
            row.owner,
            row.host,
            _EPOCH + timedelta(seconds=row.acquired_at),
            _EPOCH + timedelta(seconds=row.expires_at),
            row.expires_at <= self.now,
        )


def _fenced(row: LockRow | None, run_id: object, generation: object) -> bool:
    return row is not None and (row.run_id, row.generation or 0) == (run_id, generation)


class HybridTablesConnector(FakeDriverConnector):
    """A connector configured for hybrid tables, on its own connection to `tables`."""

    # How many connections have been opened, which numbers each one.
    _opened = 0

    def __init__(self, tables: HybridTables, *, kind: str = HYBRID_TABLES) -> None:
        HybridTablesConnector._opened += 1
        self.number = HybridTablesConnector._opened
        number = self.number

        def respond(statement: str, binds: tuple[object, ...]) -> tuple[Rows | None, int]:
            return tables.execute(number, statement, binds)

        super().__init__(FakeDriverSession(respond=respond))
        self._state_table_kind = kind

    def object_exists(self, object_type: str, qualified_name: object) -> bool:
        return True

    def _dict_rows(self, statement: object) -> tuple[dict[str, object], ...]:
        names = (
            "COMPONENT_FINGERPRINTS",
            "PHYSICAL_RESOURCES",
            "LIVE_DIGEST",
            "MEMBER_FINGERPRINTS",
            "STATE_MANIFEST_ID",
        )
        return tuple({"name": name} for name in names)
