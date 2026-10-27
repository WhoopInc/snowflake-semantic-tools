"""The Snowflake driver's connection and cursor, for the real connector's own tests.

`FakeDriverSession` stands where `snowflake.connector.connect` returns a connection: the
connector opens cursors on it and executes statements. It is the connection and its one
cursor in one object, as the connector holds one cursor at a time under its session lock.

- Every statement is recorded with its binds in `executed`; `statements` lists the texts,
  and `num_statements` the statement count, and `timeouts` the timeout, each was sent with.
- A statement whose text starts with a key of `failures` raises that error; the key
  `"cursor"` fails opening the cursor instead, and `""` fails every statement.
- A statement whose text starts with a key of `rows` returns those rows: tuples, or dicts
  keyed by column for a dictionary cursor. `respond`, when given, answers every statement
  instead: it returns the rows (None: the statement returns no result set) and the row count.
- After each statement the cursor reports its query id `q<n>` and `rowcount`.

`FakeDriverConnector` is the real `SnowflakeConnector` on such a session, without connecting.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from threading import RLock

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector

Rows = Sequence[tuple[object, ...]] | Sequence[Mapping[str, object]]
Respond = Callable[[str, tuple[object, ...]], tuple[Rows | None, int]]


class FakeDriverSession:
    """A driver connection and its cursor: statements recorded, failures and rows by prefix."""

    def __init__(
        self,
        failures: Mapping[str, BaseException] | None = None,
        rows: Mapping[str, Rows] | None = None,
        *,
        rowcount: int = 0,
        respond: Respond | None = None,
    ) -> None:
        self.failures = dict(failures or {})
        self.rows: dict[str, Rows] = dict(rows or {})
        self.respond = respond
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.num_statements: list[int | None] = []
        self.timeouts: list[int | None] = []
        self.description: tuple[tuple[str], ...] | None = None
        self.sfqid = ""
        self.rowcount = 0
        self.closed = False
        self._rowcount = rowcount
        self._result: Rows = ()

    @property
    def statements(self) -> list[str]:
        """The text of every statement executed, in order."""
        return [statement for statement, _ in self.executed]

    def cursor(self, *args: object) -> FakeDriverSession:
        if "cursor" in self.failures:
            raise self.failures["cursor"]
        return self

    def execute(
        self,
        statement: str,
        params: Sequence[object] | None = None,
        *,
        num_statements: int | None = None,
        timeout: int | None = None,
    ) -> None:
        binds = tuple(params or ())
        self.executed.append((statement, binds))
        self.num_statements.append(num_statements)
        self.timeouts.append(timeout)
        failure = next((error for prefix, error in self.failures.items() if statement.startswith(prefix)), None)
        if failure is not None:
            raise failure
        self.sfqid = f"q{len(self.executed)}"
        if self.respond is not None:
            answered, self.rowcount = self.respond(statement, binds)
            self._answer(answered)
            return
        self.rowcount = self._rowcount
        self._answer(next((rows for prefix, rows in self.rows.items() if statement.startswith(prefix)), None))

    def fetchall(self) -> list[object]:
        return list(self._result)

    def close(self) -> None:
        self.closed = True

    def _answer(self, rows: Rows | None) -> None:
        self._result = rows or ()
        first = self._result[0] if self._result else None
        columns = tuple(first) if isinstance(first, Mapping) else ("C",)
        self.description = tuple((str(column),) for column in columns) if rows is not None else None


class FakeDriverConnector(SnowflakeConnector):
    """The real connector on a `FakeDriverSession`: what connecting would have set, and no more."""

    def __init__(self, session: FakeDriverSession) -> None:
        self._lock = RLock()
        self._scoped_guard = threading.Lock()
        self._connection = session  # type: ignore[assignment]  # a double, not a driver connection
