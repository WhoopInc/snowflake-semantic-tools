"""The connector's session: one driver connection, the lock each statement takes, and SQL execution.

Concurrent evals share one connection, so every statement runs on a cursor opened under the
session lock; parallel apply workers each lease a sibling session from a pool instead.
Statements arrive as `Sql` and become text only in `_execute`, the one place this adapter
hands SQL to the driver. A driver or transport failure surfaces as `SnowflakePortError`,
carrying the diagnostic a command reports; any other exception is an SST bug and propagates
unwrapped. A halted session starts no further statement. No statement here is a USE: a scope
is a session's own, set when it connects. The private helpers here are shared by the role
modules beside this one.
"""

from __future__ import annotations

import contextlib
import json
import re
import sys
from collections.abc import Iterator, Mapping, Sequence
from threading import Lock, RLock
from types import MappingProxyType
from typing import Any, Self, cast

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import Error as DriverError

from snowflake_semantic_tools.adapters.json_files import parse_json
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import SessionFailure, session_failure
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import Sql, sql

# What a statement or fetch raises when Snowflake or the network fails: the driver's own
# errors, and the OSError family (its vendored `requests` errors, socket timeouts) that it
# lets escape a large result's chunk download. Anything else is an SST bug, never a
# Snowflake failure, so it propagates unwrapped.
_DRIVER_ERRORS = (DriverError, OSError)

# What a driver or key library may echo into an error that must never reach a diagnostic: a
# credential written as a `name=value` or `name: value` setting, and the path to a key file.
_SECRET_SETTING = re.compile(
    r"(?i)\b([\w-]*(?:password|passwd|pwd|token|passphrase|secret|private_key)[\w-]*)"
    r"([\"']?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&)\]}]+)"
)
_KEY_FILE = re.compile(r"(?i)(?:[A-Za-z]:)?[\w.~-]*(?:[/\\][^\s/\\'\"]+)+\.(?:p8|pem|key|der|p12|pfx)\b")
_REDACTED = "<redacted>"


class Session(ExecutionPort):
    """One live driver connection to Snowflake, which every role of the connector runs on.

    Constructing a session connects, and it stays connected until `close`. Each statement
    runs on its own cursor while the session lock is held; statements that must run
    together, such as a transaction, share one cursor and one hold. No statement this
    session runs changes its current database or schema: `query_in_context` runs on a
    session of its scope's own, opened on first use from the same settings with that scope
    as its database and schema, and closed with this one.

    Raises:
        SnowflakePortError: connecting failed; its diagnostic names the account.
    """

    # The sessions `query_in_context` opened, by folded database and schema; replaced, never
    # mutated, under `_scoped_guard`, which never blocks a statement on this session.
    _scoped: Mapping[tuple[str, str], Session] = MappingProxyType({})
    _scoped_guard = Lock()
    _connection_params: Mapping[str, object] = MappingProxyType({})
    # Why the session was halted; None while it may run statements.
    _halted: str | None = None

    def __init__(self, connection_params: Mapping[str, object]) -> None:
        failure: SnowflakePortError | None = None
        self._connection_params = MappingProxyType(dict(connection_params))
        self._scoped_guard = Lock()
        try:
            # Browser SSO prints its prompts to stdout, which `--output json` reserves
            # for exactly one envelope; the prompts still reach the user on stderr.
            with contextlib.redirect_stdout(sys.stderr):
                self._connection = snowflake.connector.connect(**dict(connection_params))
            self._lock = RLock()
        except Exception as exc:
            failure = _port_error(exc, connecting_to=str(connection_params.get("account") or "Snowflake"))
        # Raised outside the handler, so the error chains no unscrubbed exception from connecting.
        if failure is not None:
            raise failure

    def sibling(self) -> Self:
        """Open another session of this class from the settings this one connected with.

        It shares no connection, cursor, or lock with this one.

        Raises:
            SnowflakePortError: connecting failed.
        """
        return self._connect(self._connection_params)

    def _connect(self, settings: Mapping[str, object]) -> Self:
        """Open a session of this class with `settings`: how siblings and scoped sessions connect."""
        return type(self)(settings)

    def close(self) -> None:
        """Close the driver connection, and every scoped session, waiting for any statement that holds a lock.

        A driver failure to close propagates as the driver raised it, once every session is closed.
        """
        with self._scoped_guard:
            scoped, self._scoped = tuple(self._scoped.values()), MappingProxyType({})
        with self._lock:
            try:
                for session in scoped:
                    session.close()
            finally:
                self._connection.close()

    def halt(self, reason: str) -> None:
        """Refuse every statement from now on, here and on the scoped sessions, with `reason`.

        A statement already running finishes; the next one raises `SnowflakePortError`, and a
        script stops before its next statement. Idempotent: the first reason stands. Never
        waits: a scoped session opened meanwhile sees the reason once it is registered.
        """
        if self._halted is None:
            self._halted = reason
        for session in tuple(self._scoped.values()):
            session.halt(self._halted)

    def query(self, sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        with _as_port_errors(), self._cursor() as cursor:
            return _fetch(cursor, sql, params)

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: Sql,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        # The scoped session runs nothing but these calls, all in the one scope it connected
        # with, so no statement ever changes the scope another statement runs in.
        session = self._scoped_session(scope)
        with _as_port_errors(), session._cursor() as cursor:
            return _fetch(cursor, sql, params)

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        completed: list[str] = []
        rows = 0
        halted = self._halted
        try:
            if halted is None:
                with self._cursor() as cursor:
                    for statement in statements:
                        if (halted := self._halted) is not None:
                            break
                        _execute(cursor, statement)
                        completed.append(str(cursor.sfqid or ""))
                        rows += max(cursor.rowcount or 0, 0)
        except _DRIVER_ERRORS as exc:
            # Only the statements before the failing one completed; the result names exactly
            # those, and never counts the one that failed as written.
            error = ExecutionError(scrubbed_message(exc), getattr(exc, "sqlstate", None), getattr(exc, "errno", None))
            return ExecResult(False, tuple(completed), error, rows_affected=rows)
        if halted is not None:
            return ExecResult(False, tuple(completed), ExecutionError(halted), rows_affected=rows)
        return ExecResult(True, tuple(completed), rows_affected=rows)

    def try_execute(self, sql: Sql) -> ExecResult:
        return self.execute_script((sql,))

    def _scoped_session(self, target: SchemaScope) -> Session:
        """Return the session `query_in_context` runs on in `target`, opening it on first use.

        Opening one connects while holding only `_scoped_guard`, so a slow login never blocks
        a statement on this session.
        """
        key = (target.database.folded, target.schema.folded)
        with self._scoped_guard:
            found = self._scoped.get(key)
            if found is None:
                found = self._open_scoped(target)
                self._scoped = MappingProxyType({**self._scoped, key: found})
        # Registered before the check: a halt either sees this session or set the reason first.
        if self._halted is not None:
            found.halt(self._halted)
        return found

    def _open_scoped(self, target: SchemaScope) -> Session:
        """Connect a session whose current database and schema are `target`, and check they are.

        Raises:
            SnowflakePortError: connecting failed, or the session did not start in `target`,
                as when the database or schema does not exist or the role cannot use it.
        """
        settings = {**self._connection_params, "database": target.database.sql, "schema": target.schema.sql}
        session = self._connect(settings)
        try:
            found = session.query(sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")).rows
        except BaseException:
            session.close()
            raise
        current = tuple(str(value) if value is not None else "" for value in (found[0] if found else ()))
        if current != (target.database.folded, target.schema.folded):
            session.close()
            shown = ".".join(current) or "no schema"
            raise SnowflakePortError(f"a session for {target.sql} started in {shown}")
        return session

    @contextlib.contextmanager
    def _transaction_block(self) -> Iterator[Transaction]:
        """Hold one explicit transaction for a block, on one cursor under the session lock.

        BEGIN runs first; the block's statements run through the `Transaction`, and COMMIT
        runs when the block ends, unless it called `Transaction.rollback`. When the block
        raises, the transaction is rolled back and the failure propagates.

        Raises:
            SnowflakePortError: a statement, the BEGIN, or the COMMIT failed; the transaction
                was rolled back. A ROLLBACK that fails too is noted on the error rather than
                replacing it.
        """
        with _as_port_errors(), self._cursor() as cursor:
            transaction = Transaction(cursor)
            try:
                _execute(cursor, sql("BEGIN"))
                yield transaction
                if transaction.open:
                    transaction.end(sql("COMMIT"))
            except Exception as failure:
                if transaction.open:
                    try:
                        transaction.end(sql("ROLLBACK"))
                    except Exception as rollback_failure:
                        # The error that aborted the write is the one to report; a ROLLBACK
                        # that fails too (the session is usually gone) is context for it.
                        failure.add_note(f"ROLLBACK also failed: {rollback_failure}")
                raise

    @contextlib.contextmanager
    def _cursor(self, *cursor_class: type[SnowflakeCursor]) -> Iterator[SnowflakeCursor]:
        """Hold the session lock and one cursor for a block: the driver's default, or `cursor_class`.

        The cursor closes before the lock is released, however the block ends. Failures pass
        through as raised; wrap the block in `_as_port_errors` to report them as port errors.

        Raises:
            SnowflakePortError: the session was halted; no cursor is opened.
        """
        with self._lock:
            if self._halted is not None:
                raise SnowflakePortError(self._halted)
            cursor = self._connection.cursor(*cursor_class)
            try:
                yield cursor
            finally:
                cursor.close()

    def _dict_rows(self, sql: Sql) -> tuple[dict[str, Any], ...]:
        """Run one statement without parameters and return its rows keyed by lowercase column name.

        Raises:
            SnowflakePortError: the statement failed, or the driver returned a positional row.
        """
        with _as_port_errors(), self._cursor(DictCursor) as cursor:
            _execute(cursor, sql)
            rows = cursor.fetchall()
            if any(not isinstance(row, dict) for row in rows):
                raise SnowflakePortError("dictionary cursor returned a positional row")
            dictionaries = cast(tuple[dict[Any, Any], ...], rows)
            return tuple({str(key).lower(): value for key, value in row.items()} for row in dictionaries)


class Transaction:
    """The statements of one explicit transaction, run on the cursor that began it.

    `Session._transaction_block` makes one and ends it; until then the session lock is held,
    so no other statement of the session runs inside the transaction.

    Attributes:
        open: Whether the transaction is still open; False once it committed or rolled back.
    """

    def __init__(self, cursor: SnowflakeCursor) -> None:
        self._cursor = cursor
        self.open = True

    def rows(
        self, statement: Sql, params: Sequence[object] | Mapping[str, object] | None = None
    ) -> tuple[tuple[object, ...], ...]:
        """Run one statement with `params` bound, and return its rows."""
        return _fetch(self._cursor, statement, params).rows

    def run(self, statement: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> int:
        """Run one statement with `params` bound, and return the rows it changed."""
        _execute(self._cursor, statement, params)
        return max(self._cursor.rowcount or 0, 0)

    def rollback(self) -> None:
        """Roll the transaction back now: nothing it ran is kept, and the block then commits nothing."""
        self.end(sql("ROLLBACK"))

    def end(self, statement: Sql) -> None:
        """End the transaction with `statement`, COMMIT or ROLLBACK; it counts as ended even if that fails."""
        self.open = False
        _execute(self._cursor, statement)


@contextlib.contextmanager
def _as_port_errors() -> Iterator[None]:
    """Re-raise a driver or transport failure in the block as the port error that reports it.

    The port error's cause is the failure itself; any other exception propagates unwrapped.
    """
    try:
        yield
    except _DRIVER_ERRORS as exc:
        raise _port_error(exc) from exc


def _execute(
    cursor: SnowflakeCursor,
    statement: Sql,
    params: Sequence[object] | Mapping[str, object] | None = None,
) -> None:
    """Hand one statement to the driver with `params` bound: the only place `Sql` becomes text.

    Snowflake is told to run exactly one statement, so text that a reader other than SST's
    lexer splits into several is refused by the server rather than run. An empty `params` binds
    nothing, so the text goes as built rather than with every `%` doubled for formatting the
    driver then skips.

    Raises:
        TypeError: `statement` is not `Sql`, so nothing built from a plain string reaches the driver.
    """
    if not isinstance(statement, Sql):
        raise TypeError(f"the connector runs only Sql, found {type(statement).__name__}")
    bound = bool(params)
    connector_params = cast(Sequence[Any] | dict[Any, Any] | None, params) if bound else None
    cursor.execute(statement.for_driver(bound=bound), connector_params, num_statements=1)


def _fetch(cursor: SnowflakeCursor, sql: Sql, params: Sequence[object] | Mapping[str, object] | None) -> QueryResult:
    """Run one statement on `cursor` with `params` bound, and return its columns and rows."""
    _execute(cursor, sql, params)
    columns = tuple(item[0] for item in (cursor.description or ()))
    rows = tuple(tuple(row) for row in cursor.fetchall()) if cursor.description else ()
    return QueryResult(columns, rows)


def scrubbed_message(exc: BaseException) -> str:
    """Return a failure's message with every credential setting and key-file path replaced.

    A value written after `password`, `token`, `passphrase`, `secret` or `private_key...` (with
    `=` or `:`) and any path ending in a key-file extension, such as `.p8` or `.pem`, become
    `<redacted>`; the rest of the message, its errno and SQLSTATE prefix included, is kept.
    """
    message = _SECRET_SETTING.sub(lambda match: f"{match.group(1)}{match.group(2)}{_REDACTED}", str(exc))
    return _KEY_FILE.sub(_REDACTED, message)


def _port_error(exc: Exception, *, connecting_to: str | None = None) -> SnowflakePortError:
    """Build the port error for a connector failure, with the diagnostic a command reports for it.

    The error keeps the failure's SQLSTATE, errno, and message, scrubbed by `scrubbed_message`;
    nothing else of the failure is copied. What the failure means is the signature table's
    reading of it (`signatures.session_failure`), from its message, errno and SQLSTATE.

    Diagnostics:
        SST-PRT002: Snowflake rejected the session's credential while connecting.
        SST-PRT001: connecting to `connecting_to` failed for another reason.
        SST-PRT003: the connection dropped or the statement timed out or was cancelled.
        SST-PRT004: Snowflake refused the statement for want of a privilege.
    """
    message = scrubbed_message(exc)
    sqlstate = getattr(exc, "sqlstate", None)
    errno = getattr(exc, "errno", None)
    failure = session_failure(message, errno=errno, sqlstate=sqlstate)
    diagnostic: Diagnostic | None = None
    if connecting_to is not None and failure is SessionFailure.AUTHENTICATION:
        diagnostic = D("SST-PRT002", value=connecting_to)
    elif connecting_to is not None:
        diagnostic = D("SST-PRT001", value=connecting_to, detail=message)
    elif failure is SessionFailure.DEADLINE:
        diagnostic = D("SST-PRT003", detail=f"its deadline ({message})")
    elif failure is SessionFailure.PRIVILEGE:
        diagnostic = D("SST-PRT004", value="the session's role", detail=f"a privilege the statement needs ({message})")
    return SnowflakePortError(message, sqlstate=sqlstate, errno=errno, diagnostic=diagnostic)


def _require_ok(result: ExecResult, failure: str) -> None:
    """Raise the error that stopped a script, or `failure` when its result names none.

    Raises:
        SnowflakePortError: the script failed; the error carries only its message.
    """
    if not result.ok:
        raise SnowflakePortError(result.error.message if result.error else failure)


def _json_text(value: object) -> str:
    """Return `value` as the compact, key-sorted JSON text a PARSE_JSON parameter binds."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _variant_value(value: object, default: object) -> object:
    """Decode a VARIANT column as the driver returns it: JSON text, a decoded value, or NULL as `default`."""
    if value is None:
        return default
    if isinstance(value, str):
        return parse_json(value)
    return value
