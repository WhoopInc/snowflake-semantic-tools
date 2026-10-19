"""The connector's session: one driver connection, the lock each statement takes, and SQL execution.

Concurrent evals share one connection, so every statement runs on a cursor opened under the
session lock; parallel apply workers each lease a sibling session from a pool instead.
Statements arrive as `Sql` and become text only in `_execute`, the one place this adapter
hands SQL to the driver. A driver or transport failure surfaces as `SnowflakePortError`,
carrying the diagnostic a command reports; any other exception is an SST bug and propagates
unwrapped. A halted session starts no further statement, and a guarded one runs its guard
before each write statement of a script. No statement here is a USE: a scope
is a session's own, set when it connects. The private helpers here are shared by the role
modules beside this one. The driver is imported the first time a session needs it, never when
this module is: importing it costs most of a second, which a command that never connects
(`sst explain`, `sst compile`) must not pay.

While a `ReadCache` is in use, as it is for one plan's reads, a SHOW, DESCRIBE or LIST that
was answered once is answered again from what it returned, and a statement of any other
verb, but for SELECT, EXPLAIN and GET, forgets every answer first, so nothing read before a
write answers a read after it.
"""

from __future__ import annotations

import contextlib
import json
import re
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from threading import Event, Lock, RLock, get_ident
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Self, TextIO, TypeVar, cast

from snowflake_semantic_tools.adapters.json_files import parse_json
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import SessionFailure, match_signature, session_failure
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import (
    ScopeUnavailable,
    SnowflakePortError,
    SnowflakeStatementTimedOut,
    SnowflakeTransientError,
)
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import Sql, sql
from snowflake_semantic_tools.domain.state.lock import STANDARD_TABLES, TABLE_KIND_SETTING

if TYPE_CHECKING:
    from snowflake.connector.cursor import SnowflakeCursorBase


def _driver_errors() -> tuple[type[Exception], ...]:
    """What a statement or fetch raises when Snowflake or the network fails.

    The driver's own errors, and the OSError family (its vendored `requests` errors, socket
    timeouts) that it lets escape a large result's chunk download. Anything else is an SST bug,
    never a Snowflake failure, so it propagates unwrapped. An `except` clause evaluates this only
    once something was raised, so the driver is imported no sooner than a statement needs it.
    """
    from snowflake.connector.errors import Error as DriverError

    return (DriverError, OSError)


# What a driver or key library may echo into an error that must never reach a diagnostic: a
# credential written as a `name=value` or `name: value` setting, and the path to a key file.
_SECRET_SETTING = re.compile(
    r"(?i)\b([\w-]*(?:password|passwd|pwd|token|passphrase|secret|private_key)[\w-]*)"
    r"([\"']?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&)\]}]+)"
)
_KEY_FILE = re.compile(r"(?i)(?:[A-Za-z]:)?[\w.~-]*(?:[/\\][^\s/\\'\"]+)+\.(?:p8|pem|key|der|p12|pfx)\b")
_REDACTED = "<redacted>"
# The failures of a statement Snowflake took but did not finish in time: a statement timeout
# (SQLSTATE 57014, whether the client's or Snowflake's), and a statement that queued too long.
_TIMED_OUT = frozenset(("SST-SNO011", "SST-SNO024"))

# The verbs whose answers a `ReadCache` keeps, and those that write nothing it must forget.
_CACHED_VERBS = frozenset(("SHOW", "DESCRIBE", "DESC", "LIST"))
_READ_VERBS = _CACHED_VERBS | {"SELECT", "EXPLAIN", "GET"}

ValueT = TypeVar("ValueT")


class _LoginStdout:
    """Send stdout to stderr while any session logs in, however many log in at once.

    `sys.stdout` is the whole process's, and pool sessions log in on worker threads at the same
    time. One `contextlib.redirect_stdout` each would interleave: a login entering while another
    is in flight saves stderr as the stream to restore, and the last to leave restores it, so the
    command's `--output json` envelope would land on stderr. Here the first login to enter saves
    the real stdout and the last to leave restores it, under one lock; logins still run at once.
    """

    def __init__(self) -> None:
        self._guard = Lock()
        self._logging_in = 0
        # The stream to restore; it is read when the first login enters.
        self._saved: TextIO = sys.stdout

    @contextlib.contextmanager
    def redirected(self) -> Iterator[None]:
        """Hold stdout on stderr for the block, restoring it when no other login is in flight."""
        with self._guard:
            if self._logging_in == 0:
                self._saved, sys.stdout = sys.stdout, sys.stderr
            self._logging_in += 1
        try:
            yield
        finally:
            with self._guard:
                self._logging_in -= 1
                if self._logging_in == 0:
                    sys.stdout = self._saved


_LOGIN_STDOUT = _LoginStdout()


class _Reading:
    """A read of one key in flight: who reads it, and when it ends."""

    def __init__(self) -> None:
        self.thread = get_ident()
        self.ended = Event()


class ReadCache:
    """What the reads of one run answered, by what was read, shared by the sessions it reads on.

    A key is read once at a time: whoever asks for it while it is being read waits for that
    read and takes its answer. Only an answer is kept: when the read raised, the next asker
    reads it again, one waiting asker after another. A read that was in flight when `forget`
    ran keeps nothing, since what it read may predate the write that made the cache forget.
    """

    def __init__(self) -> None:
        self._answers: dict[str, object] = {}
        self._reading: dict[str, _Reading] = {}
        # Bumped by every `forget`, so a read that started before one keeps no answer.
        self._generation = 0
        self._guard = Lock()

    def answer(self, key: str, read: Callable[[], ValueT]) -> ValueT:
        """Return what `read` answered for `key` before, else read it now and keep the answer.

        Waits while another thread reads `key`. A thread already reading `key` that asks for it
        again reads it anew rather than waiting on itself.
        """
        while True:
            with self._guard:
                if key in self._answers:
                    return cast(ValueT, self._answers[key])
                pending = self._reading.get(key)
                if pending is not None and pending.thread == get_ident():
                    return read()
                if pending is None:
                    pending = self._reading[key] = _Reading()
                    generation = self._generation
                    break
            pending.ended.wait()
        try:
            value = read()
            with self._guard:
                if self._generation == generation:
                    self._answers[key] = value
        finally:
            with self._guard:
                del self._reading[key]
            pending.ended.set()
        return value

    def forget(self) -> None:
        """Forget every answer, as a write must, so the next read of anything reads it anew."""
        with self._guard:
            self._answers.clear()
            self._generation += 1


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
    # The answers this session's reads are kept in; None while every read reads anew.
    _read_cache: ReadCache | None = None
    # What runs before each write statement of a script; None while writes are not guarded.
    _write_guard: Callable[[], None] | None = None
    # The kind of table the state and lock tables are, `standard` or `hybrid`, as the settings'
    # `TABLE_KIND_SETTING` names it; the driver never sees that setting.
    _state_table_kind: str = STANDARD_TABLES

    def __init__(self, connection_params: Mapping[str, object]) -> None:
        failure: SnowflakePortError | None = None
        self._connection_params = MappingProxyType(dict(connection_params))
        self._scoped_guard = Lock()
        driver_params = dict(connection_params)
        self._state_table_kind = str(driver_params.pop(TABLE_KIND_SETTING, STANDARD_TABLES))
        try:
            # Browser SSO prints its prompts to stdout, which `--output json` reserves
            # for exactly one envelope; the prompts still reach the user on stderr. Pool
            # sessions log in at once, so the redirect is the shared `_LOGIN_STDOUT`'s.
            import snowflake.connector

            with _LOGIN_STDOUT.redirected(), RUN_TIMINGS.login():
                self._connection = snowflake.connector.connect(**driver_params)
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

    def guard_writes(self, guard: Callable[[], None] | None) -> None:
        """Call `guard` before each write statement a script starts, as `WriteGuardPort` says; None stops.

        Apply guards each worker's session with a read of its run lock, so a run whose lock was
        broken while it paused writes no further statement, not even within one publish.
        """
        self._write_guard = guard

    def keep_reads(self, cache: ReadCache | None) -> None:
        """Keep this session's read answers in `cache` from now on, as `ReadCache` says; None reads anew.

        Every session one run reads on is given the same cache for that run's reads alone, and
        never for a write that depends on them.
        """
        self._read_cache = cache

    def query(self, sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        self._forget_unless_read(sql)
        with _as_port_errors(), self._cursor() as cursor:
            return _fetch(cursor, sql, params)

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: Sql,
        params: Sequence[object] | Mapping[str, object] | None = None,
        *,
        timeout_seconds: int | None = None,
    ) -> QueryResult:
        # The scoped session runs nothing but these calls, all in the one scope it connected
        # with, so no statement ever changes the scope another statement runs in.
        self._forget_unless_read(sql)
        session = self._scoped_session(scope)
        with _as_port_errors(), session._cursor() as cursor:
            return _fetch(cursor, sql, params, timeout_seconds=timeout_seconds)

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        if self._read_cache is not None:
            self._read_cache.forget()
        completed: list[str] = []
        rows = 0
        halted = self._halted
        try:
            if halted is None:
                with self._cursor() as cursor:
                    for statement in statements:
                        self._guard(statement)
                        if (halted := self._halted) is not None:
                            break
                        _execute(cursor, statement)
                        completed.append(str(cursor.sfqid or ""))
                        rows += max(cursor.rowcount or 0, 0)
        except _driver_errors() as exc:
            # Only the statements before the failing one completed; the result names exactly
            # those, and never counts the one that failed as written.
            error = ExecutionError(scrubbed_message(exc), getattr(exc, "sqlstate", None), getattr(exc, "errno", None))
            return ExecResult(False, tuple(completed), error, rows_affected=rows)
        except SnowflakePortError as exc:
            # The guard could not read what it checks, or the session was halted on the way.
            error = ExecutionError(str(exc), exc.sqlstate, exc.errno)
            return ExecResult(False, tuple(completed), error, rows_affected=rows)
        if halted is not None:
            return ExecResult(False, tuple(completed), ExecutionError(halted), rows_affected=rows)
        return ExecResult(True, tuple(completed), rows_affected=rows)

    def try_execute(self, sql: Sql) -> ExecResult:
        return self.execute_script((sql,))

    def _guard(self, statement: Sql) -> None:
        """Run the write guard before a statement that may write; a read is never guarded."""
        guard = self._write_guard
        if guard is not None and _verb(statement) not in _READ_VERBS:
            guard()

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
            ScopeUnavailable: the session did not start in `target`, as when the database or
                schema does not exist or the role cannot use it.
            SnowflakePortError: connecting failed.
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
            raise ScopeUnavailable(f"a session for {target.sql} started in {shown}")
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
        if self._read_cache is not None:
            self._read_cache.forget()
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
    def _cursor(self, *cursor_class: type[SnowflakeCursorBase[Any]]) -> Iterator[SnowflakeCursorBase[Any]]:
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

    def _cached(self, key: str, read: Callable[[], ValueT]) -> ValueT:
        """Return `read()`, answered from the read cache by `key` while one is in use."""
        cache = self._read_cache
        return read() if cache is None else cache.answer(key, read)

    def _forget_unless_read(self, statement: Sql) -> None:
        """Forget every cached answer before a statement that may write, as `ReadCache` says."""
        if self._read_cache is not None and _verb(statement) not in _READ_VERBS:
            self._read_cache.forget()

    def _dict_rows(self, sql: Sql) -> tuple[dict[str, Any], ...]:
        """Run one statement without parameters and return its rows keyed by lowercase column name.

        A SHOW, DESCRIBE or LIST is answered from the read cache while one is in use.

        Raises:
            SnowflakePortError: the statement failed, or the driver returned a positional row.
        """
        if _verb(sql) in _CACHED_VERBS:
            return self._cached(str(sql), lambda: self._read_dict_rows(sql))
        self._forget_unless_read(sql)
        return self._read_dict_rows(sql)

    def _read_dict_rows(self, sql: Sql) -> tuple[dict[str, Any], ...]:
        """Run one statement without parameters and return its rows keyed by lowercase column name."""
        from snowflake.connector import DictCursor

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

    def __init__(self, cursor: SnowflakeCursorBase[Any]) -> None:
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


def _verb(statement: Sql) -> str:
    """Return a statement's first word, uppercased: the verb `ReadCache` decides by."""
    words = str(statement).split(None, 1)
    return words[0].upper() if words else ""


@contextlib.contextmanager
def _as_port_errors() -> Iterator[None]:
    """Re-raise a driver or transport failure in the block as the port error that reports it.

    The port error's cause is the failure itself; any other exception propagates unwrapped.
    """
    try:
        yield
    except _driver_errors() as exc:
        raise _port_error(exc) from exc


def _execute(
    cursor: SnowflakeCursorBase[Any],
    statement: Sql,
    params: Sequence[object] | Mapping[str, object] | None = None,
    *,
    timeout_seconds: int | None = None,
) -> None:
    """Hand one statement to the driver with `params` bound: the only place `Sql` becomes text.

    Snowflake is told to run exactly one statement, so text that a reader other than SST's
    lexer splits into several is refused by the server rather than run. An empty `params` binds
    nothing, so the text goes as built rather than with every `%` doubled for formatting the
    driver then skips. A `timeout_seconds` bounds the statement end to end: the driver cancels
    it once that long has passed, and gives up retrying its requests over a failing network
    then too, rather than after the connection's own network timeout. Every statement handed
    over counts as one round trip in `RUN_TIMINGS`, whether or not it succeeds.

    Raises:
        TypeError: `statement` is not `Sql`, so nothing built from a plain string reaches the driver.
    """
    if not isinstance(statement, Sql):
        raise TypeError(f"the connector runs only Sql, found {type(statement).__name__}")
    bound = bool(params)
    connector_params = cast(Sequence[Any] | dict[Any, Any] | None, params) if bound else None
    with RUN_TIMINGS.round_trip():
        cursor.execute(statement.for_driver(bound=bound), connector_params, num_statements=1, timeout=timeout_seconds)


def _fetch(
    cursor: SnowflakeCursorBase[Any],
    sql: Sql,
    params: Sequence[object] | Mapping[str, object] | None,
    *,
    timeout_seconds: int | None = None,
) -> QueryResult:
    """Run one statement on `cursor` with `params` bound, and return its columns and rows."""
    _execute(cursor, sql, params, timeout_seconds=timeout_seconds)
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

    The error is a `SnowflakeTransientError` when the failure happened in transit, as
    `_in_transit` reads it, so a caller may send a read again; a `SnowflakeStatementTimedOut`
    when that failure is a statement Snowflake took but did not finish in time.

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
        diagnostic = D("SST-PRT002", value=connecting_to, detail=message)
    elif connecting_to is not None:
        diagnostic = D("SST-PRT001", value=connecting_to, detail=message)
    elif failure is SessionFailure.DEADLINE:
        diagnostic = D("SST-PRT003", detail=f"its deadline ({message})")
    elif failure is SessionFailure.PRIVILEGE:
        diagnostic = D("SST-PRT004", value="the session's role", detail=f"a privilege the statement needs ({message})")
    error_class: type[SnowflakePortError] = SnowflakePortError
    if _in_transit(exc, failure, errno):
        timed_out = match_signature(message, errno=errno, sqlstate=sqlstate).code in _TIMED_OUT
        error_class = SnowflakeStatementTimedOut if timed_out else SnowflakeTransientError
    return error_class(message, sqlstate=sqlstate, errno=errno, diagnostic=diagnostic)


def _in_transit(exc: Exception, failure: SessionFailure | None, errno: object) -> bool:
    """Report whether a connector failure happened on the way to or from Snowflake.

    So it did when the transport raised (an `OSError`: a socket or HTTP failure), when the
    signature table reads it as a missed deadline (a dropped connection, a timeout, a busy
    warehouse), or when it carries the driver's number for a request it gave up on.
    """
    from snowflake.connector.errorcode import ER_FAILED_TO_REQUEST

    return isinstance(exc, OSError) or failure is SessionFailure.DEADLINE or errno == ER_FAILED_TO_REQUEST


def _require_ok(result: ExecResult, failure: str) -> None:
    """Raise the error that stopped a script, or `failure` when its result names none.

    Raises:
        SnowflakePortError: the script failed; the error carries its message, SQLSTATE and
            number, so a command classifies it by number rather than by its wording.
    """
    if result.ok:
        return
    if result.error is None:
        raise SnowflakePortError(failure)
    raise SnowflakePortError(result.error.message, sqlstate=result.error.sqlstate, errno=result.error.errno)


def _headline(message: str) -> str:
    """Return a message's first non-empty line, trimmed, with the next one when the first only opens it.

    Snowflake writes what is wrong on the line after a heading such as `SQL compilation error:`,
    so a first line ending in a colon is kept together with the next non-empty line.
    """
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    if not lines:
        return ""
    if lines[0].endswith(":") and len(lines) > 1:
        return f"{lines[0]} {lines[1]}"
    return lines[0]


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
