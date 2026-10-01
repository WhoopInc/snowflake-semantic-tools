"""The connector's session: one driver connection, the lock each statement takes, and SQL execution.

Concurrent evals share one connection, so every statement runs on a cursor opened under the
session lock. A driver or transport failure surfaces as `SnowflakePortError`, carrying the
diagnostic a command reports; any other exception is an SST bug and propagates unwrapped.
The private helpers here are shared by the role modules beside this one.
"""

from __future__ import annotations

import contextlib
import json
import sys
from threading import RLock
from typing import Any, Iterator, Mapping, Sequence, cast

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import Error as DriverError

from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake import ExecutionPort, SnowflakePortError

# What a statement or fetch raises when Snowflake or the network fails: the driver's own
# errors, and the OSError family (its vendored `requests` errors, socket timeouts) that it
# lets escape a large result's chunk download. Anything else is an SST bug, never a
# Snowflake failure, so it propagates unwrapped.
_DRIVER_ERRORS = (DriverError, OSError)


class Session(ExecutionPort):
    """One live driver connection to Snowflake, which every role of the connector runs on.

    Constructing a session connects, and it stays connected until `close`. Each statement
    runs on its own cursor while the session lock is held; statements that must run
    together, such as a USE pair and the statement it scopes, share one cursor and one hold.

    Raises:
        SnowflakePortError: connecting failed; its diagnostic names the account.
    """

    def __init__(self, connection_params: Mapping[str, object]) -> None:
        try:
            # Browser SSO prints its prompts to stdout, which `--output json` reserves
            # for exactly one envelope; the prompts still reach the user on stderr.
            with contextlib.redirect_stdout(sys.stderr):
                self._connection = snowflake.connector.connect(**dict(connection_params))
            self._lock = RLock()
        except Exception as exc:
            raise _port_error(exc, connecting_to=str(connection_params.get("account") or "Snowflake")) from exc

    def close(self) -> None:
        """Close the driver connection, waiting for any statement that holds the session lock.

        A driver failure to close propagates as the driver raised it.
        """
        with self._lock:
            self._connection.close()

    def query(self, sql: str, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        with _as_port_errors(), self._cursor() as cursor:
            return _fetch(cursor, sql, params)

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: str,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        # The USE pair and the statement share one lock hold, so no concurrent statement
        # runs between them; the session keeps `scope` afterwards.
        with _as_port_errors(), self._cursor() as cursor:
            cursor.execute(f"USE DATABASE {scope.database.sql}")
            cursor.execute(f"USE SCHEMA {scope.sql}")
            return _fetch(cursor, sql, params)

    def execute_script(self, statements: Sequence[str]) -> ExecResult:
        query_ids: list[str] = []
        try:
            with self._cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
                    query_ids.append(str(cursor.sfqid or ""))
            return ExecResult(True, tuple(query_ids), rows_affected=0)
        except Exception as exc:
            write_succeeded = bool(query_ids)
            return ExecResult(
                False,
                tuple(query_ids),
                ExecutionError(
                    str(exc),
                    getattr(exc, "sqlstate", None),
                    getattr(exc, "errno", None),
                ),
                rows_affected=(1 if write_succeeded else 0),
            )

    def try_execute(self, sql: str) -> ExecResult:
        return self.execute_script((sql,))

    @contextlib.contextmanager
    def _cursor(self, *cursor_class: type[SnowflakeCursor]) -> Iterator[SnowflakeCursor]:
        """Hold the session lock and one cursor for a block: the driver's default, or `cursor_class`.

        The cursor closes before the lock is released, however the block ends. Failures pass
        through as raised; wrap the block in `_as_port_errors` to report them as port errors.
        """
        with self._lock:
            cursor = self._connection.cursor(*cursor_class)
            try:
                yield cursor
            finally:
                cursor.close()

    def _dict_rows(self, sql: str) -> tuple[dict[str, Any], ...]:
        """Run one statement without parameters and return its rows keyed by lowercase column name.

        Raises:
            SnowflakePortError: the statement failed, or the driver returned a positional row.
        """
        with _as_port_errors(), self._cursor(DictCursor) as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            if any(not isinstance(row, dict) for row in rows):
                raise SnowflakePortError("dictionary cursor returned a positional row")
            dictionaries = cast(tuple[dict[Any, Any], ...], rows)
            return tuple({str(key).lower(): value for key, value in row.items()} for row in dictionaries)


@contextlib.contextmanager
def _as_port_errors() -> Iterator[None]:
    """Re-raise a driver or transport failure in the block as the port error that reports it.

    The port error's cause is the failure itself; any other exception propagates unwrapped.
    """
    try:
        yield
    except _DRIVER_ERRORS as exc:
        raise _port_error(exc) from exc


def _fetch(cursor: SnowflakeCursor, sql: str, params: Sequence[object] | Mapping[str, object] | None) -> QueryResult:
    """Run one statement on `cursor` with `params` bound, and return its columns and rows."""
    connector_params = cast(Sequence[Any] | dict[Any, Any] | None, params)
    cursor.execute(sql, connector_params)
    columns = tuple(item[0] for item in (cursor.description or ()))
    rows = tuple(tuple(row) for row in cursor.fetchall()) if cursor.description else ()
    return QueryResult(columns, rows)


def _port_error(exc: Exception, *, connecting_to: str | None = None) -> SnowflakePortError:
    """Build the port error for a connector failure, with the diagnostic a command reports for it.

    The error keeps the failure's message, SQLSTATE, and errno.

    Diagnostics:
        SST-PRT001: connecting to `connecting_to` failed.
        SST-PRT003: the connection dropped or the statement timed out or was cancelled.
        SST-PRT004: Snowflake refused the statement for want of a privilege.
    """
    message = str(exc)
    sqlstate = getattr(exc, "sqlstate", None)
    state = sqlstate or ""
    upper = message.upper()
    diagnostic: Diagnostic | None = None
    if connecting_to is not None:
        diagnostic = D("SST-PRT001", value=connecting_to, detail=message)
    elif state.startswith("08") or state in {"57014", "57P01"} or "TIMEOUT" in upper:
        diagnostic = D("SST-PRT003", detail=message)
    elif state.startswith("28") or state == "42501" or "INSUFFICIENT PRIVILEGES" in upper:
        diagnostic = D("SST-PRT004", value="the statement", detail=message)
    return SnowflakePortError(
        message,
        sqlstate=sqlstate,
        errno=getattr(exc, "errno", None),
        diagnostic=diagnostic,
    )


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
        return json.loads(value)
    return value
