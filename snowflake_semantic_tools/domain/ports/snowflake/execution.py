"""The port use cases run SQL through, and the pool that gives parallel workers their own sessions.

Scope guarantee: no statement changes the scope another statement runs in. Every statement SST
builds names its objects fully qualified, so it means the same thing whatever the session's
current database and schema are; authored SQL that resolves unqualified names goes through
`query_in_context`, which runs it on a dedicated session scoped for that call alone.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Protocol, TypeVar

from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult
from snowflake_semantic_tools.domain.sql import Sql

PortT = TypeVar("PortT", covariant=True)


class ExecutionPort(Protocol):
    """Run SQL on the session: a statement whose rows the caller reads, or a script of writes.

    `query` and `query_in_context` raise when a statement fails; `execute_script` and
    `try_execute` report the failure in their result instead.
    """

    def query(self, sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        """Run one statement with `params` bound to its placeholders, and return its rows.

        Writes what the statement writes, and nothing else.

        Returns:
            The column names and rows; both empty for a statement that returns no rows.

        Raises:
            SnowflakePortError: the statement failed, or Snowflake could not be reached.
        """
        ...

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: Sql,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        """Run one statement as `query` does, resolving its unqualified names in `scope`.

        For authored SQL, such as a verified query, that names objects relative to a schema.
        The statement runs on a session kept for scoped calls: `scope` is made current on it
        for this call, and no other statement, on this session or any other, runs in it or
        sees its scope.

        Raises:
            SnowflakePortError: switching to `scope` or the statement failed.
        """
        ...

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        """Run statements in order on one session, one at a time, stopping at the first failure.

        Never raises for a SQL error; the result carries it. Any other exception propagates.

        Args:
            statements: Complete statements, each executed separately (never split on ``;``).

        Returns:
            ``ok=True`` with one query id per statement, or ``ok=False`` with the error and one
            query id for each statement that completed before it, in order, and nothing for the
            one that failed; `rows_affected` sums what the completed statements reported.
        """
        ...

    def try_execute(self, sql: Sql) -> ExecResult:
        """Run one statement as a one-statement script, which reports a failure in its result.

        Never raises for a SQL error, as `execute_script` does not.
        """
        ...


class SessionPool(Protocol[PortT]):
    """Sessions for parallel workers: each lease is a session no other worker uses meanwhile.

    Every session is opened from the same connection settings as the one the pool was made
    from, and shares nothing mutable with it; the pool's owner closes them all.
    """

    def lease(self) -> AbstractContextManager[PortT]:
        """Borrow one session for a block, opening one when every open session is leased.

        Raises:
            SnowflakePortError: a new session could not be opened.
        """
        ...
