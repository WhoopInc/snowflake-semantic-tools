"""The port use cases run SQL through: a query whose rows they read, or a script of writes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult
from snowflake_semantic_tools.domain.sql import Sql


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
        """Run one statement as `query` does, with `scope` as the session's current schema.

        For SQL that resolves unqualified names against the current schema. `scope` may stay
        current on the session afterwards.

        Raises:
            SnowflakePortError: switching to `scope` or the statement failed.
        """
        ...

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        """Run statements in order on one session, stopping at the first failure.

        Never raises for a SQL error; the result carries it.

        Args:
            statements: Complete statements, each executed separately (never split on ``;``).

        Returns:
            ``ok=True`` with one query id per statement, or ``ok=False`` with the error and the
            ids of the statements that already ran, which the caller must treat as a partial write.
        """
        ...

    def try_execute(self, sql: Sql) -> ExecResult:
        """Run one statement as a one-statement script, which reports a failure in its result.

        Never raises for a SQL error, as `execute_script` does not.
        """
        ...
