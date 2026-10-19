"""The error every Snowflake port role raises when Snowflake refuses, or its answer is unusable."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic


class SnowflakePortError(RuntimeError):
    """A port operation Snowflake refused or could not be reached for, or whose answer SST cannot use.

    A command that lets one escape exits as a connection failure, reporting `diagnostic`
    when there is one and the message otherwise.

    Attributes:
        sqlstate: The SQLSTATE of the driver failure behind the error; None when there is none.
        errno: The driver's error number for that failure; None when there is none.
        diagnostic: What the command reports for the failure; None when the adapter did not
            recognise it.
    """

    def __init__(
        self,
        message: str,
        *,
        sqlstate: str | None = None,
        errno: int | None = None,
        diagnostic: Diagnostic | None = None,
    ) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.errno = errno
        # What the command reports, when the adapter recognised the failure.
        self.diagnostic = diagnostic


class SnowflakeTransientError(SnowflakePortError):
    """A statement that failed on the way to or from Snowflake, rather than in it.

    The connection dropped or could not be made, the request or statement timed out, or
    Snowflake was too busy to run it. Snowflake may or may not have run the statement, so only
    a read is safe to send again; the same read may then succeed. Anything Snowflake answered
    and SST cannot use -- a refused statement, a row describing another object -- is a plain
    `SnowflakePortError`, never this.
    """


class SnowflakeStatementTimedOut(SnowflakeTransientError):
    """A statement that ran past its timeout, so the timeout cancelled it.

    The caller's timeout or Snowflake's own cancelled it (SQLSTATE 57014), or it queued past
    the warehouse's queue timeout. Most often Snowflake took the statement and is slow -- a
    busy warehouse -- rather than out of reach: a dropped connection fails as a plain
    `SnowflakeTransientError` instead.
    """


class AgentVersionNotFound(SnowflakePortError):
    """An agent version selector names no committed version: it was never created, or was dropped."""


class ScopeUnavailable(SnowflakePortError):
    """A session for `query_in_context` that did not start in its database and schema.

    The schema or database does not exist, or the role cannot use it; nothing was run. A
    schema apply has yet to create is one, so a caller that can run the statement without
    the scope may do so.
    """
