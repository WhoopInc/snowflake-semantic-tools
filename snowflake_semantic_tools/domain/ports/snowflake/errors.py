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


class AgentVersionNotFound(SnowflakePortError):
    """An agent version selector names no committed version: it was never created, or was dropped."""
