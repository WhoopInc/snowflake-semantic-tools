"""Recover an apply run whose session Snowflake ended mid-run: one fresh connection, and SST-APL103.

A session Snowflake ended or let expire -- the network dropped, the session was aborted or timed
out -- fails every statement sent on it afterwards, the state write and the lock release among
them, though the account is still reachable: Snowflake asks for a new login. The run then opens
one fresh connection, through its `Reconnect`, for the fenced state write of what finished and
the fenced release of its lock, so the target is neither left locked nor left without a record
of what the run wrote, and the run reports the loss as SST-APL103 rather than failing to connect.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from contextlib import AbstractContextManager

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import SessionFailure, session_failure
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOutcome
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort

# Open a fresh connection for the block, closed when it ends.
Reconnect = Callable[[], AbstractContextManager[StatePort]]


def session_gone(error: SnowflakePortError) -> bool:
    """Report whether a port error is a session Snowflake ended or let expire."""
    return session_failure(str(error), errno=error.errno, sqlstate=error.sqlstate) is SessionFailure.SESSION_GONE


def _outcome_lost_session(outcome: ApplyOutcome) -> bool:
    """Report whether a change failed because Snowflake ended the session it ran on."""
    error = outcome.error
    return error is not None and session_failure(error.message, sqlstate=error.sqlstate) is SessionFailure.SESSION_GONE


class Reconnection:
    """The one fresh connection a run opens, the first time its own session is found gone.

    `recover` opens it, at most once, and hands it out again after; `close` closes it.

    Args:
        reconnect: Opens the fresh connection; None means the run cannot open one, so a lost
            session's error propagates as it would without recovery.
    """

    def __init__(self, reconnect: Reconnect | None) -> None:
        self._reconnect = reconnect
        self._opened = contextlib.ExitStack()
        self._port: StatePort | None = None

    @property
    def used(self) -> bool:
        """Whether the run had to open the fresh connection."""
        return self._port is not None

    def recover(self, error: SnowflakePortError) -> StatePort:
        """Return the fresh connection to send again what `error` failed; raise `error` when that cannot help.

        Raises:
            SnowflakePortError: `error` itself, when it is not a session Snowflake ended or the
                run cannot reconnect; or the failure to open the fresh connection.
        """
        if self._reconnect is None or not session_gone(error):
            raise error
        if self._port is None:
            self._port = self._opened.enter_context(self._reconnect())
        return self._port

    def close(self) -> None:
        """Close the fresh connection, when one was opened; idempotent."""
        self._opened.close()


def connection_lost(outcomes: tuple[ApplyOutcome, ...], *, reconnected: bool) -> tuple[Diagnostic, ...]:
    """Report a run whose session Snowflake ended mid-run, naming what finished; nothing otherwise.

    A change finished when its statements returned, so state records it; one that was cut off
    is not, and the next plan sees what it wrote.

    Diagnostics:
        SST-APL103: the run reconnected, or a change failed because its session was gone.
    """
    if not reconnected and not any(_outcome_lost_session(outcome) for outcome in outcomes):
        return ()
    finished = [outcome.key for outcome in outcomes if outcome.write_succeeded]
    return (D("SST-APL103", value=", ".join(finished) or "nothing"),)
