"""A Snowflake double whose session Snowflake ends part way through an apply, and the fresh one it reconnects to."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Sequence

from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.snowflake_fake import FakeSnowflake

GONE = "390111 (08001): Session no longer exists.  New login required to access the service."


class DroppedSession(FakeSnowflake):
    """Runs `scripts` scripts, then fails every statement as a session Snowflake ended does."""

    def __init__(self, scripts: int) -> None:
        super().__init__()
        self.remaining = scripts

    @property
    def gone(self) -> bool:
        return self.remaining < 0

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        self.remaining -= 1
        if self.gone:
            return ExecResult(False, error=ExecutionError(GONE, "08001", 390111))
        return super().execute_script(statements)

    def _check(self, method: str) -> None:
        if self.gone and method in ("write_state", "release_run_lock", "verify_run_lock", "extend_run_lock"):
            raise SnowflakePortError(GONE, errno=390111, sqlstate="08001")
        super()._check(method)


class Reconnects:
    """Opens a fresh session on the same account as `dropped`, counting how many it opened."""

    def __init__(self, dropped: FakeSnowflake) -> None:
        self.fresh = FakeSnowflake()
        self.fresh.run_locks = dropped.run_locks
        self.opened = 0

    @contextlib.contextmanager
    def __call__(self) -> Iterator[FakeSnowflake]:
        self.opened += 1
        yield self.fresh
