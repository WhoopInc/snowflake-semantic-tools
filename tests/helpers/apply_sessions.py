"""Apply over pooled sessions held in their statement: the ports, pool and use case the apply tests share."""

from __future__ import annotations

import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.apply.lock import LockPolicy
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import rendered
from tests.helpers.clocks import FixedClock, SteppedTicker
from tests.helpers.snowflake_fake import FakeSnowflake

STATE_TABLE = rendered("SST_STATE").target


def stepped(ticker: SteppedTicker | None = None, *, ttl: int = 60) -> LockPolicy:
    """A policy whose heartbeat beats only when `ticker` is stepped; one beat is one second."""
    made = ticker or SteppedTicker()
    return LockPolicy(ttl_seconds=ttl, heartbeat_seconds=1, ticker=lambda: made)


class RunClock(FixedClock):
    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def new_run_id(self) -> str:
        return self.run_id


class SlowSnowflake(FakeSnowflake):
    """Holds each script until `release` is set, and records how many ran on it at once."""

    def __init__(self, release: threading.Event | None = None) -> None:
        super().__init__()
        self.release = release
        self.entered = threading.Event()
        self.running = 0
        self.most_at_once = 0
        self._guard = threading.Lock()

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        with self._guard:
            self.running += 1
            self.most_at_once = max(self.most_at_once, self.running)
        self.entered.set()
        try:
            if self.release is not None:
                assert self.release.wait(timeout=5)
            return super().execute_script(statements)
        finally:
            with self._guard:
                self.running -= 1


class ListPool:
    """A session pool over fixed ports, recording which change ran on which port, and its halt."""

    def __init__(self, ports: Sequence[FakeSnowflake]) -> None:
        self.idle = list(ports)
        self.guard = threading.Lock()
        self.leases = 0
        self.halted: str | None = None

    @contextmanager
    def lease(self) -> Iterator[CatalogPublicationPort]:
        with self.guard:
            port = self.idle.pop()
            self.leases += 1
        try:
            yield port
        finally:
            with self.guard:
                self.idle.append(port)

    def halt(self, reason: str) -> None:
        self.halted = reason


def apply_with(
    port: FakeSnowflake,
    run_id: str,
    *,
    store: InMemoryStateStore | None = None,
    sessions: object = None,
    heartbeat: FakeSnowflake | None = None,
    policy: LockPolicy | None = None,
) -> ApplyArtifacts:
    return ApplyArtifacts(
        port,
        store or InMemoryStateStore(),
        RunClock(run_id),
        state_table=STATE_TABLE,
        actor="ROLE",
        host="host-" + run_id,
        sessions=sessions,  # type: ignore[arg-type]
        heartbeat=heartbeat,
        lock_policy=policy or stepped(),
    )
