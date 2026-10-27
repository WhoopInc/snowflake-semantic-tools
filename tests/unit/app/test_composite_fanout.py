"""`PlanArtifacts.composite_plans` on concurrent sessions: the same plans as one at a time.

Each composite plan here waits a random moment first, so concurrent plans finish in a different
order each run; the plans, their key order, and their diagnostics must not depend on that order,
and each concurrent plan must run on the session leased for it, not on the command's.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.lifecycle.composite import unobservable
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import (
    ApplyOptions,
    ApplyOutcome,
    Change,
    CompositePlan,
    RecoveryProof,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry, AppliedResourceInput, Manifest
from tests.helpers.artifact_builders import manifest, rendered, state
from tests.helpers.snowflake_fake import FakeSnowflake


class _Session(FakeSnowflake):
    """A session a handler can be bound to, named so a plan says which one it ran on."""

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name


class _Handler:
    """A composite handler whose plan waits a moment, then reports the session it ran on."""

    artifact_type = "eval"

    def __init__(self, session: _Session, seen: list[str], guard: threading.Lock) -> None:
        self._session = session
        self._seen = seen
        self._guard = guard

    def for_session(self, session: object) -> _Handler:
        assert isinstance(session, _Session)
        return _Handler(session, self._seen, self._guard)

    def plan(self, artifact: RenderedArtifact, state_entry: AppliedEntry | None, manifest: Manifest) -> CompositePlan:
        del state_entry, manifest
        time.sleep(random.uniform(0, 0.004))
        with self._guard:
            self._seen.append(self._session.name)
        return unobservable(artifact.key, SnowflakePortError(f"{artifact.key} read refused"))

    # Planning composites reaches a handler only through `for_session` and `plan`.
    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        raise NotImplementedError

    def merge_physical_resources(
        self, current: tuple[tuple[str, str], ...], previous: AppliedEntry | None
    ) -> tuple[AppliedResourceInput, ...]:
        raise NotImplementedError

    def recover(self, change: Change) -> RecoveryProof:
        raise NotImplementedError

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        raise NotImplementedError


class _Pool:
    """Lends a fresh named session for each lease, so a plan's session can be told apart."""

    def __init__(self) -> None:
        self._count = 0
        self._guard = threading.Lock()

    @contextmanager
    def lease(self) -> Iterator[_Session]:
        with self._guard:
            self._count += 1
            name = f"sibling{self._count}"
        yield _Session(name)

    def halt(self, reason: str) -> None:
        del reason


def _evals() -> dict[str, RenderedArtifact]:
    artifacts = [replace(rendered(f"E{index}"), key=f"eval:e{index}", artifact_type="eval") for index in range(8)]
    return {artifact.key: artifact for artifact in artifacts}


def _planned(workers: int) -> tuple[dict[str, CompositePlan], list[str]]:
    command = _Session("command")
    seen: list[str] = []
    handler = _Handler(command, seen, threading.Lock())
    artifacts = _evals()
    planner = PlanArtifacts(command, lifecycle_handlers={"eval": handler}, readers=Fanout(command, _Pool(), workers))
    return planner.composite_plans(artifacts, manifest(artifacts), state()), seen


def test_concurrent_composite_plans_are_the_serial_ones_whatever_order_they_finish_in() -> None:
    serial, serial_seen = _planned(1)
    assert list(serial) == list(_evals())
    assert serial_seen == ["command"] * 8
    for _ in range(5):
        concurrent, seen = _planned(4)
        assert concurrent == serial
        assert list(concurrent) == list(serial)
        # Every plan ran on a leased session; none on the command's own.
        assert len(seen) == 8 and "command" not in seen


def test_composite_plans_without_readers_run_on_the_handlers_own_session() -> None:
    command = _Session("command")
    seen: list[str] = []
    artifacts = _evals()
    planner = PlanArtifacts(command, lifecycle_handlers={"eval": _Handler(command, seen, threading.Lock())})
    assert planner.composite_plans(artifacts, manifest(artifacts), state()) == _planned(1)[0]
    assert seen == ["command"] * 8
