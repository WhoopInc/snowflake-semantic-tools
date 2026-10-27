"""How apply schedules a plan's changes: each starts once those it depends on ended, never at a wave's end.

Whatever order the changes end in, the run records the same outcomes and diagnostics, in the
same order, as one that ran them one at a time.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.apply.errors import _failed
from snowflake_semantic_tools.app.apply.waves import _WaveRun
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    ApplyOptions,
    ApplyOutcome,
    Change,
    ClassifiedError,
    ErrorKind,
    FailurePolicy,
    OutcomeStatus,
)
from tests.helpers.apply_sessions import ListPool, apply_with
from tests.helpers.artifact_builders import change, changeset, rendered, state
from tests.helpers.snowflake_fake import FakeSnowflake


def _change(name: str, *depends_on: str) -> Change:
    return replace(change(rendered(name)), depends_on=tuple(f"semantic_view:{item}" for item in depends_on))


# Three independent roots, a diamond under them, and a chain past it: four waves.
PLAN = (
    _change("a"),
    _change("b"),
    _change("c"),
    _change("d", "a", "b"),
    _change("e", "b"),
    _change("f", "d", "e"),
    _change("g", "f"),
    _change("h", "c"),
)


class _Applier:
    """Apply each change after a random pause, failing the keys it is told to fail."""

    def __init__(self, seed: int, failing: frozenset[str] = frozenset()) -> None:
        self._random = random.Random(seed)
        self._failing = failing
        self._guard = threading.Lock()
        self.ran: list[str] = []

    def __call__(self, item: Change, options: ApplyOptions) -> ApplyOutcome:
        del options
        with self._guard:
            pause = self._random.uniform(0, 0.02)
        time.sleep(pause)
        with self._guard:
            self.ran.append(item.key)
        if item.key in self._failing:
            return _failed(item, ClassifiedError("SST-APL001", f"{item.key} failed", ErrorKind.UNKNOWN), "")
        return ApplyOutcome(item.key, item.action, OutcomeStatus.APPLIED, 1, 1, "", write_succeeded=True)


def _record(
    seed: int, parallelism: int, policy: FailurePolicy, failing: frozenset[str] = frozenset()
) -> tuple[list[tuple[str, OutcomeStatus]], list[tuple[str, str]]]:
    options = ApplyOptions(parallelism=parallelism, on_failure=policy)
    outcomes, diagnostics = _WaveRun(changeset(*PLAN), options, _Applier(seed, failing)).run()
    return [(item.key, item.status) for item in outcomes], [(item.code, item.message) for item in diagnostics]


@pytest.mark.parametrize("policy", [FailurePolicy.STOP_DEPENDENTS, FailurePolicy.CONTINUE])
@pytest.mark.parametrize("failing", [frozenset(), frozenset({"semantic_view:b"}), frozenset({"semantic_view:f"})])
def test_the_record_is_the_same_however_the_changes_interleave(policy: FailurePolicy, failing: frozenset[str]) -> None:
    serial = _record(0, 1, policy, failing)
    for seed in range(12):
        assert _record(seed, 4, policy, failing) == serial


def test_the_record_follows_the_waves_skipped_first_then_ran_each_in_key_order() -> None:
    outcomes, diagnostics = _record(3, 4, FailurePolicy.STOP_DEPENDENTS, frozenset({"semantic_view:b"}))
    applied, failed, skipped = OutcomeStatus.APPLIED, OutcomeStatus.FAILED, OutcomeStatus.SKIPPED
    assert outcomes == [
        ("semantic_view:a", applied),
        ("semantic_view:b", failed),
        ("semantic_view:c", applied),
        ("semantic_view:d", skipped),
        ("semantic_view:e", skipped),
        ("semantic_view:h", applied),
        ("semantic_view:f", skipped),
        ("semantic_view:g", skipped),
    ]
    # A skip propagates: f is blocked by the skipped d and e, and g by the skipped f.
    assert [code for code, _ in diagnostics] == ["SST-APL001", "SST-APL002", "SST-APL002", "SST-APL002", "SST-APL002"]
    assert [message for _, message in diagnostics][1:] == [
        message for _, message in _record(0, 1, FailurePolicy.STOP_DEPENDENTS, frozenset({"semantic_view:b"}))[1][1:]
    ]


def test_continue_runs_every_change_whatever_failed() -> None:
    outcomes, _ = _record(5, 4, FailurePolicy.CONTINUE, frozenset({"semantic_view:b"}))
    assert [status for _, status in outcomes].count(OutcomeStatus.SKIPPED) == 0


def test_a_change_never_waits_for_one_it_does_not_depend_on() -> None:
    # `slow` holds a worker until `next` -- in the wave after it, depending only on `quick` --
    # has started. Under wave barriers `next` would wait for `slow`, and `slow` for ever.
    started_next = threading.Event()

    def apply_one(item: Change, options: ApplyOptions) -> ApplyOutcome:
        del options
        if item.key == "semantic_view:slow":
            assert started_next.wait(timeout=5), "the next wave waited for a change it does not depend on"
        if item.key == "semantic_view:next":
            started_next.set()
        return ApplyOutcome(item.key, item.action, OutcomeStatus.APPLIED, 1, 1, "", write_succeeded=True)

    plan = changeset(_change("slow"), _change("quick"), _change("next", "quick"))
    outcomes, diagnostics = _WaveRun(plan, ApplyOptions(parallelism=2), apply_one).run()
    assert [item.key for item in outcomes] == ["semantic_view:quick", "semantic_view:slow", "semantic_view:next"]
    assert diagnostics == ()


def test_stop_all_still_runs_one_at_a_time_in_plan_order() -> None:
    applier = _Applier(1, frozenset({"semantic_view:d"}))
    outcomes, _ = _WaveRun(
        changeset(*PLAN), ApplyOptions(parallelism=4, on_failure=FailurePolicy.STOP_ALL), applier
    ).run()
    assert applier.ran == ["semantic_view:a", "semantic_view:b", "semantic_view:c", "semantic_view:d"]
    assert [item.status for item in outcomes].count(OutcomeStatus.SKIPPED) == 4


def test_a_lost_lock_starts_nothing_further_and_skips_the_rest_in_plan_order() -> None:
    lost: list[str] = []

    def apply_one(item: Change, options: ApplyOptions) -> ApplyOutcome:
        del options
        lost.append("another run")
        return ApplyOutcome(item.key, item.action, OutcomeStatus.APPLIED, 1, 1, "", write_succeeded=True)

    plan = changeset(_change("a"), _change("b", "a"), _change("c", "b"))
    outcomes, diagnostics = _WaveRun(
        plan, ApplyOptions(parallelism=2), apply_one, lambda: lost[0] if lost else None
    ).run()
    assert [(item.key, item.status) for item in outcomes] == [
        ("semantic_view:a", OutcomeStatus.APPLIED),
        ("semantic_view:b", OutcomeStatus.SKIPPED),
        ("semantic_view:c", OutcomeStatus.SKIPPED),
    ]
    assert [item.code for item in diagnostics] == ["SST-APL011"]


def _needing(name: str, *relations: str) -> Change:
    artifact = replace(
        rendered(name), required_relations=tuple(QualifiedName.parse(f"DB.SCHEMA.{item}") for item in relations)
    )
    return change(artifact)


def test_the_preflight_looks_every_relation_up_at_once_on_worker_sessions() -> None:
    main = FakeSnowflake()
    workers = [FakeSnowflake() for _ in range(3)]
    for port in (main, *workers):
        port.existing = {"DB.SCHEMA.A", "DB.SCHEMA.C"}
    pool = ListPool(workers)
    plan = changeset(_needing("X", "C", "B"), _needing("Y", "A", "D"))
    result = apply_with(main, "run-a", sessions=pool).run(plan, state(), ApplyOptions(parallelism=3))
    errors = {item.error.message for item in result.outcomes if item.error is not None}
    # Reported in name order, whichever lookup answered first; none ran on the command's own session.
    assert errors == {"missing required relations: DB.SCHEMA.B, DB.SCHEMA.D"}
    assert pool.leases == 4 and main.scripts == []
