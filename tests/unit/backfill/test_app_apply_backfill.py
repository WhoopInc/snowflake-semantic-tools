"""Applying under STOP_ALL when the lock is lost, and an extension drop whose lock was broken."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.apply.waves import _WaveRun
from snowflake_semantic_tools.app.drop import EXTENSION, DropObject, DropRequest
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    ApplyOptions,
    ApplyOutcome,
    Change,
    FailurePolicy,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import change, changeset, empty_state, rendered
from tests.helpers.clocks import FixedClock
from tests.helpers.skill_inputs import compile_extensions, publish_extensions, skill, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake

STOP_ALL = ApplyOptions(on_failure=FailurePolicy.STOP_ALL)


def _change(name: str, *depends_on: str) -> Change:
    return replace(change(rendered(name)), depends_on=tuple(f"semantic_view:{item}" for item in depends_on))


def _applied(item: Change, options: ApplyOptions) -> ApplyOutcome:
    del options
    return ApplyOutcome(item.key, item.action, OutcomeStatus.APPLIED, 1, 1, "", write_succeeded=True)


def test_under_stop_all_a_lock_lost_after_a_wave_skips_every_later_wave() -> None:
    lost: list[str] = []

    def apply_one(item: Change, options: ApplyOptions) -> ApplyOutcome:
        lost.append("another run")
        return _applied(item, options)

    plan = changeset(_change("a"), _change("b", "a"), _change("c", "b"))
    outcomes, diagnostics = _WaveRun(plan, STOP_ALL, apply_one, lambda: lost[0] if lost else None).run()
    assert [(item.key, item.status) for item in outcomes] == [
        ("semantic_view:a", OutcomeStatus.APPLIED),
        ("semantic_view:b", OutcomeStatus.SKIPPED),
        ("semantic_view:c", OutcomeStatus.SKIPPED),
    ]
    assert [item.code for item in diagnostics] == ["SST-APL011"]


def test_under_stop_all_a_lock_lost_before_the_first_wave_runs_nothing() -> None:
    ran: list[str] = []

    def apply_one(item: Change, options: ApplyOptions) -> ApplyOutcome:
        ran.append(item.key)
        return _applied(item, options)

    plan = changeset(_change("a"), _change("b", "a"))
    outcomes, diagnostics = _WaveRun(plan, STOP_ALL, apply_one, lambda: "another run").run()
    assert ran == []
    assert {item.status for item in outcomes} == {OutcomeStatus.SKIPPED}
    assert [item.code for item in diagnostics] == ["SST-APL011"]


TABLE = QualifiedName.parse("DB.S.SST_STATE")


def test_an_extension_drop_whose_lock_was_broken_before_it_forgets_leaves_its_entry() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, compile_extensions(skill_catalog(skill())), empty_state())
    [(key, entry)] = published.applied.items()
    port.remote_state = {key: entry}
    execute = port.execute_script

    def broken_meanwhile(statements: object) -> object:
        port.run_locks.now = 10_000.0
        assert port.run_locks.acquire_run_lock(TABLE, "dev", LockClaim("rival"), break_stale=True).acquired
        return execute(statements)  # type: ignore[arg-type]

    port.execute_script = broken_meanwhile  # type: ignore[assignment, method-assign]
    request = DropRequest(QualifiedName.parse(entry.qualified_name), "skill", EXTENSION, "dev")
    result = DropObject(port, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(request)
    assert (result.outcome, result.forgotten, result.dropped) == ("dropped", (), True)
    assert [item.code for item in result.diagnostics] == ["SST-APL011"]
    assert set(port.remote_state or {}) == {key}
