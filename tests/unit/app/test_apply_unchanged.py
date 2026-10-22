"""A plan with nothing to write ends once planned: no lock, no state read, no session, the same result."""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts, run, writes_nothing
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyResult,
    ChangeSet,
    FailurePolicy,
    OutcomeStatus,
)
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_sessions import ListPool, RecordingSnowflake, RunClock, apply_with
from tests.helpers.artifact_builders import change, changeset, marker, observed, rendered, state
from tests.helpers.snowflake_fake import FakeSnowflake


def _unchanged() -> ChangeSet:
    """A plan with no write: a NOOP, a blocked change, and a NOOP and a blocked change depending on it."""
    noop = replace(change(rendered("A")), action=Action.NOOP)
    blocked = replace(change(rendered("B")), action=Action.BLOCKED)
    behind = replace(change(rendered("C", depends_on=("semantic_view:b",))), action=Action.NOOP)
    stuck = replace(change(rendered("D", depends_on=("semantic_view:b",))), action=Action.BLOCKED)
    warning = D("SST-APL015", artifact="semantic_view:a")
    return replace(changeset(noop, blocked, behind, stuck), diagnostics=DiagnosticBag((warning,)))


def _same(result: ApplyResult) -> tuple[object, ...]:
    """What a run reports, less its id and when it ran."""
    return (result.outcomes, tuple(result.diagnostics), result.success, result.state_written, result.interrupted)


def test_an_unchanged_plan_sends_snowflake_nothing_and_leases_no_session() -> None:
    port, worker, heartbeat = RecordingSnowflake(), RecordingSnowflake(), RecordingSnowflake()
    pool = ListPool([worker])
    store = InMemoryStateStore()
    use_case = apply_with(port, "quiet", store=store, sessions=pool, heartbeat=heartbeat)

    result = use_case.run(_unchanged(), state(), ApplyOptions(on_failure=FailurePolicy.CONTINUE))

    assert (port.calls, worker.calls, heartbeat.calls, pool.leases) == ([], [], [], 0)
    assert not store.locked and store.writes == [] and port.run_locks.holder("verify") is None
    assert [outcome.status for outcome in result.outcomes] == [OutcomeStatus.SKIPPED] * 4
    assert not result.state_written


@pytest.mark.parametrize("policy", [FailurePolicy.CONTINUE, FailurePolicy.STOP_DEPENDENTS, FailurePolicy.STOP_ALL])
def test_an_unchanged_plan_reports_what_the_locked_run_reported(
    monkeypatch: pytest.MonkeyPatch, policy: FailurePolicy
) -> None:
    options = ApplyOptions(on_failure=policy)
    quick = apply_with(FakeSnowflake(), "run").run(_unchanged(), state(), options)
    monkeypatch.setattr(run, "writes_nothing", lambda changeset: False)
    locked_port = RecordingSnowflake()
    locked = apply_with(locked_port, "run").run(_unchanged(), state(), options)

    assert "acquire_run_lock" in locked_port.calls or not locked.outcomes
    assert _same(quick) == _same(locked)


def test_blocked_skips_and_their_dependents_report_as_under_the_lock() -> None:
    result = apply_with(FakeSnowflake(), "run").run(
        _unchanged(), state(), ApplyOptions(on_failure=FailurePolicy.CONTINUE)
    )
    codes = [item.code for item in result.diagnostics]
    # The plan's own first, then one SST-APL042 per blocked change; a run that skips one does not succeed.
    assert codes == ["SST-APL015", "SST-APL042", "SST-APL042"]
    assert not result.success


def test_an_unchanged_plan_the_run_refuses_is_refused_before_anything() -> None:
    port = RecordingSnowflake()
    blocked = changeset(replace(change(rendered("B")), action=Action.BLOCKED))
    result = apply_with(port, "run").run(blocked, state(), ApplyOptions(on_failure=FailurePolicy.STOP_ALL))
    assert [item.code for item in result.diagnostics] == ["SST-APL003"]
    assert result.outcomes == () and port.calls == []


def test_a_plan_with_one_write_still_locks_rereads_and_records_state() -> None:
    port, store = RecordingSnowflake(), InMemoryStateStore()
    plan = changeset(replace(change(rendered("A")), action=Action.NOOP), change(rendered("B")))
    assert not writes_nothing(plan)

    result = apply_with(port, "write", store=store).run(plan, state())

    assert result.success and result.state_written and store.writes
    assert {"acquire_run_lock", "read_state", "write_state", "release_run_lock"} <= set(port.calls)


def test_a_report_only_prune_still_takes_the_lock_and_records_the_run() -> None:
    artifact = rendered()
    live = observed(artifact, ownership=marker(artifact))
    report_only = replace(change(artifact, Action.PRUNE, live=live), prune_executable=False)
    plan = changeset(report_only)
    assert not plan.writes and not writes_nothing(plan)
    port = RecordingSnowflake()

    result = apply_with(port, "prune").run(plan, state(), ApplyOptions(allow_prune=True))

    assert result.state_written and port.scripts == []
    assert {"acquire_run_lock", "write_state"} <= set(port.calls)


def test_a_plan_that_creates_a_schema_still_takes_the_full_path() -> None:
    plan = replace(
        changeset(replace(change(rendered("A")), action=Action.NOOP)), schemas=(SchemaScope.parse("DB.NEW"),)
    )
    assert not writes_nothing(plan)
    port = RecordingSnowflake()

    ApplyArtifacts(port, InMemoryStateStore(), RunClock("schema"), state_table=rendered("STATE").target).run(
        plan, state()
    )

    assert port.scripts[0] == ("CREATE SCHEMA IF NOT EXISTS DB.NEW",)
    assert "acquire_run_lock" in port.calls


def test_an_empty_plan_writes_nothing() -> None:
    assert writes_nothing(changeset())
    result = apply_with(RecordingSnowflake(), "empty").run(changeset(), state())
    assert result.success and result.outcomes == () and not result.state_written
