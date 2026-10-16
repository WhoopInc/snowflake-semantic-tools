"""Apply when Snowflake ends its session mid-run: one fresh connection releases the lock and records what finished."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, OutcomeStatus
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_sessions import STATE_TABLE, RunClock, stepped
from tests.helpers.artifact_builders import change, changeset, rendered, state
from tests.helpers.dropped_sessions import DroppedSession, Reconnects
from tests.helpers.snowflake_fake import FakeSnowflake


def _apply(port: FakeSnowflake, store: InMemoryStateStore, reconnect: Reconnects | None) -> ApplyArtifacts:
    return ApplyArtifacts(
        port,
        store,
        RunClock("run-a"),
        state_table=STATE_TABLE,
        actor="ROLE",
        lock_policy=stepped(),
        reconnect=reconnect,
    )


def test_a_session_ended_mid_run_releases_the_lock_and_records_what_finished_on_a_fresh_connection() -> None:
    port, store = DroppedSession(scripts=1), InMemoryStateStore()
    reconnect = Reconnects(port)
    artifacts = [rendered(name) for name in ("A", "B", "C")]
    result = _apply(port, store, reconnect).run(
        changeset(*(change(item) for item in artifacts)), state(), ApplyOptions(parallelism=1)
    )

    assert [(item.key, item.status, item.write_succeeded) for item in result.outcomes] == [
        ("semantic_view:a", OutcomeStatus.APPLIED, True),
        ("semantic_view:b", OutcomeStatus.FAILED, False),
        ("semantic_view:c", OutcomeStatus.FAILED, False),
    ]
    # One fresh connection, for the state write and the release; the target is not left locked.
    assert reconnect.opened == 1 and port.run_locks.rows == {}
    [written] = reconnect.fresh.state_writes
    assert set(written.upserts) == {"semantic_view:a"} and result.state_written
    assert store.state is not None and set(store.state.applied) == {"semantic_view:a"}
    lost = [item for item in result.diagnostics if item.code == "SST-APL103"]
    assert [item.message for item in lost] == [
        "the connection to Snowflake was lost mid-run; finished before it: semantic_view:a"
    ]
    assert not result.success


def test_without_a_way_to_reconnect_a_lost_session_still_propagates() -> None:
    port, store = DroppedSession(scripts=0), InMemoryStateStore()
    with pytest.raises(SnowflakePortError, match="Session no longer exists"):
        _apply(port, store, None).run(changeset(change(rendered())), state())
    # The local lock is released all the same; the remote one expires on its own.
    assert not store.locked and port.run_locks.rows != {}


def test_a_failure_that_is_not_a_lost_session_is_never_retried_on_a_fresh_connection() -> None:
    port, store = FakeSnowflake(), InMemoryStateStore()
    port.fail("write_state", SnowflakePortError("lock timeout", errno=625, sqlstate="57014"))
    reconnect = Reconnects(port)
    with pytest.raises(SnowflakePortError, match="lock timeout"):
        _apply(port, store, reconnect).run(changeset(change(rendered())), state())
    assert reconnect.opened == 0 and port.run_locks.rows == {}


def test_a_worker_session_lost_while_the_commands_survives_is_still_reported() -> None:
    from tests.helpers.apply_sessions import ListPool

    main, worker = FakeSnowflake(), DroppedSession(scripts=0)
    worker.run_locks = main.run_locks
    reconnect = Reconnects(main)
    result = ApplyArtifacts(
        main,
        InMemoryStateStore(),
        RunClock("run-a"),
        state_table=STATE_TABLE,
        sessions=ListPool([worker]),
        lock_policy=stepped(),
        reconnect=reconnect,
    ).run(changeset(change(rendered())), state())
    assert reconnect.opened == 0 and result.state_written and main.run_locks.rows == {}
    assert [item.message for item in result.diagnostics if item.code == "SST-APL103"] == [
        "the connection to Snowflake was lost mid-run; finished before it: nothing"
    ]
