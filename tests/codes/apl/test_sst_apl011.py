"""SST-APL011: another run holds the target's apply lock, or broke this run's."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, OutcomeStatus
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.apply_runs import STATE_TABLE, apply_plan
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl011_fires() -> None:
    port = FakeSnowflake()
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("other", "ROLE", "laptop", 60), break_stale=False)
    result = apply_plan(changeset(change(rendered())), port)
    diagnostic = only(result.diagnostics, "SST-APL011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "run other (ROLE on laptop), expires 60.0 holds the apply lock"
    assert port.scripts == [] and not result.state_written


def test_sst_apl011_fires_for_a_local_lock_naming_its_file_and_when_it_goes_stale(tmp_path: Path) -> None:
    taken = datetime(2026, 10, 16, 11, 21, 12, tzinfo=UTC)
    store = StateFileStore(tmp_path / "state.verify.json", now=lambda: taken)
    assert store.acquire_lock("killed-run", break_stale=False)[0]
    port = FakeSnowflake()
    result = apply_plan(changeset(change(rendered())), port, store=store)  # type: ignore[arg-type]
    diagnostic = only(result.diagnostics, "SST-APL011")
    assert diagnostic.message == (
        f"run killed-run, by the lock file {tmp_path / 'state.verify.json.lock'} "
        "(this checkout only; stale after 2026-10-16T11:51:12+00:00) holds the apply lock"
    )
    assert port.scripts == [] and port.run_locks.claims == []


def test_sst_apl011_fires_when_another_run_took_the_lock_before_the_next_change() -> None:
    port = FakeSnowflake()

    def taken_over_meanwhile(*args: object) -> ExecResult:
        port.run_locks.now = 1_000_000.0
        port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("rival"), break_stale=True)
        return ExecResult(True, ("q1",))

    port.execute_script = taken_over_meanwhile  # type: ignore[assignment, method-assign]
    result = apply_plan(changeset(change(rendered("A")), change(rendered("B"))), port)
    diagnostic = only(result.diagnostics, "SST-APL011")
    assert diagnostic.message == "another run, which broke this run's lock holds the apply lock"
    assert [outcome.status for outcome in result.outcomes] == [OutcomeStatus.APPLIED, OutcomeStatus.SKIPPED]
    assert not result.state_written and port.state_writes == []


def test_sst_apl011_silent() -> None:
    port = FakeSnowflake()
    assert "SST-APL011" not in codes(apply_plan(changeset(change(rendered())), port).diagnostics)
    assert port.run_locks.rows == {}
