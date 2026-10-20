"""A run on this machine whose process is gone has its locks released by the next run, with no flag."""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import ApplyResult
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import STATE_TABLE
from tests.helpers.artifact_builders import change, changeset, rendered, state
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake


def _crashed(*, row_host: str = "laptop", gone: bool = True) -> tuple[FakeSnowflake, InMemoryStateStore]:
    """The locks a run killed on `row_host` left: its live row, and its lock file here."""
    port = FakeSnowflake()
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("crashed", "ROLE", row_host), break_stale=False)
    store = InMemoryStateStore()
    store.locked, store.holder, store.gone = True, "crashed", gone
    return port, store


def _apply(port: FakeSnowflake, store: InMemoryStateStore, host: str = "laptop") -> ApplyResult:
    use_case = ApplyArtifacts(port, store, FixedClock(), state_table=STATE_TABLE, host=host)
    return use_case.run(changeset(change(rendered())), state())


def test_the_locks_of_a_gone_run_on_this_host_are_released_and_the_run_goes_ahead() -> None:
    port, store = _crashed()
    result = _apply(port, store)
    assert result.success, result.diagnostics
    assert only(result.diagnostics, "SST-APL010").message == (
        "broke a stale lock held by run crashed (ROLE on laptop), expires 1800.0, whose process on this host is gone"
    )
    assert port.run_locks.holder("verify") is None and not store.locked


def test_a_row_another_machine_holds_is_kept_though_the_local_lock_is_released() -> None:
    port, store = _crashed(row_host="ci-box")
    result = _apply(port, store)
    assert not result.success and "SST-APL011" in codes(result.diagnostics)
    assert port.run_locks.holder("verify") == "crashed"
    assert only(result.diagnostics, "SST-APL010").message == (
        "broke a stale lock held by run crashed, whose process on this host is gone"
    )


def test_a_run_that_names_no_host_releases_only_the_local_lock() -> None:
    port, store = _crashed()
    result = _apply(port, store, host="")
    assert "SST-APL011" in codes(result.diagnostics) and port.run_locks.holder("verify") == "crashed"


def test_a_holder_whose_process_runs_keeps_its_locks() -> None:
    port, store = _crashed(gone=False)
    result = _apply(port, store)
    assert "SST-APL010" not in codes(result.diagnostics) and "SST-APL011" in codes(result.diagnostics)
    assert store.holder == "crashed"
