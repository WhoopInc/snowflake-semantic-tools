"""The local state lock as a refusal names it: the run, the lock file, that it locks one checkout, and its expiry."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import StateFileStore

NOW = datetime(2026, 10, 16, 11, 21, 12, tzinfo=UTC)


def test_a_refused_local_lock_names_its_run_its_file_its_reach_and_when_it_goes_stale(tmp_path: Path) -> None:
    store = StateFileStore(tmp_path / "state.dev.json", now=lambda: NOW)
    assert store.acquire_lock("b669caf7", break_stale=False)[0]
    locked, holder, _ = store.acquire_lock("next", break_stale=True)
    assert not locked and holder == "b669caf7"
    assert store.describe_lock(holder) == (
        f"run b669caf7, by the lock file {tmp_path / 'state.dev.json.lock'} "
        "(this checkout only; stale after 2026-10-16T11:51:12+00:00)"
    )


def test_a_lock_file_that_records_no_time_says_it_never_goes_stale_by_itself(tmp_path: Path) -> None:
    (tmp_path / "state.dev.json.lock").write_text(json.dumps({"run_id": "old"}), encoding="utf-8")
    store = StateFileStore(tmp_path / "state.dev.json", now=lambda: NOW)
    assert store.describe_lock(None) == (
        f"a run SST cannot name, by the lock file {tmp_path / 'state.dev.json.lock'} "
        "(this checkout only; it records no time, so delete it once its run has ended)"
    )
