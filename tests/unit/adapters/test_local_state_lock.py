"""The local state lock as a refusal names it: the run, the lock file, that it locks one checkout, and its expiry."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.fs.local import StateFileStore, process_alive

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


def test_a_lock_records_its_host_and_process_and_is_gone_only_once_this_host_runs_no_such_process(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.dev.json"
    lock = tmp_path / "state.dev.json.lock"
    assert StateFileStore(path, process=("laptop", 41)).acquire_lock("crashed", break_stale=False)[0]
    payload = json.loads(lock.read_text(encoding="utf-8"))
    assert (payload["run_id"], payload["host"], payload["pid"]) == ("crashed", "laptop", 41)

    def store(host: str, pid: int, *, alive: bool) -> StateFileStore:
        return StateFileStore(path, process=(host, pid), alive=lambda _: alive)

    assert store("laptop", 99, alive=False).gone_holder() == "crashed"
    # A running process, another machine, or this very process is never gone.
    assert store("laptop", 99, alive=True).gone_holder() is None
    assert store("other", 99, alive=False).gone_holder() is None
    assert store("laptop", 41, alive=False).gone_holder() is None
    # A lock taken before SST recorded its process, one it cannot read, and none at all.
    for written in ('{"run_id":"old","created_at":"2026-10-01T00:00:00+00:00"}', "not json", "[1]", "x" * 5000):
        lock.write_text(written, encoding="utf-8")
        assert store("laptop", 99, alive=False).gone_holder() is None
    lock.unlink()
    assert store("laptop", 99, alive=False).gone_holder() is None


def test_a_process_is_running_unless_this_machine_says_it_is_not(monkeypatch: pytest.MonkeyPatch) -> None:
    assert process_alive(os.getpid())
    ended = subprocess.Popen([sys.executable, "-c", "pass"])
    ended.wait()
    assert not process_alive(ended.pid)
    # A process another user runs cannot be signalled, and is running all the same.
    monkeypatch.setattr(os, "kill", _refused)
    assert process_alive(ended.pid)
    # Windows would end a process asked this way, so it is never asked there.
    monkeypatch.setattr(os, "name", "nt")
    assert process_alive(ended.pid)


def _refused(pid: int, signal: int) -> None:
    raise PermissionError(pid, signal)
