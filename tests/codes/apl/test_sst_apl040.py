"""SST-APL040: `sst state unlock` released the run lock the named run held."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.fs.local import StateFileStore, state_file
from tests.helpers.state_unlock import codes, locked_port, unlock, unlock_project


def test_sst_apl040_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = unlock_project(tmp_path)
    local = StateFileStore(state_file(project / "sst_target", "dev"), process=("other-box", 1))
    assert local.acquire_lock("crashed", break_stale=False)[0]
    port = locked_port()
    result = unlock(monkeypatch, port, project, "--run", "crashed", "--yes")
    assert result.exit_code == 0, result.output
    [diagnostic] = [item for item in json.loads(result.stdout)["diagnostics"] if item["code"] == "SST-APL040"]
    assert diagnostic["severity"] == "warning"
    assert diagnostic["message"] == "released the run lock held by run crashed (ROLE on ci-box), expires 1800.0"
    assert json.loads(result.stdout)["data"]["released"] is True
    # Both locks are gone: the row in Snowflake and the lock file beside the local cache.
    assert port.run_locks.holder("dev") is None
    assert local.acquire_lock("next", break_stale=False)[0]


def test_sst_apl040_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # A run that does not hold the lock releases nothing, and the holder keeps it.
    port = locked_port("live-run")
    result = unlock(monkeypatch, port, unlock_project(tmp_path), "--run", "crashed", "--yes")
    assert "SST-APL040" not in codes(result)
    assert port.run_locks.holder("dev") == "live-run"
