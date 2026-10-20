"""SST-APL041: `sst state unlock` named a run that does not hold the target's run lock."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.state_unlock import codes, locked_port, unlock, unlock_project


def test_sst_apl041_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for index, port in enumerate((locked_port("live-run"), FakeSnowflake())):
        result = unlock(monkeypatch, port, unlock_project(tmp_path / str(index)), "--run", "crashed", "--yes")
        assert result.exit_code == 1, result.output
        [diagnostic] = [item for item in json.loads(result.stdout)["diagnostics"] if item["code"] == "SST-APL041"]
        assert diagnostic["severity"] == "error"
        assert diagnostic["message"] == (
            "run crashed does not hold the run lock of target dev, so nothing was released"
        )
        assert json.loads(result.stdout)["data"]["released"] is False


def test_sst_apl041_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = unlock(monkeypatch, locked_port(), unlock_project(tmp_path), "--run", "crashed", "--yes")
    assert result.exit_code == 0 and "SST-APL041" not in codes(result)
