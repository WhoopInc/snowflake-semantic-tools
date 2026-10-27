"""`sst state unlock`: it needs --yes, and releases a named run's locks remote and local."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.state_unlock import locked_port, unlock, unlock_project


def test_unlock_without_yes_is_refused_before_it_connects(tmp_path: Path) -> None:
    project = unlock_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["state", "unlock", "--target", "dev", "--project-dir", str(project), "--run", "x"]
    )
    assert result.exit_code == 3, result.output
    assert "sst state unlock requires --yes" in result.output


def test_unlock_reports_the_target_lock_table_and_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = unlock(monkeypatch, locked_port(), unlock_project(tmp_path), "--run", "crashed", "--yes")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"] == {
        "target": "dev",
        "lock_table": "SST_REF_DEV.JAFFLE.SST_STATE_LOCK",
        "run_id": "crashed",
        "released": True,
    }
