"""`sst plan --select state:modified --state <dir>`: an impact-scoped plan, and its fallback without a manifest."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from tests.helpers.cli_projects import common, compile_project, invoke_with_port, project_copy
from tests.helpers.recorded_snowflake import RecordedSnowflake


def _plan(monkeypatch: pytest.MonkeyPatch, project: Path, *extra: str) -> dict[str, object]:
    result = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--select", "state:modified", "--output", "json", *extra],
    )
    assert result.exit_code in (0, 2), result.output
    payload: dict[str, object] = json.loads(result.output)
    return payload


def test_state_modified_plans_only_what_changed_since_the_previous_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    previous = tmp_path / "previous"
    previous.mkdir()
    shutil.copy(project / "target" / "sst" / "manifest.json", previous / "manifest.json")

    unchanged = _plan(monkeypatch, project, "--state", str(previous))
    data = unchanged["data"]
    assert isinstance(data, dict) and data["changes"] == []
    diagnostics = unchanged["diagnostics"]
    assert isinstance(diagnostics, list) and "SST-PLN006" not in [item["code"] for item in diagnostics]

    full = _plan(monkeypatch, project)
    full_data, full_diagnostics = full["data"], full["diagnostics"]
    assert isinstance(full_data, dict) and len(full_data["changes"]) == 14
    assert isinstance(full_diagnostics, list) and full_diagnostics[0]["code"] == "SST-PLN006"
