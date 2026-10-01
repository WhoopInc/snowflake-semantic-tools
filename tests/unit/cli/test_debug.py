"""`sst debug`: the resolved target and, with `--test-connection`, the live session."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.cli_projects import common, invoke_with_port, project_copy
from tests.helpers.recorded_snowflake import RecordedSnowflake


def test_debug_connection_and_human_apply_prompt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    debug_port = RecordedSnowflake(role="R", account_locator="A")
    debugged = invoke_with_port(
        monkeypatch,
        debug_port,
        ["debug", "--project-dir", str(project), "--test-connection", "--output", "json"],
    )
    assert debugged.exit_code == 0
    assert json.loads(debugged.output)["data"]["current_role"] == "R"

    apply_port = RecordedSnowflake(state={})
    applied = invoke_with_port(
        monkeypatch,
        apply_port,
        ["apply", *common(project), "--target", "dev"],
    )
    assert applied.exit_code != 0 and "Apply this plan?" in applied.output
