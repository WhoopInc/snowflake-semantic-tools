"""What `sst` prints: the JSON envelope, and human output across commands."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools import __version__
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import FIXTURE, MANIFEST, common, invoke_with_port, project_copy
from tests.helpers.recorded_snowflake import RecordedSnowflake


def test_validate_json_emits_one_v2_envelope() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--output",
            "json",
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert envelope["tool"] == "sst"
    assert envelope["schema_version"] == 2
    assert envelope["sst_version"] == __version__
    assert envelope["invocation"]["argv"][1] == "validate"
    assert envelope["invocation"]["project_dir"] == str(FIXTURE.resolve())
    assert envelope["invocation"]["config_file"] == str((FIXTURE / "sst_config.yml").resolve())
    assert envelope["invocation"]["started_at"]
    assert envelope["invocation"]["duration_s"] >= 0
    assert envelope["status"] == "ok"
    # One info is SST-VAL854: the fixture's profile registry is not Desktop's. The
    # one warning is SST-VAL528; the plugin has a consumer now, the operator profile.
    assert envelope["summary"] == {
        "error": 0,
        "warning": 1,
        "info": 22,
        "promoted": 0,
        "suppressed_cascade": 0,
        "baselined": 0,
    }


def test_human_output_and_usage_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    initialized = CliRunner().invoke(cli, ["init", "--project-dir", str(tmp_path / "human")])
    assert initialized.exit_code == 0 and "initialized" in initialized.output
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project)])
    assert debugged.exit_code == 0 and "state_table:" in debugged.output
    compiled = CliRunner().invoke(cli, ["compile", *common(project), "--print-ddl"])
    assert compiled.exit_code == 0 and "CREATE OR REPLACE" in compiled.output

    conflict = CliRunner().invoke(
        cli,
        ["plan", *common(project), "--plan-out", str(tmp_path / "p.json"), "--no-plan-out"],
    )
    assert conflict.exit_code == 3
    prune = CliRunner().invoke(cli, ["apply", *common(project), "--prune"])
    assert prune.exit_code == 3

    port = RecordedSnowflake(state={})
    planned = invoke_with_port(monkeypatch, port, ["plan", *common(project), "--target", "dev"])
    assert planned.exit_code == 2 and "Plan:" in planned.output
    cleaned = CliRunner().invoke(cli, ["clean", "--project-dir", str(project)])
    assert cleaned.exit_code == 0 and "removed" in cleaned.output
    nothing = CliRunner().invoke(cli, ["clean", "--project-dir", str(project)])
    assert nothing.exit_code == 0 and "nothing to remove" in nothing.output
