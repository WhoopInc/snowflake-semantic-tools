"""`sst` itself: the console scripts, the version, global options, and usage errors."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools import __version__
from snowflake_semantic_tools.cli.group import SstGroup
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.state import SST_VERSION

from .helpers import FIXTURE, MANIFEST, REPO_ROOT


def test_sst_console_script_targets_the_one_point_zero_cli() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = project["tool"]["poetry"]["scripts"]
    assert scripts["sst"] == "snowflake_semantic_tools.cli.main:cli"
    assert scripts["snowflake-semantic-tools"] == "snowflake_semantic_tools.cli.main:cli"


def test_one_version_string_feeds_the_package_the_cli_and_the_manifest() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["tool"]["poetry"]["version"] == __version__
    assert SST_VERSION == __version__
    assert CliRunner().invoke(cli, ["--version"]).output == f"sst, version {__version__}\n"


def test_global_output_and_project_dir_are_forwarded_to_command_defaults() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "--output",
            "json",
            "--project-dir",
            str(FIXTURE),
            "validate",
            "--manifest",
            str(MANIFEST),
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0
    assert json.loads(result.output)["command"] == "validate"


def test_usage_errors_exit_three() -> None:
    result = CliRunner().invoke(cli, ["plan", "--unknown"])
    assert result.exit_code == 3
    comma = CliRunner().invoke(cli, ["plan", "--select", "a,b"])
    assert comma.exit_code == 3
    supported = CliRunner().invoke(cli, ["plan", "--select", "type:agent"])
    assert supported.exit_code != 3
    machine = CliRunner().invoke(cli, ["--output", "json", "plan", "--unknown"])
    assert machine.exit_code == 3
    assert json.loads(machine.output)["exit_code"] == 3


def test_a_usage_error_before_any_command_exits_three_in_both_modes() -> None:
    human = CliRunner().invoke(cli, ["--output", "xml", "list"])
    assert human.exit_code == 3 and "Error: 'xml' is not one of 'human', 'json'." in human.output
    machine = CliRunner().invoke(cli, ["--output", "json", "--no-such-option"])
    envelope = json.loads(machine.output)
    assert (machine.exit_code, envelope["command"], envelope["exit_code"], envelope["status"]) == (3, "", 3, "error")


def test_an_interrupt_outside_a_command_body_still_exits_130() -> None:
    group = SstGroup(name="demo")

    @group.command()
    def stop() -> None:
        raise KeyboardInterrupt

    result = CliRunner().invoke(group, ["stop"])
    assert (result.exit_code, result.output) == (130, "Aborted.\n")


def test_without_standalone_mode_main_returns_the_exit_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["--output", "json", "plan", "--no-such-option"], prog_name="sst", standalone_mode=False) == 3
    assert json.loads(capsys.readouterr().out)["exit_code"] == 3
    assert cli.main(["clean", "--project-dir", str(tmp_path)], prog_name="sst", standalone_mode=False) is None
    assert capsys.readouterr().out == f"nothing to remove at {tmp_path / 'target' / 'sst'}\n"
