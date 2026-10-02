"""SST-PRT106: `--output` names a format the command does not support; there is no fallback."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy


def _refusal(args: list[str]) -> tuple[int, list[dict[str, object]]]:
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    envelope = json.loads(result.output)
    return result.exit_code, envelope["diagnostics"]


def test_sst_prt106_fires(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["validate", *common(project_copy(tmp_path)), "--output", "csv"])
    assert result.exit_code == 3
    assert (
        "error[SST-PRT106]: 'csv' is not a supported format for sst validate; supported: table, plain, json"
        in result.output
    )


def test_sst_prt106_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    assert CliRunner().invoke(cli, ["compile", *common(project)]).exit_code == 0
    listed = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "csv"])
    assert listed.exit_code == 0 and listed.output.splitlines()[0].startswith("key,target,fingerprint")
    as_yaml = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "yaml"])
    assert as_yaml.exit_code == 0 and "tool: sst" in as_yaml.output
    hidden = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "human"])
    assert hidden.exit_code == 0 and "semantic_view:jaffle_menu" in hidden.output
