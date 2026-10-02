"""SST-PRT110: `--select` or `--exclude` is passed to a command that takes no selector."""

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


def test_sst_prt110_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = _refusal(["debug", *common(project_copy(tmp_path)), "--select", "orders"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT110", "error")
    assert diagnostic["message"] == "sst debug takes no selector; 'orders' is not accepted"


def test_sst_prt110_silent(tmp_path: Path) -> None:
    exit_code, _ = _refusal(["debug", *common(project_copy(tmp_path)), "--no-connect"])
    assert exit_code == 0
