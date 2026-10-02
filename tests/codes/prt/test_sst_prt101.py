"""SST-PRT101: a selector contains a comma; union is spelled with spaces and intersection is unsupported."""

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


def test_sst_prt101_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = _refusal(["validate", *common(project_copy(tmp_path)), "--select", "a,b"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT101", "error")
    assert diagnostic["message"] == "selector 'a,b' contains a comma"


def test_sst_prt101_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    args = ["validate", *common(project), "--select", "jaffle_menu", "--select", "jaffle_sales"]
    exit_code, diagnostics = _refusal(args)
    assert exit_code == 0 and "SST-PRT101" not in [item["code"] for item in diagnostics]
