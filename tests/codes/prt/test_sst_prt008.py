"""SST-PRT008: a file a command writes could not be written; the run exits 1."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy


def test_sst_prt008_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    blocked = tmp_path / "ddl"
    blocked.write_text("a file, not a directory", encoding="utf-8")
    result = CliRunner().invoke(
        cli, ["compile", *common(project), "--emit-ddl", str(blocked / "out"), "--output", "json"]
    )
    envelope = json.loads(result.output)
    [diagnostic] = envelope["diagnostics"]
    assert (result.exit_code, diagnostic["code"], diagnostic["severity"]) == (1, "SST-PRT008", "error")
    assert diagnostic["message"].startswith(f"could not write {blocked / 'out'}")


def test_sst_prt008_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(
        cli, ["compile", *common(project), "--emit-ddl", str(tmp_path / "ddl"), "--output", "json"]
    )
    assert result.exit_code == 0 and "SST-PRT008" not in result.output
