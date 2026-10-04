"""SST-PRT109: a run whose confirmation is mandatory was not given `--yes`."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def _refusal(args: list[str]) -> tuple[int, list[dict[str, object]]]:
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    envelope = json.loads(result.output)
    return result.exit_code, envelope["diagnostics"]


def test_sst_prt109_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    for flags in (["--prune"], []):
        exit_code, [diagnostic] = _refusal(["apply", *common(project), *flags])
        assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT109", "error")
    assert diagnostic["message"] == "sst apply --output json requires --yes"


def test_sst_prt109_silent(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["apply", *common(project_copy(tmp_path)), "--prune"])
    assert "error[SST-PRT109]: sst apply --prune requires --yes" in result.output
    # With --yes the refusal is gone; what is left is that the project was never compiled.
    exit_code, diagnostics = _refusal(["apply", *common(project_copy(tmp_path / "y")), "--yes"])
    assert exit_code == 4 and [item["code"] for item in diagnostics] == ["SST-MAN001"]
