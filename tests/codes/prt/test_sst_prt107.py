"""SST-PRT107: the run was interrupted; after `apply`, partial work may have been applied."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt107_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupt(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.compile.compile_result", interrupt)
    result = CliRunner().invoke(cli, ["validate", *common(project_copy(tmp_path)), "--output", "json"])
    [diagnostic] = json.loads(result.output)["diagnostics"]
    assert (result.exit_code, diagnostic["code"], diagnostic["severity"]) == (130, "SST-PRT107", "info")
    assert diagnostic["message"] == "interrupted after sst validate started; nothing was written to Snowflake"
    human = CliRunner().invoke(cli, ["apply", *common(project_copy(tmp_path / "a")), "--yes"])
    assert human.exit_code == 130 and "partial work may have been applied" in human.output


def test_sst_prt107_silent(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["validate", *common(project_copy(tmp_path)), "--output", "json"])
    assert result.exit_code == 0 and "SST-PRT107" not in result.output
