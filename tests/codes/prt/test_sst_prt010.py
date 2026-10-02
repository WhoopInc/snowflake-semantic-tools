"""SST-PRT010: `sst clean` could not remove the build directory."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import project_copy


def test_sst_prt010_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    (project / "target" / "sst").mkdir(parents=True)

    def refuse(path: Path) -> None:
        raise PermissionError("read-only")

    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.clean.shutil.rmtree", refuse)
    result = CliRunner().invoke(cli, ["clean", "--project-dir", str(project), "--output", "json"])
    [diagnostic] = json.loads(result.output)["diagnostics"]
    assert (result.exit_code, diagnostic["code"], diagnostic["severity"]) == (1, "SST-PRT010", "error")
    assert diagnostic["message"] == f"could not remove {project / 'target' / 'sst'}: read-only"


def test_sst_prt010_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    (project / "target" / "sst").mkdir(parents=True)
    preview = CliRunner().invoke(cli, ["clean", "--project-dir", str(project), "--dry-run", "--output", "json"])
    assert json.loads(preview.output)["data"]["would_remove"] == [str(project / "target" / "sst")]
    assert (project / "target" / "sst").is_dir()
    result = CliRunner().invoke(cli, ["clean", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 0 and not (project / "target" / "sst").exists()
