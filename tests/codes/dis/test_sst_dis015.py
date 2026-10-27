"""SST-DIS015: `--exclude` removed every artifact `--select` chose."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def _list(project: Path, *selectors: str) -> tuple[int, str]:
    result = CliRunner().invoke(cli, ["-o", "agent", "list", "--no-manifest", *common(project), *selectors])
    return result.exit_code, result.stdout


def test_sst_dis015_fires(tmp_path: Path) -> None:
    # GitHub #272: this once said `--select jaffle*` matched no artifact.
    exit_code, out = _list(project_copy(tmp_path), "--select", "jaffle*", "--exclude", "jaffle*")
    assert exit_code == 3
    assert "W SST-DIS015 | --exclude removed every artifact --select jaffle* chose" in out.splitlines()


def test_sst_dis015_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    exit_code, out = _list(project, "--select", "nosuchthing")
    assert exit_code == 3 and "SST-DIS015" not in out and "SST-DIS010" in out
    assert "SST-DIS015" not in _list(project, "--select", "jaffle*", "--exclude", "jaffle_minimal*")[1]
