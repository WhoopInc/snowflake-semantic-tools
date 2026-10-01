"""`sst docs`, and the help text every command and option carries."""

from __future__ import annotations

import json
from pathlib import Path

import click
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import REPO_ROOT


def test_docs_writes_checks_and_reports_drift(tmp_path: Path) -> None:
    written = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path)])
    assert written.exit_code == 0, written.output
    pages = sorted(path.name for path in (tmp_path / "docs" / "reference").iterdir())
    assert pages == ["artifacts.md", "cli.md", "config.md", "error-codes.md"]
    assert "wrote docs/reference/cli.md" in written.output
    assert CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--check"]).exit_code == 0

    config = tmp_path / "docs" / "reference" / "config.md"
    config.write_text(config.read_text(encoding="utf-8") + "hand edit\n", encoding="utf-8")
    drifted = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--check"])
    assert drifted.exit_code == 1
    assert "out of date: docs/reference/config.md" in drifted.output
    machine = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--check", "--output", "json"])
    assert machine.exit_code == 1
    assert json.loads(machine.output)["data"]["drifted"] == ["docs/reference/config.md"]
    assert "hand edit" in config.read_text(encoding="utf-8")

    repaired = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--output", "json"])
    assert repaired.exit_code == 0
    assert json.loads(repaired.output)["data"]["written"] == ["docs/reference/config.md"]
    assert "hand edit" not in config.read_text(encoding="utf-8")


def test_committed_reference_pages_are_current() -> None:
    result = CliRunner().invoke(cli, ["docs", "--project-dir", str(REPO_ROOT), "--check"])
    assert result.exit_code == 0, result.output


def test_every_command_and_option_is_documented() -> None:
    def walk(command: click.Command, path: str) -> list[str]:
        missing = [path] if not command.help else []
        missing += [
            f"{path} {param.opts[0]}"
            for param in command.params
            if isinstance(param, click.Option) and not param.hidden and not param.help
        ]
        for name, child in getattr(command, "commands", {}).items():
            missing += walk(child, f"{path} {name}")
        return missing

    assert walk(cli, "sst") == []
