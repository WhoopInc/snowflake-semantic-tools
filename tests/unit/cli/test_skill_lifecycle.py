"""A skill's whole life from the command line: published, edited, reverted, deleted, and dropped."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.cli_projects import invoke_with_port, skill_project
from tests.helpers.snowflake_fake import FakeSnowflake

SKILL = QualifiedName.parse("DB.SCH.MONTH_CLOSE")


def _compiled(project: Path) -> None:
    assert CliRunner().invoke(cli, ["compile", "--project-dir", str(project)]).exit_code == 0


def _run(monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake, project: Path, *args: str) -> Result:
    return invoke_with_port(monkeypatch, port, [*args, "--project-dir", str(project)])


def _default(port: FakeSnowflake) -> list[str]:
    return [item.name for item in port.extension_versions(SKILL) if item.is_default]


def test_a_reverted_skill_moves_the_default_back_under_strict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    steps = project / "skills" / "month-close" / "reference" / "steps.md"
    _compiled(project)
    assert _run(monkeypatch, port := FakeSnowflake(existing=()), project, "apply", "--yes").exit_code == 0
    steps.write_text("steps, revised\n", encoding="utf-8")
    _compiled(project)
    assert _run(monkeypatch, port, project, "apply", "--yes").exit_code == 0
    assert _default(port) == ["VERSION$2"]

    steps.write_text("steps\n", encoding="utf-8")
    _compiled(project)
    planned = _run(monkeypatch, port, project, "plan", "--strict", "--output", "json")
    assert planned.exit_code == 2, planned.output
    payload = json.loads(planned.output)
    assert [(item["action"], item["reason"]) for item in payload["data"]["changes"]] == [
        ("update", "default_version_differs")
    ]
    assert "SST-VAL841" not in [item["code"] for item in payload["diagnostics"]]
    human = _run(monkeypatch, port, project, "plan", "--strict")
    [row] = [line for line in human.output.splitlines() if "skill:month-close" in line]
    assert "another version is the default" in row

    applied = _run(monkeypatch, port, project, "apply", "--yes", "--strict")
    assert applied.exit_code == 0, applied.output
    assert _default(port) == ["VERSION$1"]
    assert _run(monkeypatch, port, project, "plan", "--strict").exit_code == 0
