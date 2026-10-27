"""`sst import` and `sst state adopt` from the command line, against the reference project and a fake target."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.helpers.cli_projects import common, compile_project, invoke_with_port, on_a_terminal
from tests.helpers.import_fakes import FakeImportPort, staged_agent
from tests.helpers.reference_project import project_copy


def _port() -> FakeImportPort:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    return port


def test_import_reports_what_it_would_write_and_exits_2_without_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    result = invoke_with_port(
        monkeypatch, _port(), ["import", "agent", "DB.S.SALES_BOT", *common(project), "-o", "json"]
    )
    assert result.exit_code == 2, result.output
    data = json.loads(result.stdout)["data"]
    [obj] = data["objects"]
    assert (obj["artifact"], obj["status"], data["written"]) == ("agent:sales_bot", "would write", [])
    assert not any((project / path).exists() for path in obj["files"])


def test_import_writes_the_files_with_write_and_never_replaces_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    args = ["import", "agent", "DB.S.SALES_BOT", *common(project), "--write", "-o", "json"]
    written = invoke_with_port(monkeypatch, _port(), args)
    assert written.exit_code == 0, written.output
    [path] = json.loads(written.stdout)["data"]["written"]
    assert (project / path).read_text(encoding="utf-8").startswith("name: sales_bot\n")
    again = invoke_with_port(monkeypatch, _port(), args)
    assert again.exit_code == 1, again.output
    assert "SST-IMP004" in again.stdout


@pytest.mark.parametrize(
    "args",
    [
        ["import", "views", "DB.S"],
        ["import", "agent", "DB.S", "--define"],
        ["import", "eval", "DB.S.T"],
        ["import", "agent", "DB.S.X", "--like", "A%"],
        ["import", "semantic_view", "@DB.S.STAGE/x/"],
        ["import", "agent"],
    ],
)
def test_import_refuses_a_command_line_it_does_not_take(
    args: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = invoke_with_port(monkeypatch, _port(), [*args, *common(project_copy(tmp_path))])
    assert result.exit_code == 3, result.output
    assert "SST-PRT100" in result.output


def test_the_wizard_asks_for_what_is_missing_and_offers_to_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    on_a_terminal(monkeypatch)
    answers: Iterator[str] = iter(["agent", "object", "DB", "S", "1", "", "n", "y"])
    monkeypatch.setattr("snowflake_semantic_tools.cli.confirm.ask", lambda question, default: next(answers))
    result = invoke_with_port(monkeypatch, _port(), ["import", *common(project)])
    assert result.exit_code == 0, result.output
    assert (project / "agents" / "sales_bot" / "agent.yml").is_file()


def test_adopt_requires_a_selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    result = invoke_with_port(monkeypatch, FakeImportPort(), ["state", "adopt", *common(project), "--yes"])
    assert result.exit_code == 3, result.output
    assert "requires --select" in result.output
