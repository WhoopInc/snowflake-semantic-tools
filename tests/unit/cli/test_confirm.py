"""Questions are asked only where they can be answered; an interrupted apply reports what it recorded.

`sst` asks on stderr, and only when stdin and stderr are both terminals. Off a terminal a
command that would ask refuses at once, before it plans or connects, with SST-PRT109.
"""

from __future__ import annotations

import io
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.cli import confirm
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


class _Stream(io.StringIO):
    def __init__(self, terminal: bool) -> None:
        super().__init__()
        self.terminal = terminal

    def isatty(self) -> bool:
        return self.terminal


@pytest.mark.parametrize(
    ("stdin", "stderr", "expected"),
    [(True, True, True), (True, False, False), (False, True, False)],
)
def test_a_question_is_asked_only_when_stdin_and_stderr_are_both_terminals(
    monkeypatch: pytest.MonkeyPatch, stdin: bool, stderr: bool, expected: bool
) -> None:
    monkeypatch.setattr(sys, "stdin", _Stream(stdin))
    monkeypatch.setattr(sys, "stderr", _Stream(stderr))
    assert confirm.can_ask() is expected


def test_a_closed_or_absent_stream_is_not_a_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    closed = _Stream(True)
    closed.close()
    monkeypatch.setattr(sys, "stdin", closed)
    assert not confirm.can_ask()
    monkeypatch.setattr(sys, "stdin", None)
    assert not confirm.can_ask()


def _never_connects(params: object) -> FakeSnowflake:
    raise AssertionError("refused before connecting")


def test_apply_without_yes_off_a_terminal_refuses_before_it_plans_or_connects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", _never_connects)

    refused = CliRunner().invoke(cli, ["apply", *common(project), "--target", "dev"], input="y\n")

    assert refused.exit_code == 3, refused.output
    assert "error[SST-PRT109]: sst apply requires --yes" in refused.output
    assert "note: there is no terminal to ask on: stdin and stderr must both be terminals" in refused.output
    assert "Apply this plan?" not in refused.output


def test_baseline_all_warnings_refuses_when_only_its_output_is_redirected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    # stdin is a terminal and stderr, where the question would go, is not.
    monkeypatch.setattr("snowflake_semantic_tools.cli.confirm._is_terminal", lambda stream: stream is sys.stdin)

    refused = CliRunner().invoke(cli, ["baseline", "add", "--all-warnings", *common(project)])

    assert refused.exit_code == 3, refused.output
    assert "error[SST-PRT109]: sst baseline add --all-warnings requires --yes" in refused.output
    assert "? [y/N]" not in refused.output


def _interrupted_after_applying(monkeypatch: pytest.MonkeyPatch, *, state_written: bool) -> None:
    run = ApplyArtifacts.run

    def stopped(self: ApplyArtifacts, *args: object, **kwargs: object) -> object:
        result = run(self, *args, **kwargs)  # type: ignore[arg-type]
        return replace(result, interrupted=True, state_written=state_written)

    monkeypatch.setattr(ApplyArtifacts, "run", stopped)


def test_an_interrupted_apply_exits_130_and_says_what_state_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    _interrupted_after_applying(monkeypatch, state_written=True)

    human = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["apply", *common(project), "--target", "dev", "--yes"]
    )

    assert human.exit_code == 130, human.output
    assert "applied:" in human.output and "Aborted." in human.output
    assert (
        "info[SST-PRT107]: interrupted after sst apply started; the changes that finished are recorded in "
        "state and the rest were skipped; run sst plan to see what remains"
    ) in human.output


def test_an_interrupted_apply_reports_it_in_json_and_warns_when_state_was_not_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    _interrupted_after_applying(monkeypatch, state_written=False)

    result = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["apply", *common(project), "--target", "dev", "--yes", "--output", "json"],
    )

    envelope = json.loads(result.output)
    assert result.exit_code == 130 and envelope["exit_code"] == 130
    assert envelope["data"]["interrupted"] is True and envelope["data"]["state_written"] is False
    [notice] = [item for item in envelope["diagnostics"] if item["code"] == "SST-PRT107"]
    assert notice["message"].endswith("partial work may have been applied; run sst plan to see what remains")
