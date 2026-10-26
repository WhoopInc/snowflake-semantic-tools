"""How the runner reports a run: progress around the body, held-back warnings, audits, and failures."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, WriteFailure, command_body, guarded
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, cascade
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError


def _demo(body: Callable[[], CommandResult]) -> click.Command:
    @click.command()
    @command_body("demo", config=ConfigNeed.OPTIONAL)
    def demo() -> CommandResult:
        """Demonstrate the runner."""
        return body()

    return demo


@pytest.fixture
def root_logging() -> Iterator[logging.Logger]:
    root = logging.getLogger()
    level, handlers = root.level, list(root.handlers)
    yield root
    root.setLevel(level)
    root.handlers[:] = handlers


def test_in_ci_progress_shows_around_the_body_and_a_held_back_library_warning_is_counted(
    root_logging: logging.Logger, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    from snowflake_semantic_tools.cli.render.progress import active_display

    seen: list[object] = []

    def body() -> CommandResult:
        seen.append(active_display())
        logging.getLogger("snowflake.connector.vendored.urllib3").warning("Retrying after RemoteDisconnected")
        return CommandResult()

    monkeypatch.setenv("CI", "true")
    result = CliRunner().invoke(_demo(body), ["--project-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert seen and seen[0] is not None and active_display() is None
    told = result.output + capfd.readouterr().err
    assert "1 library warning hidden (--log-level warn shows them)" in told


def test_a_result_whose_diagnostics_break_an_invariant_fails_with_the_audit_finding(tmp_path: Path) -> None:
    dangling = cascade(D("SST-MEM107", member="m", count=1, subject="semantic_view:v"), "SST-REF001")
    command = _demo(lambda: CommandResult(diagnostics=DiagnosticBag((dangling,))))
    machine = CliRunner().invoke(command, ["--project-dir", str(tmp_path), "--output", "json"])
    envelope = json.loads(machine.output)
    assert machine.exit_code == 1 and envelope["exit_code"] == 1
    assert "SST-INT008" in [item["code"] for item in envelope["diagnostics"]]


def _fails(error: BaseException) -> Callable[[], None]:
    def fail() -> None:
        raise error

    return fail


@pytest.mark.parametrize(
    ("error", "exit_code", "code"),
    [
        (SnowflakePortError("some unknown failure"), 5, "SST-PRT001"),
        (WriteFailure(Path("sst_target/manifest.json"), OSError(28, "No space left on device")), 1, "SST-PRT008"),
        (RuntimeError("an invariant broke"), 1, "SST-INT001"),
    ],
)
def test_each_failure_under_agent_output_is_an_agent_document(
    error: BaseException, exit_code: int, code: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(click.exceptions.Exit) as exited:
        guarded(_fails(error), command="plan", output="agent", project_dir=None)
    assert exited.value.exit_code == exit_code
    out = capsys.readouterr().out
    assert out.startswith("sst plan:") and code in out


def test_a_snowflake_failure_outside_a_project_logs_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(click.exceptions.Exit) as exited:
        guarded(_fails(SnowflakePortError("some unknown failure")), command="plan", output="table", project_dir=None)
    assert exited.value.exit_code == 5
    assert not list(tmp_path.rglob("run_log.jsonl"))
