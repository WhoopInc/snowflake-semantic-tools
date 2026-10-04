"""How a command fails: internal errors, Snowflake failures, and interrupted or declined runs."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.reference_project import DBT_MANIFEST, FIXTURE, REPO_ROOT, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

# INT902 means SST broke an invariant; every user-caused condition has its own code.
INT902_ALLOWLIST = {
    "snowflake_semantic_tools/app/apply/errors.py": 1,  # an APL028 outcome the plan never recorded
    # compile_each: rendering a view, tool, or eval that validated (VAL762 guards eval templates)
    "snowflake_semantic_tools/app/compile/base.py": 1,
}
# INT001 is the one crash handler's: an exception nothing above it expects.
INT001_ALLOWLIST = {"snowflake_semantic_tools/cli/runner.py": 1}


def _emit_sites(code: str) -> dict[str, int]:
    package = REPO_ROOT / "snowflake_semantic_tools"
    return {
        path.relative_to(REPO_ROOT).as_posix(): count
        for path in sorted(package.rglob("*.py"))
        if "domain/diagnostics/" not in path.as_posix()
        and (count := path.read_text(encoding="utf-8").count(f'"{code}"'))
    }


def test_int902_and_int001_are_emitted_only_at_their_allowlists() -> None:
    assert _emit_sites("SST-INT902") == INT902_ALLOWLIST
    assert _emit_sites("SST-INT001") == INT001_ALLOWLIST


def test_unexpected_json_failure_emits_one_error_document(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("unexpected failure")

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.compile.compile_result", fail)
    result = CliRunner().invoke(
        cli,
        ["compile", "--project-dir", str(FIXTURE), "--manifest", str(DBT_MANIFEST), "--output", "json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["diagnostics"][0]["code"] == "SST-INT001"
    assert payload["diagnostics"][0]["message"] == "internal error: RuntimeError: unexpected failure"


def test_a_recognised_snowflake_failure_is_reported_as_its_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise SnowflakePortError("refused", diagnostic=D("SST-PRT001", value="acme", detail="refused"))

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.compile.compile_result", fail)
    args = ["compile", "--project-dir", str(FIXTURE), "--manifest", str(DBT_MANIFEST)]
    as_json = CliRunner().invoke(cli, [*args, "--output", "json"])
    assert as_json.exit_code == 5
    assert [item["code"] for item in json.loads(as_json.output)["diagnostics"]] == ["SST-PRT001"]
    human = CliRunner().invoke(cli, args)
    assert human.exit_code == 5 and "SST-PRT001" in human.output


def test_a_declined_or_interrupted_run_exits_130_without_an_internal_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    declined = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["apply", *common(project), "--target", "dev"])
    assert declined.exit_code == 130, declined.output
    assert "Apply this plan?" in declined.output and "Aborted." in declined.output
    assert "SST-INT001" not in declined.output

    def interrupt(params: object) -> FakeSnowflake:
        raise KeyboardInterrupt

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", interrupt)
    human = CliRunner().invoke(cli, ["plan", *common(project), "--target", "dev"])
    assert human.exit_code == 130 and "SST-INT001" not in human.output
    as_json = CliRunner().invoke(cli, ["plan", *common(project), "--target", "dev", "--output", "json"])
    assert as_json.exit_code == 130
    envelope = json.loads(as_json.output)
    assert (envelope["exit_code"], envelope["status"]) == (130, "error")
    assert [(item["code"], item["severity"]) for item in envelope["diagnostics"]] == [("SST-PRT107", "info")]


def demo_command(body: Callable[[], CommandResult]) -> click.Command:
    """A throwaway command whose work is `body`, run and reported by `command_body`."""

    @click.command()
    @command_body("demo", config=ConfigNeed.OPTIONAL)
    def demo() -> CommandResult:
        """Demonstrate the runner."""
        return body()

    return demo


def raising(error: BaseException) -> Callable[[], CommandResult]:
    def body() -> CommandResult:
        raise error

    return body


def test_command_body_keeps_the_body_name_and_help_and_returns_nothing() -> None:
    command = demo_command(CommandResult)
    assert (command.name, command.help) == ("demo", "Demonstrate the runner.")
    assert command.callback is not None and command.callback() is None


def test_a_result_renders_its_diagnostics_then_its_human_report_then_exits_with_its_code() -> None:
    warning = DiagnosticBag((D("SST-LOD003", file="warning.yml"),))
    shown = demo_command(lambda: CommandResult(2, warning, human=lambda: click.echo("the report")))
    human = CliRunner().invoke(shown, [])
    assert human.exit_code == 2
    assert human.output.index("SST-LOD003") < human.output.index("the report")
    hidden = CliRunner().invoke(demo_command(lambda: CommandResult(diagnostics=warning, show_diagnostics=False)), [])
    assert (hidden.exit_code, hidden.output) == (0, "")

    machine = CliRunner().invoke(shown, ["--output", "json"])
    envelope = json.loads(machine.output)
    assert machine.exit_code == 2 and "the report" not in machine.output
    assert (envelope["command"], envelope["exit_code"], envelope["status"], envelope["data"]) == (
        "demo",
        2,
        "changes",
        {},
    )
    assert [item["code"] for item in envelope["diagnostics"]] == ["SST-LOD003"]


def test_a_failing_human_report_fails_the_command_like_its_body() -> None:
    def unwritable() -> None:
        raise PermissionError("read-only")

    command = demo_command(lambda: CommandResult(human=unwritable))
    human = CliRunner().invoke(command, [])
    assert human.exit_code == 4 and "error: read-only" in human.output
    # The human report is never run for `--output json`.
    assert CliRunner().invoke(command, ["--output", "json"]).exit_code == 0


@pytest.mark.parametrize(
    ("error", "human_exit", "human_text", "json_exit", "json_codes"),
    [
        pytest.param(SnowflakePortError("refused"), 5, "error[SST-PRT001]", 5, ["SST-PRT001"], id="port-error"),
        pytest.param(
            SnowflakePortError("refused", diagnostic=D("SST-PRT001", value="acme", detail="refused")),
            5,
            "error[SST-PRT001]",
            5,
            ["SST-PRT001"],
            id="port-error-with-diagnostic",
        ),
        pytest.param(ProjectError("broken project"), 4, "error: broken project", 4, [], id="project-error"),
        pytest.param(ValueError("bad value"), 4, "error: bad value", 4, [], id="value-error"),
        pytest.param(RuntimeError("boom"), 1, "error[SST-INT001]", 1, ["SST-INT001"], id="internal-error"),
        pytest.param(KeyboardInterrupt(), 130, "Aborted.", 130, ["SST-PRT107"], id="interrupt"),
        pytest.param(
            SstUsageError("not like that"), 3, "error[SST-PRT100]: not like that", None, None, id="usage-error"
        ),
        pytest.param(click.exceptions.Exit(7), 7, "", None, None, id="exit"),
    ],
)
def test_each_exception_ends_the_run_with_its_exit_code(
    error: BaseException, human_exit: int, human_text: str, json_exit: int | None, json_codes: list[str] | None
) -> None:
    command = demo_command(raising(error))
    human = CliRunner().invoke(command, [])
    assert human.exit_code == human_exit and human_text in human.output
    if json_exit is None:
        return
    machine = CliRunner().invoke(command, ["--output", "json"])
    envelope = json.loads(machine.output)
    assert (machine.exit_code, envelope["exit_code"], envelope["status"]) == (json_exit, json_exit, "error")
    assert [item["code"] for item in envelope["diagnostics"]] == json_codes
    assert envelope["data"]["error"] == ("interrupted" if json_exit == 130 else str(error))
