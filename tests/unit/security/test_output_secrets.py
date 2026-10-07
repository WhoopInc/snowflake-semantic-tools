"""Every form of command output is withheld when it would carry a credential the run resolved."""

from __future__ import annotations

import click
import pytest

from snowflake_semantic_tools.cli import output
from snowflake_semantic_tools.cli.output import captured, print_report, register_secrets


@pytest.fixture(autouse=True)
def _no_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(output, "_SECRETS", set())


def test_a_report_carrying_a_credential_is_withheld_for_sst_prt012(capsys: pytest.CaptureFixture[str]) -> None:
    register_secrets(("s3cret-value",))
    assert print_report("ROLE,s3cret-value\n", "CSV report") is True
    printed = capsys.readouterr()
    assert printed.out == "" and "s3cret-value" not in printed.err
    assert "SST-PRT012" in printed.err and "a credential in the CSV report" in printed.err


def test_a_clean_report_is_printed_as_written(capsys: pytest.CaptureFixture[str]) -> None:
    register_secrets(("s3cret-value",))
    assert print_report("ok\n", "human report") is False
    assert capsys.readouterr().out == "ok\n"


def test_a_human_report_is_captured_before_anything_is_printed(capsys: pytest.CaptureFixture[str]) -> None:
    def report() -> None:
        click.echo("line one")
        click.echo("warn", err=True)

    assert captured(report) == "line one\n"
    printed = capsys.readouterr()
    assert printed.out == "" and printed.err == "warn\n"


@pytest.mark.parametrize(
    ("policy", "shown"),
    [
        (output.RenderPolicy(), True),
        (output.RenderPolicy(output="csv"), True),
        (output.RenderPolicy(output="json"), False),
        (output.RenderPolicy(output="yaml"), False),
        (output.RenderPolicy(quiet=True), False),
    ],
    ids=["table", "csv", "json", "yaml", "quiet"],
)
def test_progress_lines_go_to_stderr_unless_the_output_is_machine_read_or_quiet(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, policy: output.RenderPolicy, shown: bool
) -> None:
    monkeypatch.setattr(output, "_POLICY", [policy])
    output.print_progress("eval run R: starting (attempt 1 of 1)")
    output.print_progress("eval run R: status read failed (1/5), retrying: Read timed out.", warning=True)
    printed = capsys.readouterr()
    assert printed.out == ""
    expected = (
        "eval run R: starting (attempt 1 of 1)\nwarn  eval run R: status read failed (1/5), retrying: Read timed out.\n"
    )
    assert printed.err == (expected if shown else "")


def test_a_progress_line_carrying_a_credential_is_withheld(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(output, "_POLICY", [output.RenderPolicy()])
    register_secrets(("s3cret-value",))
    output.print_progress("status read failed: password=s3cret-value", warning=True)
    printed = capsys.readouterr().err
    assert "s3cret-value" not in printed and "a credential in the progress output" in printed
