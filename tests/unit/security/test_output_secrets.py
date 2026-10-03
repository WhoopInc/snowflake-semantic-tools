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
