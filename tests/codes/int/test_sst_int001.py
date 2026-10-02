"""SST-INT001: an exception crossed every phase boundary unhandled."""

from __future__ import annotations

import json

import click
from click.testing import CliRunner

from snowflake_semantic_tools.cli.options import output_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body


def command(error: Exception) -> click.Command:
    @click.command()
    @output_option()
    @command_body("demo")
    def demo(output: str) -> CommandResult:
        raise error

    return demo


def test_sst_int001_fires() -> None:
    result = CliRunner().invoke(command(RuntimeError("boom")), ["--output", "json"])
    [diagnostic] = json.loads(result.output)["diagnostics"]
    assert (result.exit_code, diagnostic["code"], diagnostic["severity"]) == (1, "SST-INT001", "error")
    assert diagnostic["message"] == "internal error: RuntimeError: boom"
    assert diagnostic["artifact"] is None and diagnostic["params"] == {"detail": "RuntimeError: boom"}


def test_sst_int001_silent() -> None:
    # A project the command cannot use is a configuration failure, not an internal one.
    result = CliRunner().invoke(command(ValueError("bad value")), ["--output", "json"])
    assert result.exit_code == 4 and json.loads(result.output)["diagnostics"] == []
