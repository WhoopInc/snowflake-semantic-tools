"""SST-INT010: a refusal reaches the runner with no diagnostic of its own; it still exits 4."""

from __future__ import annotations

import json
from pathlib import Path

import click
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body
from snowflake_semantic_tools.domain.diagnostics import D, Severity


def _refusing(error: Exception) -> click.Command:
    @click.command()
    @command_body("demo", config=ConfigNeed.OPTIONAL)
    def demo() -> CommandResult:
        """Refuse as `error` does."""
        raise error

    return demo


def test_sst_int010_fires(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        _refusing(ValueError("bad value")), ["--project-dir", str(tmp_path), "--output", "json"]
    )
    envelope = json.loads(result.output)
    [diagnostic] = envelope["diagnostics"]
    assert (result.exit_code, diagnostic["code"], diagnostic["severity"]) == (4, "SST-INT010", "error")
    assert diagnostic["message"] == "uncoded refusal: bad value"
    assert (envelope["summary"]["error"], envelope["data"]["error"]) == (1, "bad value")


def test_sst_int010_fires_for_a_project_error_built_without_one() -> None:
    [diagnostic] = ProjectError("no code", diagnostics=()).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-INT010", Severity.ERROR)
    assert diagnostic.message == "uncoded refusal: no code"


def test_sst_int010_silent(tmp_path: Path) -> None:
    coded = ProjectError(diagnostics=(D("SST-MAN001", path="m.json"),))
    assert str(coded) == "no SST manifest at m.json"
    result = CliRunner().invoke(_refusing(coded), ["--project-dir", str(tmp_path), "--output", "json"])
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-MAN001"]
