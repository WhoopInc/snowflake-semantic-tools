"""SST-INT001: an exception no handler expects reached the one crash handler; the run exits 1."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_int001_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.compile.compile_result", broken)
    result = CliRunner().invoke(cli, ["validate", *common(project_copy(tmp_path)), "--output", "json"])
    [diagnostic] = json.loads(result.output)["diagnostics"]
    assert (result.exit_code, diagnostic["code"], diagnostic["severity"]) == (1, "SST-INT001", "error")
    assert diagnostic["message"] == "internal error: RuntimeError: boom"


def test_sst_int001_silent(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["validate", *common(project_copy(tmp_path)), "--output", "json"])
    assert result.exit_code == 0 and "SST-INT001" not in result.output
