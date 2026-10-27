"""`--select` and `--exclude` on `sst test` and `sst eval`: which artifacts each covers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import applied_port, common, compile_project, invoke_with_port
from tests.helpers.reference_project import project_copy


def _smoke_probes(monkeypatch: pytest.MonkeyPatch, project: Path, *flags: str) -> int:
    """Run `sst test` on `project`, every artifact applied; return how many probes it ran."""
    args = ["test", *common(project), "--target", "dev", *flags, "-o", "json"]
    result = invoke_with_port(monkeypatch, applied_port(project), args)
    assert result.exit_code == 0, result.output
    return int(json.loads(result.output)["data"]["attempted"])


def test_select_and_exclude_narrow_the_smoke_probes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    every = _smoke_probes(monkeypatch, project)
    selected = _smoke_probes(monkeypatch, project, "--select", "jaffle_minimal")
    excluded = _smoke_probes(monkeypatch, project, "--exclude", "jaffle_minimal")
    assert 0 < selected < every
    assert selected + excluded == every

    nothing = CliRunner().invoke(cli, ["test", *common(project), "--select", "no_such_artifact", "-o", "json"])
    assert nothing.exit_code == 3
    assert "SST-DIS010" in {item["code"] for item in json.loads(nothing.stdout)["diagnostics"]}


def test_a_selected_connected_run_still_checks_the_whole_compiled_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    def unreachable(params: object) -> None:
        raise SnowflakePortError("unreachable", diagnostic=None)

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", unreachable)
    smoke = CliRunner().invoke(cli, ["test", *common(project), "--select", "jaffle_minimal"])
    assert smoke.exit_code == 5, smoke.output
    evals = CliRunner().invoke(cli, ["eval", *common(project), "--select", "type:eval"])
    assert evals.exit_code == 5, evals.output
