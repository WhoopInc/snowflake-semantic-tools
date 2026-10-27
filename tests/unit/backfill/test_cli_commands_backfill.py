"""`sst` commands at the edges the broader suites skip, run against the reference project and fakes."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, invoke_with_port, skill_project
from tests.helpers.reference_project import break_menu_view, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def _codes(output: str) -> list[str]:
    return [item["code"] for item in json.loads(output)["diagnostics"]]


def test_a_conversation_in_a_project_with_no_agent_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.ask._interactive", lambda: True)
    refused = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["ask", "--project-dir", str(project)])
    assert refused.exit_code == 3 and "no agent compiles in this project" in refused.output


def test_baselining_every_warning_under_json_output_needs_yes(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    refused = CliRunner().invoke(cli, ["baseline", "add", "--all-warnings", *common(project), "-o", "json"])
    assert refused.exit_code == 3
    assert _codes(refused.output) == ["SST-PRT109"]


def test_debug_reads_the_dbt_block_of_a_configuration_that_resolves(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--no-connect", "-o", "json"])
    assert debugged.exit_code == 0, debugged.output
    assert json.loads(debugged.output)["data"]["manifest"]


def test_debug_still_reports_when_the_configuration_does_not_resolve(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    (project / "sst_config.yml").write_text("project: [\n", encoding="utf-8")
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--no-connect", "-o", "json"])
    assert debugged.exit_code == 4, debugged.output
    assert json.loads(debugged.output)["diagnostics"]


@pytest.mark.parametrize("command", ["test", "eval"])
def test_a_project_that_does_not_compile_runs_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    project = project_copy(tmp_path)
    break_menu_view(project)
    port = FakeSnowflake(state={})
    refused = invoke_with_port(monkeypatch, port, [command, *common(project), "--target", "dev", "-o", "json"])
    assert refused.exit_code == 1, refused.output
    assert json.loads(refused.output)["summary"]["error"] >= 1
    assert port.scripts == []


def test_a_test_run_on_another_target_than_the_compile_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    assert CliRunner().invoke(cli, ["compile", *common(project), "--target", "dev"]).exit_code == 0
    refused = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["test", *common(project), "--target", "prod", "-o", "json"]
    )
    assert refused.exit_code == 4, refused.output
    assert _codes(refused.output) == ["SST-MAN006"]


def test_a_pruning_plan_leaves_the_excluded_keys_out_of_the_prune_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    planned = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        [
            "plan",
            *common(project),
            "--target",
            "dev",
            "--prune",
            "--select",
            "*",
            "--exclude",
            "semantic_view:jaffle_menu",
            "-o",
            "json",
        ],
    )
    payload = json.loads(planned.output)
    keys = [change["artifact_key"] for change in payload["data"]["changes"]]
    assert keys and "semantic_view:jaffle_menu" not in keys
    # The agent over the excluded view cannot publish before it, which the plan says.
    assert planned.exit_code == 1 and "SST-PLN038" in _codes(planned.output)


def test_a_state_selector_with_a_state_directory_that_holds_no_manifest_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    empty = tmp_path / "previous"
    empty.mkdir()
    refused = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--state", str(empty), "--select", "state:new", "-o", "json"],
    )
    assert refused.exit_code == 4, refused.output
    assert _codes(refused.output) == ["SST-MAN001"]
