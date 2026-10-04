"""`sst debug`: the resolved target and, with `--test-connection`, the live session."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def test_debug_connection_and_human_apply_prompt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    debug_port = FakeSnowflake(role="R", account_locator="A")
    debugged = invoke_with_port(
        monkeypatch,
        debug_port,
        ["debug", "--project-dir", str(project), "--output", "json"],
    )
    assert debugged.exit_code == 0
    assert json.loads(debugged.output)["data"]["connection"] == {
        "tested": True,
        "ok": True,
        "role": "R",
        "account": "A",
    }

    apply_port = FakeSnowflake(state={})
    applied = invoke_with_port(
        monkeypatch,
        apply_port,
        ["apply", *common(project), "--target", "dev"],
    )
    assert applied.exit_code != 0 and "Apply this plan?" in applied.output


def test_debug_reports_every_candidate_and_refuses_what_it_cannot_resolve(tmp_path: Path) -> None:
    from click.testing import CliRunner

    from snowflake_semantic_tools.cli.main import cli

    missing = CliRunner().invoke(cli, ["debug", "--project-dir", str(tmp_path), "--output", "json"])
    envelope = json.loads(missing.output)
    assert (missing.exit_code, envelope["diagnostics"][0]["code"]) == (4, "SST-CFG001")
    assert [item["exists"] for item in envelope["data"]["config"]["candidates"]] == [False, False]
    (tmp_path / "sst_config.yml").write_text("validation:\n  snowflake_syntax_check: false\n", encoding="utf-8")
    (tmp_path / "dbt_project.yml").write_text("profile: nope\n", encoding="utf-8")
    unresolved = CliRunner().invoke(cli, ["debug", "--project-dir", str(tmp_path), "--output", "json"])
    assert (unresolved.exit_code, json.loads(unresolved.output)["diagnostics"][0]["code"]) == (4, "SST-CFG009")
    human = CliRunner().invoke(cli, ["debug", "--project-dir", str(tmp_path)])
    assert human.exit_code == 4 and "candidates:" in human.output


def test_debug_reports_a_refused_connection_and_the_signature_rate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from click.testing import CliRunner

    from snowflake_semantic_tools.cli.main import cli
    from snowflake_semantic_tools.cli.run_log import append_run_log
    from snowflake_semantic_tools.domain.diagnostics import D
    from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

    project = project_copy(tmp_path)

    def refuse(params: object) -> FakeSnowflake:
        raise SnowflakePortError("refused")

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", refuse)
    refused = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--output", "json"])
    payload = json.loads(refused.output)
    assert refused.exit_code == 5 and payload["data"]["connection"]["ok"] is False
    assert [item["code"] for item in payload["diagnostics"]][-1] == "SST-PRT001"

    build = project / "target" / "sst"
    append_run_log(project, build, (D("SST-PRT001", value="a", detail="b"),), command="plan")
    assert not (build / "run_log.jsonl").exists()
    append_run_log(project, build, (D("SST-SNO001", detail="x"), D("SST-SNO002", value="y")), command="apply")
    (build / "run_log.jsonl").open("a", encoding="utf-8").write("not json\n")
    signatures = CliRunner().invoke(
        cli, ["debug", "--project-dir", str(project), "--snowflake-signatures", "--output", "json"]
    )
    report = json.loads(signatures.output)["data"]["signatures"]
    assert (report["matched"], report["unmatched"], report["sno001_rate"]) == (1, 1, 0.5)
    empty = CliRunner().invoke(
        cli, ["debug", "--project-dir", str(project_copy(tmp_path / "e")), "--snowflake-signatures"]
    )
    assert "sno001_rate: 0.0" in empty.output


@pytest.mark.parametrize("output_format", ["json", "human"])
def test_debug_never_prints_a_credential_a_failed_connection_echoes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output_format: str
) -> None:
    """The leak guard knows the profile's secrets, so it catches what the message scrubber misses."""
    from click.testing import CliRunner

    from snowflake_semantic_tools.cli import output
    from snowflake_semantic_tools.cli.main import cli
    from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

    project = project_copy(tmp_path)
    profiles = project / "profiles.yml"
    text = profiles.read_text(encoding="utf-8")
    profiles.write_text(text.replace("user: sst_reference\n", "user: sst_reference\n      password: hunter2-x\n", 1))
    monkeypatch.setattr(output, "_SECRETS", set())

    def refuse(params: object) -> FakeSnowflake:
        raise SnowflakePortError("login as hunter2-x refused")

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", refuse)
    refused = CliRunner().invoke(
        cli, ["debug", "--project-dir", str(project), "--target", "dev", "--output", output_format]
    )
    assert "hunter2-x" not in refused.output
    assert "SST-PRT012" in refused.output and refused.exit_code == 1
