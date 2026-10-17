"""`sst debug`: the resolved target and, with `--test-connection`, the live session."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from tests.helpers.cli_projects import common, invoke_with_port, on_a_terminal
from tests.helpers.reference_project import project_copy
from tests.helpers.seam_projects import FakeDbt, SmallProject, manifest
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
    on_a_terminal(monkeypatch)
    applied = invoke_with_port(
        monkeypatch,
        apply_port,
        ["apply", *common(project), "--target", "dev"],
    )
    assert applied.exit_code != 0 and "Apply this plan?" in applied.output


def _debug_data(monkeypatch: pytest.MonkeyPatch, root: Path, config: str, dbt: FakeDbt) -> dict[str, Any]:
    from click.testing import CliRunner

    from snowflake_semantic_tools.cli.commands import debug
    from snowflake_semantic_tools.cli.main import cli

    SmallProject(root, files={"sst_config.yml": config, "dbt_project.yml": DBT_PROJECT}).write()
    document = manifest()
    document["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v11.json"
    (root / "out").mkdir()
    (root / "out" / "manifest.json").write_text(json.dumps(document), encoding="utf-8")
    monkeypatch.setattr(debug, "dbt_runner", dbt)
    result = CliRunner().invoke(cli, ["debug", "--project-dir", str(root), "--no-connect", "-o", "json"])
    envelope: dict[str, Any] = json.loads(result.stdout)
    envelope["data"]["exit_code"] = result.exit_code
    return envelope


DBT_PROJECT = "name: fixture\nprofile: fixture\nmodel-paths: [models]\ntarget-path: out\n"
CONFIG = "project:\n  semantic_models_dir: semantic_models\nvalidation:\n  snowflake_syntax_check: false\n"


def test_debug_reports_the_dbt_sst_runs_and_the_manifest_it_accepts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dbt = FakeDbt(version=CompletedRun(0, "Core:\n  - installed: 1.9.4\n  - latest: 1.9.4\n", ""))
    accepted = CONFIG + "dbt:\n  command: compile\n  manifest_schema_versions: [11]\n"
    envelope = _debug_data(monkeypatch, tmp_path / "accepted", accepted, dbt)
    data = envelope["data"]
    # The version is the one `dbt --version` reports, which is the dbt SST runs.
    assert dbt.runs == [("dbt", "--version")]
    assert data["versions"]["dbt"] == "1.9.4"
    assert data["versions"]["dbt_installation"] == "core"
    assert data["manifest"]["source"] == "dbt compile" and data["manifest"]["runs_dbt"] is True
    assert data["manifest"]["path"] == str(tmp_path / "accepted" / "out" / "manifest.json")
    assert (data["manifest"]["accepted_schema_versions"], data["manifest"]["supported"]) == (["v11"], True)
    assert data["exit_code"] == 0 and "SST-DBT017" not in {item["code"] for item in envelope["diagnostics"]}

    unread = FakeDbt()
    refused = _debug_data(monkeypatch, tmp_path / "refused", CONFIG + "dbt:\n  invoke: false\n", unread)
    assert unread.runs == []
    assert refused["data"]["versions"]["dbt"] is None
    assert refused["data"]["manifest"]["source"] == "on disk (dbt.invoke: false)"
    assert refused["data"]["manifest"]["accepted_schema_versions"] == ["v12"]
    [diagnostic] = [item for item in refused["diagnostics"] if item["code"] == "SST-DBT017"]
    assert "v12" in diagnostic["message"]
    assert refused["data"]["exit_code"] == 1


@pytest.mark.parametrize(("banner", "installation"), [("dbt 2.0.8\n", "v2"), ("dbt-oss 2.0.8\n", "v2-oss")])
def test_debug_reports_a_dbt_v2_with_its_distribution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, banner: str, installation: str
) -> None:
    envelope = _debug_data(monkeypatch, tmp_path / "v2", CONFIG, FakeDbt(version=CompletedRun(0, banner, "")))
    versions = envelope["data"]["versions"]
    assert (versions["dbt"], versions["dbt_installation"]) == ("2.0.8", installation)
    assert "SST-DBT020" not in {item["code"] for item in envelope["diagnostics"]}


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
