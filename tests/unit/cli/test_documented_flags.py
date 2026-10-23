"""The flags the CLI reference documents beyond the core path: path overrides, schema checks, smoke runs."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.project import target_dir
from tests.helpers.cli_projects import applied_port, common, compile_project, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def test_validate_verify_schema_connects_and_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    result = invoke_with_port(
        monkeypatch, FakeSnowflake(), ["validate", *common(project), "--verify-schema", "-o", "json"]
    )
    envelope = json.loads(result.stdout)
    assert result.exit_code == 1
    assert "SST-PRT005" in {item["code"] for item in envelope["diagnostics"]}
    offline = CliRunner().invoke(cli, ["validate", *common(project), "-o", "json"])
    assert "SST-PRT005" not in {item["code"] for item in json.loads(offline.stdout)["diagnostics"]}


def test_semantic_reads_another_directory_and_dbt_replaces_model_paths(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    empty = project / "empty_semantic"
    empty.mkdir()
    elsewhere = CliRunner().invoke(cli, ["compile", *common(project), "--semantic", str(empty), "-o", "json"])
    assert elsewhere.exit_code == 4
    assert "empty_semantic" in json.loads(elsewhere.stdout)["data"]["error"]
    dbt = project / "dbt_project.yml"
    text = dbt.read_text(encoding="utf-8")
    dbt.write_text(re.sub(r"(?m)^model-paths:.*$", "model-paths: 5", text), encoding="utf-8")
    unreadable = CliRunner().invoke(cli, ["compile", *common(project), "-o", "json"])
    assert "SST-DBT022" in unreadable.stdout
    replaced = CliRunner().invoke(cli, ["compile", *common(project), "--dbt", str(project / "models"), "-o", "json"])
    assert replaced.exit_code == 0, replaced.output
    assert "SST-DBT022" not in replaced.stdout
    assert CliRunner().invoke(cli, ["validate", *common(project), "--dbt", str(project / "missing")]).exit_code == 3


def test_list_without_a_manifest_compiles_in_memory_and_writes_nothing(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    assert CliRunner().invoke(cli, ["list", *common(project)]).exit_code == 4
    listed = CliRunner().invoke(cli, ["list", *common(project), "--no-manifest", "semantic-views", "-o", "json"])
    assert listed.exit_code == 0, listed.output
    assert json.loads(listed.stdout)["data"]["count"] == 3
    assert not target_dir(project).exists()


def test_test_needs_the_compiled_manifest_then_probes_without_running_an_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    uncompiled = CliRunner().invoke(cli, ["test", *common(project), "-o", "json"])
    assert uncompiled.exit_code == 4, uncompiled.output
    assert [item["code"] for item in json.loads(uncompiled.stdout)["diagnostics"]] == ["SST-MAN001"]
    compile_project(project)
    port = applied_port(project)
    result = invoke_with_port(monkeypatch, port, ["test", *common(project), "--target", "dev", "-o", "json"])
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope["command"] == "test" and envelope["data"]["attempted"] > 0
    assert not any("EXECUTE_AI_EVALUATION" in str(sql) for sql in port.queries)


def test_debug_lists_fragile_signatures_and_refuses_an_unsupported_manifest(tmp_path: Path) -> None:
    from snowflake_semantic_tools.domain.diagnostics.signatures import fragile_signatures

    project = project_copy(tmp_path)
    signatures = CliRunner().invoke(
        cli, ["debug", "--project-dir", str(project), "--snowflake-signatures", "-o", "json"]
    )
    fragile = json.loads(signatures.stdout)["data"]["signatures"]["fragile"]
    assert [item["code"] for item in fragile] == [row.code for row in fragile_signatures()]
    manifest = project / "target" / "manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text('{"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v99.json"}}')
    refused = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--no-connect", "-o", "json"])
    assert refused.exit_code == 1
    assert "SST-DBT017" in {item["code"] for item in json.loads(refused.stdout)["diagnostics"]}
    allowed = CliRunner().invoke(
        cli, ["debug", "--project-dir", str(project), "--no-connect", "--allow-unsupported-manifest-schema"]
    )
    assert allowed.exit_code == 0, allowed.output
