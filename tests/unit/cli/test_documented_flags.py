"""The flags the CLI reference documents beyond the core path: path overrides, schema checks, suites."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.verify_schema import _references, verify_columns
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.cli_projects import DBT_MANIFEST, REPO_ROOT, common, compile_project, invoke_with_port, project_copy
from tests.helpers.recorded_snowflake import RecordedSnowflake

GOLDEN = REPO_ROOT / "tests" / "golden" / "expected" / "ddl"


def _tables(project: Path) -> tuple[object, dict[str, tuple[tuple[str, str], ...]]]:
    from snowflake_semantic_tools.adapters.locations import locate_project

    compiled = compiling.compile_result(locate_project(project), None, DBT_MANIFEST)
    tables: dict[str, set[str]] = {}
    for item in compiled.compiled:
        if isinstance(item, CompiledView):
            for fqn, _model, column in _references(item):
                tables.setdefault(fqn, set()).add(column.upper())
    return compiled, {fqn: tuple((name, "TEXT") for name in sorted(names)) for fqn, names in tables.items()}


def test_verify_columns_reports_absent_tables_and_columns_only(tmp_path: Path) -> None:
    compiled, tables = _tables(project_copy(tmp_path))
    port = RecordedSnowflake()
    port.tables = {QualifiedName.parse(fqn).sql: columns for fqn, columns in tables.items()}
    assert verify_columns(port, compiled) == ()  # type: ignore[arg-type]
    first = sorted(tables)[0]
    port.tables[QualifiedName.parse(first).sql] = tables[first][1:]
    second = sorted(tables)[1]
    del port.tables[QualifiedName.parse(second).sql]
    found = verify_columns(port, compiled)  # type: ignore[arg-type]
    codes = sorted({item.code for item in found})
    assert codes == ["SST-PRT005", "SST-VAL325"]
    [absent] = [item for item in found if item.code == "SST-PRT005"]
    assert absent.context["value"] == second
    assert any(item.context["column"].upper() == tables[first][0][0] for item in found if item.code == "SST-VAL325")


def test_validate_verify_schema_connects_and_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    result = invoke_with_port(
        monkeypatch, RecordedSnowflake(), ["validate", *common(project), "--verify-schema", "-o", "json"]
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
    listed = CliRunner().invoke(cli, ["list", *common(project), "--no-manifest", "semantic_view", "-o", "json"])
    assert listed.exit_code == 0, listed.output
    assert json.loads(listed.stdout)["data"]["count"] == 3
    assert not target_dir(project).exists()


def test_test_runs_every_applicable_suite_by_default(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    offline = CliRunner().invoke(cli, ["test", *common(project), "--golden-dir", str(GOLDEN), "-o", "json"])
    assert offline.exit_code == 0, offline.output
    data = json.loads(offline.stdout)["data"]
    assert (data["suites"], data["skipped_suites"], data["passed"]) == (["golden"], ["smoke", "evals"], 1)
    human = CliRunner().invoke(cli, ["test", *common(project), "--golden-dir", str(GOLDEN)])
    assert "1 suite(s) passed, 0 failed, 2 skipped" in human.output
    failing = CliRunner().invoke(cli, ["test", *common(project), "--golden-dir", str(tmp_path), "--fail-fast"])
    assert failing.exit_code == 1


def test_compiled_projects_add_the_connected_suites(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    result = invoke_with_port(
        monkeypatch, RecordedSnowflake(state={}), ["test", *common(project), "--golden-dir", str(GOLDEN), "-o", "json"]
    )
    data = json.loads(result.stdout)["data"]
    assert data["suites"][0] == "golden" and "smoke" in data["suites"]


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
