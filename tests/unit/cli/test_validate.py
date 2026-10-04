"""`sst validate`: offline rules, the configured strictness, and the connected syntax check."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.app.validate import CONNECTED_RULES, VALIDATION_RULES
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.reference_project import DBT_MANIFEST, FIXTURE, project_copy


def test_validate_accepts_the_recorded_manifest_offline() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(DBT_MANIFEST),
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0, result.output
    # SST-VAL528, SST-RND010 for the minimal agent's empty tool list, SST-RND013 for the generic
    # tool's resources, and SST-CFG018 for each of the two partner tool members nothing references.
    assert "validated 14 artifact(s): 0 errors, 5 warnings" in result.output


def test_validate_uses_config_strict_unless_cli_overrides() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(DBT_MANIFEST),
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0


def test_validate_connected_syntax_check_requires_a_connection() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(DBT_MANIFEST),
            "--snowflake-syntax-check",
        ],
    )
    assert result.exit_code != 0
    assert "Password is empty" in result.output


def test_validate_reports_view_error_but_compile_fails_closed(tmp_path: Path) -> None:
    import shutil

    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project, ignore=shutil.ignore_patterns("target"))
    views_path = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    text = views_path.read_text(encoding="utf-8")
    text = text.replace("{{ ref('products') }}", "{{ ref('missing') }}", 1)
    views_path.write_text(text, encoding="utf-8")

    common = ["--project-dir", str(project), "--manifest", str(DBT_MANIFEST)]
    validated = CliRunner().invoke(cli, ["validate", *common, "--no-snowflake-syntax-check"])
    assert validated.exit_code != 0
    assert "error[SST-REF001]" in validated.output
    assert "ref('missing')" in validated.output

    compiled = CliRunner().invoke(cli, ["compile", *common])
    assert compiled.exit_code != 0
    assert "ref('missing')" in compiled.output


def test_validate_reports_an_agent_loader_error_once(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    broken = project / "agents" / "broken"
    broken.mkdir(parents=True)
    (broken / "agent.yml").write_text("name: broken_agent\nmeta: text\n", encoding="utf-8")

    result = CliRunner().invoke(cli, ["validate", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST)])

    assert result.exit_code == 1, result.output
    # The eval catalog once carried the agent loader's diagnostics too, so each was reported twice.
    assert result.output.count("'meta' expects a mapping, found str") == 1


def test_validate_json_data_counts_rules_artifacts_and_what_the_baseline_suppresses(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    args = ["--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "--no-strict", "-o", "json"]
    plain = CliRunner().invoke(cli, ["validate", *args])
    data = json.loads(plain.output)["data"]
    assert data == {
        "rules_run": len(VALIDATION_RULES) - len(CONNECTED_RULES),
        "artifacts_checked": 14,
        "suppressed_by_baseline": 0,
    }
    added = CliRunner().invoke(cli, ["baseline", "add", "--all-warnings", "--yes", *args[:4]])
    assert added.exit_code == 0, added.output
    baselined = CliRunner().invoke(cli, ["validate", *args])
    envelope = json.loads(baselined.output)
    assert envelope["data"]["suppressed_by_baseline"] == envelope["summary"]["baselined"] == 5
