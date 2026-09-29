"""Command-level checks for the SST 1.0 composition root."""

from __future__ import annotations

import json
import shutil
import tomllib
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


def test_sst_console_script_targets_the_one_point_zero_cli() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = project["tool"]["poetry"]["scripts"]
    assert scripts["sst"] == "snowflake_semantic_tools.cli.main:cli"
    assert scripts["snowflake-semantic-tools"] == "snowflake_semantic_tools.cli.main:cli"


def test_compile_accepts_an_explicit_manifest_without_invoking_dbt() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
            "--print-ddl",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL" in result.output
    assert "JAFFLE_SALES" not in result.output


def test_compile_rejects_an_unknown_target_before_rendering() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--target",
            "does_not_exist",
        ],
    )
    assert result.exit_code != 0
    assert "target 'does_not_exist' is absent from profile 'sst_reference_impl'" in result.output


def test_compile_writes_one_deterministic_file_per_view(tmp_path: Path) -> None:
    output_dir = tmp_path / "ddl"
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
            "--emit-ddl",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert sorted(path.name for path in output_dir.iterdir()) == ["jaffle_minimal.sql"]
    ddl = (output_dir / "jaffle_minimal.sql").read_text(encoding="utf-8")
    assert ddl.startswith("CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL")
    assert ddl.endswith(";\n")


def test_compile_writes_deterministic_manifest(tmp_path: Path) -> None:
    manifest_output = tmp_path / "sst" / "manifest.json"
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--manifest-output",
            str(manifest_output),
        ],
    )
    assert result.exit_code == 0, result.output
    document = json.loads(manifest_output.read_text(encoding="utf-8"))
    assert document["schema_version"] == 2
    assert sorted(document["artifacts"]) == [
        "agent:jaffle_analytics_agent",
        "agent:jaffle_delivery_agent",
        "agent:jaffle_minimal_agent",
        "eval:jaffle_analytics_agent",
        "plugin:jaffle-toolkit",
        "profile:jaffle-analyst",
        "semantic_view:jaffle_menu",
        "semantic_view:jaffle_minimal",
        "semantic_view:jaffle_sales",
        "skill:jaffle-catalogue",
        "skill:jaffle-operations",
        "skill:jaffle-semantics",
        "tool:menu_docs_search",
    ]
    assert "generated_at" not in document


def test_compile_json_emits_artifact_fingerprints() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert len(envelope["data"]["artifacts"]) == 13
    assert all(len(artifact["fingerprint"]) == 64 for artifact in envelope["data"]["artifacts"])


def test_compile_json_honors_selection() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    artifacts = json.loads(result.output)["data"]["artifacts"]
    assert [artifact["artifact_key"] for artifact in artifacts] == ["semantic_view:jaffle_minimal"]


def test_compile_selected_manifest_contains_only_selected_view(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
            "--manifest-output",
            str(path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert tuple(json.loads(path.read_text(encoding="utf-8"))["artifacts"]) == ("semantic_view:jaffle_minimal",)


def test_compile_selection_keeps_the_canonical_manifest_full(tmp_path: Path) -> None:
    import shutil

    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)
    shutil.rmtree(project / "target", ignore_errors=True)
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
        ],
    )
    assert result.exit_code == 0, result.output
    canonical = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    assert len(canonical["artifacts"]) == 13


def test_validate_accepts_the_recorded_manifest_offline() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0, result.output
    # Two warnings are SST-VAL804: no agent references jaffle-catalogue or jaffle-operations.
    assert "validated 13 artifact(s): 0 errors, 2 warnings" in result.output


def test_validate_uses_config_strict_unless_cli_overrides() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
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
            str(MANIFEST),
            "--snowflake-syntax-check",
        ],
    )
    assert result.exit_code != 0
    assert "User is empty" in result.output


def test_validate_reports_view_error_but_compile_fails_closed(tmp_path: Path) -> None:
    import shutil

    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)
    views_path = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    text = views_path.read_text(encoding="utf-8")
    text = text.replace("{{ ref('products') }}", "{{ ref('missing') }}", 1)
    views_path.write_text(text, encoding="utf-8")

    common = ["--project-dir", str(project), "--manifest", str(MANIFEST)]
    validated = CliRunner().invoke(cli, ["validate", *common, "--no-snowflake-syntax-check"])
    assert validated.exit_code != 0
    assert "error[SST-REF001]" in validated.output
    assert "ref('missing')" in validated.output

    compiled = CliRunner().invoke(cli, ["compile", *common])
    assert compiled.exit_code != 0
    assert "ref('missing')" in compiled.output


def test_validate_json_emits_one_v2_envelope() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--output",
            "json",
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert envelope["tool"] == "sst"
    assert envelope["schema_version"] == 2
    assert envelope["sst_version"] == "1.0.0.dev0"
    assert envelope["invocation"]["argv"][1] == "validate"
    assert envelope["invocation"]["project_dir"] == str(FIXTURE.resolve())
    assert envelope["invocation"]["config_file"] == str((FIXTURE / "sst_config.yml").resolve())
    assert envelope["invocation"]["started_at"]
    assert envelope["invocation"]["duration_s"] >= 0
    assert envelope["status"] == "ok"
    # Seven infos are SST-CFG044, keys the fixture sets that 1.0 reads nowhere, and
    # one is SST-VAL854: the fixture's profile registry is not Desktop's.
    assert envelope["summary"] == {
        "error": 0,
        "warning": 2,
        "info": 12,
        "promoted": 0,
        "suppressed_cascade": 0,
        "baselined": 0,
    }


def test_global_output_and_project_dir_are_forwarded_to_command_defaults() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "--output",
            "json",
            "--project-dir",
            str(FIXTURE),
            "validate",
            "--manifest",
            str(MANIFEST),
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0
    assert json.loads(result.output)["command"] == "validate"


def test_golden_suite_compares_every_compiled_view() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "test",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--suite",
            "golden",
            "--golden-dir",
            str(REPO_ROOT / "tests" / "golden" / "expected" / "ddl"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "golden suite passed for 13 artifact(s)" in result.output


def test_golden_suite_reports_a_diff(tmp_path: Path) -> None:
    expected_root = tmp_path / "expected"
    shutil.copytree(REPO_ROOT / "tests" / "golden" / "expected", expected_root)
    golden_dir = expected_root / "ddl"
    path = golden_dir / "jaffle_minimal.sql"
    path.write_text(path.read_text(encoding="utf-8").replace("Menu products only", "Drifted"), encoding="utf-8")
    result = CliRunner().invoke(
        cli,
        [
            "test",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--suite",
            "golden",
            "--golden-dir",
            str(golden_dir),
        ],
    )
    assert result.exit_code != 0
    assert "golden suite failed" in result.output
    assert "-  COMMENT = 'Drifted" in result.output


def test_golden_suite_compares_eval_source_sql(tmp_path: Path) -> None:
    expected_root = tmp_path / "expected"
    shutil.copytree(REPO_ROOT / "tests" / "golden" / "expected", expected_root)
    source = expected_root / "eval" / "jaffle_analytics_source.sql"
    source.write_text(
        source.read_text(encoding="utf-8").replace("GROUND_TRUTH VARIANT", "GROUND_TRUTH VARCHAR"),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli,
        [
            "test",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--suite",
            "golden",
            "--golden-dir",
            str(expected_root / "ddl"),
        ],
    )

    assert result.exit_code == 1
    assert "jaffle_analytics_source.sql" in result.output
    assert "GROUND_TRUTH VARCHAR" in result.output


def test_plan_requires_live_snowflake_observation() -> None:
    compiled = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
        ],
    )
    assert compiled.exit_code == 0
    result = CliRunner().invoke(
        cli,
        [
            "plan",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 5
    assert json.loads(result.output)["exit_code"] == 5
