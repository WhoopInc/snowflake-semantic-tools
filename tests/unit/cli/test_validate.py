"""`sst validate`: offline rules, the configured strictness, and the connected syntax check."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli

from .helpers import FIXTURE, MANIFEST


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
    assert "validated 14 artifact(s): 0 errors, 1 warnings" in result.output


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
