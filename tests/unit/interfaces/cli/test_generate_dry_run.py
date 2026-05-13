"""Tests for sst generate --dry-run SQL file output."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.generate import generate

_PATCH_PREFIX = "snowflake_semantic_tools.interfaces.cli.commands.generate"


@pytest.fixture
def mock_generate_result():
    result = MagicMock()
    result.success = True
    result.views_created = 2
    result.errors = []
    result.sql_statements = {
        "customer_sv": "CREATE OR REPLACE SEMANTIC VIEW customer_sv\n  TABLES (CUSTOMERS)\n  ...",
        "orders_sv": "CREATE OR REPLACE SEMANTIC VIEW orders_sv\n  TABLES (ORDERS)\n  ...",
    }
    result.print_summary = MagicMock()
    return result


def _run_generate(runner, args, mock_result):
    mock_service = MagicMock()
    mock_service.generate.return_value = mock_result
    mock_service.__enter__ = MagicMock(return_value=mock_service)
    mock_service.__exit__ = MagicMock(return_value=False)

    mock_defer = MagicMock(enabled=False, only_modified=False)
    mock_config_obj = MagicMock()
    mock_config_obj.get.side_effect = lambda key, default=None: {
        "generation.threads": 1,
        "generation.view_timeout": 300,
    }.get(key, default)

    with patch(f"{_PATCH_PREFIX}.setup_command"), patch(
        f"{_PATCH_PREFIX}.build_snowflake_config",
        return_value=MagicMock(profile_name="test", target_name="dev"),
    ), patch(f"{_PATCH_PREFIX}.get_target_database_schema", return_value=("DB", "SCHEMA")), patch(
        f"{_PATCH_PREFIX}.resolve_defer_config", return_value=mock_defer
    ), patch(
        f"{_PATCH_PREFIX}.SemanticViewGenerationService", return_value=mock_service
    ), patch(
        f"{_PATCH_PREFIX}.get_config", return_value=mock_config_obj
    ):
        return runner.invoke(generate, args)


class TestDryRunSQLOutput:

    def test_dry_run_writes_sql_files(self, tmp_path, mock_generate_result):
        runner = CliRunner()
        output_dir = str(tmp_path / "sql_out")

        result = _run_generate(runner, ["--all", "--dry-run", "--output-dir", output_dir], mock_generate_result)

        if result.exit_code != 0 and result.exception:
            raise result.exception

        assert result.exit_code == 0
        out_path = Path(output_dir)
        assert (out_path / "customer_sv.sql").exists()
        assert (out_path / "orders_sv.sql").exists()
        assert "2 file(s)" in result.output

    def test_dry_run_default_output_dir(self, tmp_path, mock_generate_result, monkeypatch):
        runner = CliRunner()
        monkeypatch.chdir(tmp_path)

        result = _run_generate(runner, ["--all", "--dry-run"], mock_generate_result)

        if result.exit_code != 0 and result.exception:
            raise result.exception

        assert result.exit_code == 0
        default_dir = tmp_path / "target" / "semantic_views"
        assert default_dir.exists()
        assert len(list(default_dir.glob("*.sql"))) == 2

    def test_non_dry_run_does_not_write_files(self, tmp_path, monkeypatch):
        runner = CliRunner()
        monkeypatch.chdir(tmp_path)

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.views_created = 1
        mock_result.errors = []
        mock_result.sql_statements = None
        mock_result.print_summary = MagicMock()

        result = _run_generate(runner, ["--all"], mock_result)

        assert result.exit_code == 0
        assert not (tmp_path / "target" / "semantic_views").exists()

    def test_sql_file_content_matches(self, tmp_path, mock_generate_result):
        runner = CliRunner()
        output_dir = str(tmp_path / "out")

        _run_generate(runner, ["--all", "--dry-run", "--output-dir", output_dir], mock_generate_result)

        content = (Path(output_dir) / "orders_sv.sql").read_text()
        assert content == mock_generate_result.sql_statements["orders_sv"]

    def test_output_dir_without_dry_run_rejected(self):
        runner = CliRunner()
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.sql_statements = None
        mock_result.print_summary = MagicMock()

        result = _run_generate(runner, ["--all", "--output-dir", "/tmp/foo"], mock_result)
        assert result.exit_code != 0
        assert "--output-dir can only be used with --dry-run" in result.output

    def test_dry_run_empty_sql_statements_warns(self, tmp_path, monkeypatch):
        runner = CliRunner()
        monkeypatch.chdir(tmp_path)

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.views_created = 0
        mock_result.errors = []
        mock_result.sql_statements = {}
        mock_result.print_summary = MagicMock()

        result = _run_generate(runner, ["--all", "--dry-run"], mock_result)
        assert "no SQL statements" in result.output or result.exit_code == 0
