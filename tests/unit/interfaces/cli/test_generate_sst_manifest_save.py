"""Tests for SST manifest save in generate command."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.generate import generate

_PATCH_PREFIX = "snowflake_semantic_tools.interfaces.cli.commands.generate"
_MANIFEST_MODULE = "snowflake_semantic_tools.core.parsing.sst_manifest"


def _run_generate(runner, args, mock_result):
    mock_service = MagicMock()
    mock_service.generate.return_value = mock_result
    mock_service.get_available_views.return_value = []
    mock_service.__enter__ = MagicMock(return_value=mock_service)
    mock_service.__exit__ = MagicMock(return_value=False)

    mock_config_obj = MagicMock()
    mock_config_obj.get.side_effect = lambda key, default=None: {
        "generation.threads": 1,
        "generation.view_timeout": 300,
    }.get(key, default)

    with patch(f"{_PATCH_PREFIX}.setup_command"), patch(
        f"{_PATCH_PREFIX}.build_snowflake_config", return_value=MagicMock()
    ), patch(f"{_PATCH_PREFIX}.get_target_database_schema", return_value=("DB", "SCHEMA")), patch(
        f"{_PATCH_PREFIX}.resolve_defer_config",
        return_value=MagicMock(
            enabled=False,
            only_modified=False,
            manifest_path=None,
            target=None,
            manifest_target_warning=None,
        ),
    ), patch(
        f"{_PATCH_PREFIX}.SemanticViewGenerationService", return_value=mock_service
    ), patch(
        f"{_PATCH_PREFIX}.get_config", return_value=mock_config_obj
    ), patch(
        f"{_PATCH_PREFIX}.ManifestParser"
    ) as mock_mp:
        mock_mp.return_value.load.return_value = False
        result = runner.invoke(generate, args, catch_exceptions=False, obj={"output_format": "table"})
    return result


class TestGenerateSSTManifestSave:
    def test_saves_manifest_on_success(self, tmp_path):
        runner = CliRunner()
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.views_created = 1
        mock_result.errors = []
        mock_result.sql_statements = {}

        with patch(f"{_MANIFEST_MODULE}.SSTManifest") as MockManifest:
            mock_inst = MagicMock()
            mock_inst.build.return_value = mock_inst
            MockManifest.return_value = mock_inst
            result = _run_generate(runner, ["--all"], mock_result)

        assert result.exit_code == 0
        mock_inst.build.assert_called_once()
        mock_inst.save.assert_called_once()

    def test_does_not_save_on_dry_run(self, tmp_path):
        runner = CliRunner()
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.views_created = 1
        mock_result.errors = []
        mock_result.sql_statements = {"v1": "CREATE SEMANTIC VIEW v1 ..."}

        with patch(f"{_MANIFEST_MODULE}.SSTManifest") as MockManifest:
            result = _run_generate(runner, ["--all", "--dry-run"], mock_result)

        MockManifest.return_value.build.assert_not_called()

    def test_does_not_save_on_failure(self, tmp_path):
        runner = CliRunner()
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.views_created = 0
        mock_result.errors = ["some error"]
        mock_result.sql_statements = {}

        with patch(f"{_MANIFEST_MODULE}.SSTManifest") as MockManifest:
            result = _run_generate(runner, ["--all"], mock_result)

        MockManifest.return_value.build.assert_not_called()
