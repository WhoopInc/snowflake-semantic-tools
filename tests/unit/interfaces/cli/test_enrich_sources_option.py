"""Tests for enrich CLI source options."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.enrich import enrich


class TestEnrichSourceCLIFlags:

    def test_sources_only_flag(self):
        runner = CliRunner()
        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            mock_svc = MagicMock()
            mock_svc.enrich.return_value = MagicMock(status="complete", processed=0, total=0, errors=[], results=[])
            mock_service.return_value = mock_svc

            result = runner.invoke(enrich, ["--sources-only"])
            assert result.exit_code == 0

    def test_source_flag_with_valid_selector(self):
        runner = CliRunner()
        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            mock_svc = MagicMock()
            mock_svc.enrich.return_value = MagicMock(status="complete", processed=0, total=0, errors=[], results=[])
            mock_service.return_value = mock_svc

            result = runner.invoke(enrich, ["--source", "raw.orders"])
            assert result.exit_code == 0

    def test_source_flag_invalid_selector(self):
        runner = CliRunner()
        result = runner.invoke(enrich, ["--source", "invalid_no_dot"])
        assert result.exit_code != 0
        assert "SST-E010" in result.output

    def test_sources_only_with_target_path_rejected(self):
        runner = CliRunner()
        result = runner.invoke(enrich, ["models/", "--sources-only"])
        assert result.exit_code != 0
        assert "--sources-only cannot be combined" in result.output

    def test_sources_only_with_models_rejected(self):
        runner = CliRunner()
        result = runner.invoke(enrich, ["--models", "customers", "--sources-only"])
        assert result.exit_code != 0
        assert "--sources-only cannot be combined" in result.output

    def test_source_with_target_path_rejected(self):
        runner = CliRunner()
        result = runner.invoke(enrich, ["models/", "--source", "raw.orders"])
        assert result.exit_code != 0
        assert "--source cannot be combined" in result.output

    def test_source_with_include_sources_rejected(self):
        runner = CliRunner()
        result = runner.invoke(enrich, ["--source", "raw.orders", "--include-sources"])
        assert result.exit_code != 0

    def test_sources_only_and_source_rejected(self):
        runner = CliRunner()
        result = runner.invoke(enrich, ["--sources-only", "--source", "raw.orders"])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_include_sources_with_models(self):
        runner = CliRunner()
        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names",
            return_value=["models/customers.sql"],
        ), patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService") as mock_service:
            mock_svc = MagicMock()
            mock_svc.enrich.return_value = MagicMock(status="complete", processed=1, total=1, errors=[], results=[])
            mock_service.return_value = mock_svc

            result = runner.invoke(enrich, ["--models", "customers", "--include-sources"])
            if result.exit_code != 0:
                pass
            config_call = mock_service.call_args
            assert config_call is not None
            config = config_call[0][0]
            assert config.include_sources is True
