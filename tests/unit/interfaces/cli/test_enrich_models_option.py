"""
Unit tests for the --models option in sst enrich command.

Tests Issue #72: Simplify sst enrich with --models option
"""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.enrich import _resolve_model_names, enrich
from snowflake_semantic_tools.services.enrich_metadata import EnrichmentConfig

MANIFEST_PARSER_PATH = "snowflake_semantic_tools.core.parsing.parsers.manifest_parser.ManifestParser"


class TestResolveModelNames:
    """Tests for _resolve_model_names helper function."""

    def test_resolve_single_model(self):
        """Test resolving a single model name."""
        output = Mock()
        output.debug = Mock()

        with patch(MANIFEST_PARSER_PATH) as mock_parser_class:
            mock_parser = Mock()
            mock_parser.load.return_value = True
            mock_parser.get_location.return_value = {
                "database": "PROD",
                "schema": "ANALYTICS",
                "original_file_path": "models/marts/customers.sql",
            }
            mock_parser_class.return_value = mock_parser

            result = _resolve_model_names(["customers"], None, output)

            assert result == ["models/marts/customers.sql"]
            mock_parser.get_location.assert_called_once_with("customers")

    def test_resolve_multiple_models(self):
        """Test resolving multiple model names."""
        output = Mock()
        output.debug = Mock()

        with patch(MANIFEST_PARSER_PATH) as mock_parser_class:
            mock_parser = Mock()
            mock_parser.load.return_value = True
            mock_parser.get_location.side_effect = [
                {"original_file_path": "models/marts/customers.sql"},
                {"original_file_path": "models/marts/orders.sql"},
            ]
            mock_parser_class.return_value = mock_parser

            result = _resolve_model_names(["customers", "orders"], None, output)

            assert result == [
                "models/marts/customers.sql",
                "models/marts/orders.sql",
            ]

    def test_resolve_model_not_found(self):
        """Test error when model is not found in manifest."""
        output = Mock()
        output.debug = Mock()

        with patch(MANIFEST_PARSER_PATH) as mock_parser_class:
            mock_parser = Mock()
            mock_parser.load.return_value = True
            mock_parser.get_location.return_value = None
            mock_parser.model_locations = {"customers": {}, "orders": {}}
            mock_parser_class.return_value = mock_parser

            import click

            with pytest.raises(click.ClickException) as exc_info:
                _resolve_model_names(["nonexistent"], None, output)

            assert "not found in manifest" in str(exc_info.value)
            assert "nonexistent" in str(exc_info.value)

    def test_resolve_no_manifest(self):
        """Test error when manifest is not found."""
        output = Mock()
        output.debug = Mock()

        with patch(MANIFEST_PARSER_PATH) as mock_parser_class:
            mock_parser = Mock()
            mock_parser.load.return_value = False
            mock_parser_class.return_value = mock_parser

            import click

            with pytest.raises(click.ClickException) as exc_info:
                _resolve_model_names(["customers"], None, output)

            assert "manifest" in str(exc_info.value).lower()

    def test_resolve_with_explicit_manifest_path(self):
        """Test resolving with explicit manifest path."""
        output = Mock()
        output.debug = Mock()
        manifest_path = Path("/path/to/manifest.json")

        with patch(MANIFEST_PARSER_PATH) as mock_parser_class:
            mock_parser = Mock()
            mock_parser.load.return_value = True
            mock_parser.get_location.return_value = {"original_file_path": "models/customers.sql"}
            mock_parser_class.return_value = mock_parser

            _resolve_model_names(["customers"], manifest_path, output)

            mock_parser_class.assert_called_once_with(manifest_path)


class TestEnrichmentConfigWithModelFiles:
    """Tests for EnrichmentConfig with model_files parameter."""

    def test_config_with_target_path(self):
        """Test config creation with target_path."""
        config = EnrichmentConfig(target_path="models/")
        assert config.target_path == "models/"
        assert config.model_files is None

    def test_config_with_model_files(self):
        """Test config creation with model_files."""
        config = EnrichmentConfig(model_files=["models/customers.sql", "models/orders.sql"])
        assert config.target_path is None
        assert config.model_files == ["models/customers.sql", "models/orders.sql"]

    def test_config_requires_either_path_or_files(self):
        """Test that config requires either target_path or model_files."""
        with pytest.raises(ValueError) as exc_info:
            EnrichmentConfig()
        assert "Either target_path or model_files must be provided" in str(exc_info.value)


class TestEnrichCLIModelsOption:
    """Tests for the enrich CLI command with --models option."""

    def test_models_option_without_path(self):
        """Test that --models works without TARGET_PATH."""
        runner = CliRunner()

        with patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names"
        ) as mock_resolve, patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            mock_resolve.return_value = ["models/customers.sql"]

            # Mock the service
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.enrich = Mock(
                return_value=Mock(
                    status="success",
                    models_enriched=1,
                    failed_models=[],
                    print_summary=Mock(),
                )
            )
            mock_instance.close = Mock()
            mock_service.return_value = mock_instance

            result = runner.invoke(enrich, ["--models", "customers"])

            # Check that resolve was called
            mock_resolve.assert_called_once()
            # First arg is the list of model names
            assert mock_resolve.call_args[0][0] == ["customers"]

    def test_mutual_exclusivity_error(self, tmp_path):
        """Test error when both TARGET_PATH and --models are provided."""
        runner = CliRunner()

        # Create a temp directory to use as target_path
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(enrich, [str(models_dir), "--models", "customers"])

        assert result.exit_code != 0
        assert "Specify either TARGET_PATH or --models, not both" in result.output

    def test_missing_argument_error(self):
        """Test error when neither TARGET_PATH nor --models is provided."""
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"):
            result = runner.invoke(enrich, [])

            assert result.exit_code != 0
            assert "Missing required argument" in result.output

    def test_models_comma_separated(self):
        """Test that --models accepts comma-separated values."""
        runner = CliRunner()

        with patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names"
        ) as mock_resolve, patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            mock_resolve.return_value = [
                "models/customers.sql",
                "models/orders.sql",
            ]

            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.enrich = Mock(
                return_value=Mock(
                    status="success",
                    models_enriched=2,
                    failed_models=[],
                    print_summary=Mock(),
                )
            )
            mock_instance.close = Mock()
            mock_service.return_value = mock_instance

            result = runner.invoke(enrich, ["--models", "customers,orders"])

            mock_resolve.assert_called_once()
            assert mock_resolve.call_args[0][0] == ["customers", "orders"]

    def test_models_short_option(self):
        """Test that -m short option works."""
        runner = CliRunner()

        with patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names"
        ) as mock_resolve, patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            mock_resolve.return_value = ["models/customers.sql"]

            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.enrich = Mock(
                return_value=Mock(
                    status="success",
                    models_enriched=1,
                    failed_models=[],
                    print_summary=Mock(),
                )
            )
            mock_instance.close = Mock()
            mock_service.return_value = mock_instance

            result = runner.invoke(enrich, ["-m", "customers"])

            mock_resolve.assert_called_once()


class TestEnrichServiceWithModelFiles:
    """Tests for MetadataEnrichmentService with model_files."""

    def test_discover_models_uses_explicit_files(self):
        """Test that _discover_models returns explicit files when provided."""
        from snowflake_semantic_tools.services.enrich_metadata import MetadataEnrichmentService

        config = EnrichmentConfig(model_files=["models/customers.sql", "models/orders.sql"])
        service = MetadataEnrichmentService(config)

        result = service._discover_models()

        assert result == ["models/customers.sql", "models/orders.sql"]

    def test_discover_models_falls_back_to_target_path(self, tmp_path):
        """Test that _discover_models uses target_path when model_files not provided."""
        from snowflake_semantic_tools.services.enrich_metadata import MetadataEnrichmentService

        # Create a temp directory with SQL files
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "customers.sql").write_text("SELECT 1")
        (models_dir / "orders.sql").write_text("SELECT 2")

        config = EnrichmentConfig(target_path=str(models_dir))
        service = MetadataEnrichmentService(config)

        result = service._discover_models()

        # Should discover the SQL files
        assert len(result) == 2
        assert any("customers.sql" in f for f in result)
        assert any("orders.sql" in f for f in result)
