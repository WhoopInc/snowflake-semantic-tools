"""
Tests for CLI defer module.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest

from snowflake_semantic_tools.infrastructure.dbt import DbtType
from snowflake_semantic_tools.interfaces.cli.defer import (
    DeferConfig,
    display_defer_info,
    get_defer_summary,
    resolve_defer_config,
    resolve_defer_manifest,
    validate_dbt_cloud_cli_compatibility,
)


class TestDeferConfig:
    """Tests for DeferConfig dataclass."""

    def test_defer_config_defaults(self):
        """Test DeferConfig with default values."""
        config = DeferConfig(enabled=False)
        assert config.enabled is False
        assert config.target is None
        assert config.state_path is None
        assert config.manifest_path is None
        assert config.only_modified is False
        assert config.source == "none"

    def test_defer_config_enabled(self):
        """Test DeferConfig with enabled defer."""
        config = DeferConfig(
            enabled=True,
            target="prod",
            state_path=Path("/path/to/state"),
            manifest_path=Path("/path/to/manifest.json"),
            only_modified=True,
            source="cli",
        )
        assert config.enabled is True
        assert config.target == "prod"
        assert config.only_modified is True
        assert config.source == "cli"

    def test_defer_config_only_modified_requires_enabled(self):
        """Test that only_modified raises error when defer is disabled."""
        with pytest.raises(ValueError, match="only_modified requires defer to be enabled"):
            DeferConfig(enabled=False, only_modified=True)


class TestValidateDbtCloudCliCompatibility:
    """Tests for validate_dbt_cloud_cli_compatibility function."""

    def test_dbt_core_no_validation_errors(self):
        """Test that dbt Core passes validation."""
        # Should not raise any errors
        validate_dbt_cloud_cli_compatibility(
            dbt_type=DbtType.CORE,
            defer_target="prod",
            auto_compile=True,  # Would fail for Cloud CLI
        )

    def test_dbt_cloud_cli_auto_compile_error(self):
        """Test that auto_compile raises error for dbt Cloud CLI."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_dbt_cloud_cli_compatibility(
                dbt_type=DbtType.CLOUD_CLI,
                auto_compile=True,
            )
        assert "auto_compile is not supported with dbt Cloud CLI" in str(exc_info.value)

    def test_dbt_cloud_cli_defer_without_state_logs_warning(self):
        """Test that defer_target without state_path logs warning for Cloud CLI."""
        # Should not raise, just log a warning
        validate_dbt_cloud_cli_compatibility(
            dbt_type=DbtType.CLOUD_CLI,
            defer_target="prod",
            auto_compile=False,
            state_path=None,
        )

    def test_dbt_cloud_cli_with_state_path_passes(self):
        """Test that Cloud CLI with explicit state_path passes."""
        validate_dbt_cloud_cli_compatibility(
            dbt_type=DbtType.CLOUD_CLI,
            defer_target="prod",
            auto_compile=False,
            state_path=Path("/path/to/state"),
        )


class TestResolveDeferManifest:
    """Tests for resolve_defer_manifest function."""

    def test_explicit_state_path_found(self, tmp_path):
        """Test that explicit --state path works."""
        # Create state directory with manifest
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        manifest_file = state_dir / "manifest.json"
        manifest_file.write_text('{"nodes": {}}')

        # Mock DbtClient
        with patch("snowflake_semantic_tools.interfaces.cli.defer.DbtClient") as mock_client:
            mock_client.return_value.dbt_type = DbtType.CORE

            result = resolve_defer_manifest(
                defer_target="prod",
                state_path=state_dir,
                project_dir=tmp_path,
            )

        assert result == manifest_file

    def test_explicit_state_path_not_found(self, tmp_path):
        """Test that missing manifest in --state raises error."""
        state_dir = tmp_path / "empty_state"
        state_dir.mkdir()

        with patch("snowflake_semantic_tools.interfaces.cli.defer.DbtClient") as mock_client:
            mock_client.return_value.dbt_type = DbtType.CORE

            with pytest.raises(click.ClickException) as exc_info:
                resolve_defer_manifest(
                    defer_target="prod",
                    state_path=state_dir,
                    project_dir=tmp_path,
                )

        assert "Manifest not found at" in str(exc_info.value)

    def test_auto_detect_manifest(self, tmp_path):
        """Test auto-detection of manifest in standard locations."""
        # Create target_prod/manifest.json
        target_dir = tmp_path / "target_prod"
        target_dir.mkdir()
        manifest_file = target_dir / "manifest.json"
        manifest_file.write_text('{"nodes": {}}')

        with patch("snowflake_semantic_tools.interfaces.cli.defer.DbtClient") as mock_client:
            mock_client.return_value.dbt_type = DbtType.CORE

            result = resolve_defer_manifest(
                defer_target="prod",
                project_dir=tmp_path,
            )

        assert result == manifest_file

    def test_dbt_core_no_manifest_suggests_compile(self, tmp_path):
        """Test that dbt Core suggests compile when manifest not found."""
        with patch("snowflake_semantic_tools.interfaces.cli.defer.DbtClient") as mock_client:
            mock_client.return_value.dbt_type = DbtType.CORE

            with pytest.raises(click.ClickException) as exc_info:
                resolve_defer_manifest(
                    defer_target="prod",
                    project_dir=tmp_path,
                )

        error_msg = str(exc_info.value)
        assert "dbt compile --target prod" in error_msg
        assert "Since you're using dbt Core" in error_msg

    def test_dbt_cloud_cli_no_manifest_suggests_download(self, tmp_path):
        """Test that dbt Cloud CLI suggests artifact download when manifest not found."""
        with patch("snowflake_semantic_tools.interfaces.cli.defer.DbtClient") as mock_client:
            mock_client.return_value.dbt_type = DbtType.CLOUD_CLI

            with pytest.raises(click.ClickException) as exc_info:
                resolve_defer_manifest(
                    defer_target="prod",
                    project_dir=tmp_path,
                )

        error_msg = str(exc_info.value)
        assert "dbt Cloud CLI" in error_msg
        assert "Download artifacts from dbt Cloud" in error_msg


class TestResolveDeferConfig:
    """Tests for resolve_defer_config function."""

    def test_no_defer_flag_disables(self, tmp_path):
        """Test that --no-defer disables defer."""
        config = resolve_defer_config(
            defer_target="prod",
            no_defer=True,
            project_dir=tmp_path,
        )
        assert config.enabled is False
        assert config.source == "cli"

    def test_only_modified_without_defer_target_errors(self, tmp_path):
        """Test that --only-modified without defer target raises error."""
        with pytest.raises(click.ClickException) as exc_info:
            resolve_defer_config(
                only_modified=True,
                project_dir=tmp_path,
            )
        assert "only-modified requires --defer-target" in str(exc_info.value)

    def test_no_defer_target_returns_disabled(self, tmp_path):
        """Test that no defer target returns disabled config."""
        config = resolve_defer_config(project_dir=tmp_path)
        assert config.enabled is False
        assert config.source == "none"


class TestDisplayDeferInfo:
    """Tests for display_defer_info function."""

    def test_display_disabled_does_nothing(self):
        """Test that disabled config doesn't display anything."""
        mock_output = MagicMock()
        config = DeferConfig(enabled=False)

        display_defer_info(mock_output, config)

        mock_output.info.assert_not_called()

    def test_display_enabled_shows_info(self):
        """Test that enabled config displays info."""
        mock_output = MagicMock()
        config = DeferConfig(
            enabled=True,
            target="prod",
            manifest_path=Path("/path/to/manifest.json"),
            only_modified=True,
            source="cli",
        )

        display_defer_info(mock_output, config)

        # Should have called info for each piece of info
        assert mock_output.info.call_count >= 1


class TestGetDeferSummary:
    """Tests for get_defer_summary function."""

    def test_summary_contains_all_fields(self):
        """Test that summary contains all expected fields."""
        config = DeferConfig(
            enabled=True,
            target="prod",
            state_path=Path("/path/to/state"),
            manifest_path=Path("/path/to/manifest.json"),
            only_modified=True,
            source="cli",
        )

        summary = get_defer_summary(config)

        assert summary["enabled"] is True
        assert summary["target"] == "prod"
        assert summary["state_path"] == "/path/to/state"
        assert summary["manifest_path"] == "/path/to/manifest.json"
        assert summary["only_modified"] is True
        assert summary["source"] == "cli"

    def test_summary_handles_none_paths(self):
        """Test that summary handles None paths."""
        config = DeferConfig(enabled=False)

        summary = get_defer_summary(config)

        assert summary["state_path"] is None
        assert summary["manifest_path"] is None
