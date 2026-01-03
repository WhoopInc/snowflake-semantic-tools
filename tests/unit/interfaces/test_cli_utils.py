"""
Test CLI Utilities

Tests for shared CLI helper functions in interfaces/cli/utils.py
"""

import logging
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.infrastructure.dbt.exceptions import DbtProfileNotFoundError
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.interfaces.cli.utils import build_snowflake_config, setup_command

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def fixtures_path():
    """Return path to test fixtures."""
    return Path(__file__).parent.parent.parent / "fixtures" / "dbt_profiles"


@pytest.fixture
def temp_project_dir(fixtures_path):
    """Create a temporary project directory with dbt_project.yml and profiles.yml."""
    temp_dir = tempfile.mkdtemp()

    # Copy dbt_project.yml
    shutil.copy(
        fixtures_path / "valid_dbt_project.yml",
        Path(temp_dir) / "dbt_project.yml",
    )

    # Copy profiles.yml
    shutil.copy(
        fixtures_path / "valid_password_profile.yml",
        Path(temp_dir) / "profiles.yml",
    )

    yield Path(temp_dir)

    # Cleanup
    shutil.rmtree(temp_dir)


# =============================================================================
# Tests
# =============================================================================


class TestSetupCommand:
    """Test common command setup."""

    @patch("snowflake_semantic_tools.interfaces.cli.utils.setup_events")
    @patch("snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config")
    def test_calls_all_setup_functions(self, mock_validate, mock_events):
        """Test that setup_command calls all initialization functions."""
        setup_command(verbose=False, quiet=False, validate_config=True)

        # Verify setup functions were called
        mock_events.assert_called_once()
        mock_validate.assert_called_once()

    @patch("snowflake_semantic_tools.interfaces.cli.utils.setup_events")
    @patch("snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config")
    def test_skips_config_validation_when_requested(self, mock_validate, mock_events):
        """Test that config validation can be skipped."""
        setup_command(verbose=False, validate_config=False)

        # Verify validation was NOT called
        mock_validate.assert_not_called()

    @patch("snowflake_semantic_tools.interfaces.cli.utils.setup_events")
    @patch("snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config")
    @patch("logging.getLogger")
    def test_sets_debug_logging_when_verbose(self, mock_get_logger, mock_validate, mock_events):
        """Test that verbose mode sets DEBUG logging."""
        setup_command(verbose=True, validate_config=False)

        mock_logger = mock_get_logger.return_value
        mock_logger.setLevel.assert_called()
        # Check if DEBUG level was set (level 10)
        call_args = mock_logger.setLevel.call_args[0][0]
        assert call_args == logging.DEBUG

    @patch("snowflake_semantic_tools.interfaces.cli.utils.setup_events")
    @patch("snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config")
    @patch("logging.getLogger")
    def test_sets_error_logging_when_quiet(self, mock_get_logger, mock_validate, mock_events):
        """Test that quiet mode sets ERROR logging."""
        setup_command(quiet=True, validate_config=False)

        mock_logger = mock_get_logger.return_value
        mock_logger.setLevel.assert_called()
        call_args = mock_logger.setLevel.call_args[0][0]
        assert call_args == logging.ERROR


class TestBuildSnowflakeConfig:
    """Test Snowflake configuration builder using dbt profiles."""

    def test_builds_config_from_dbt_profile(self, temp_project_dir):
        """Test building config from dbt profiles.yml."""
        original_cwd = Path.cwd()
        try:
            os.chdir(temp_project_dir)

            config = build_snowflake_config(
                target="dev",
                database="TEST_DB",
                schema="TEST_SCHEMA",
            )

            assert isinstance(config, SnowflakeConfig)
            assert config.account == "test_account"
            assert config.user == "test_user"
            assert config.password == "test_password"
            assert config.database == "TEST_DB"
            assert config.schema == "TEST_SCHEMA"
            assert config.profile_name == "test_project"
            assert config.target_name == "dev"

        finally:
            os.chdir(original_cwd)

    def test_uses_default_target_when_not_specified(self, temp_project_dir):
        """Test that default target is used when not specified."""
        original_cwd = Path.cwd()
        try:
            os.chdir(temp_project_dir)

            config = build_snowflake_config(
                database="TEST_DB",
                schema="TEST_SCHEMA",
            )

            # Default target from profile is 'dev'
            assert config.target_name == "dev"

        finally:
            os.chdir(original_cwd)

    def test_database_override(self, temp_project_dir):
        """Test that database override works."""
        original_cwd = Path.cwd()
        try:
            os.chdir(temp_project_dir)

            config = build_snowflake_config(
                database="OVERRIDE_DB",
                schema="TEST_SCHEMA",
            )

            # Database should be overridden and uppercased
            assert config.database == "OVERRIDE_DB"

        finally:
            os.chdir(original_cwd)

    def test_schema_override(self, temp_project_dir):
        """Test that schema override works."""
        original_cwd = Path.cwd()
        try:
            os.chdir(temp_project_dir)

            config = build_snowflake_config(
                database="TEST_DB",
                schema="OVERRIDE_SCHEMA",
            )

            # Schema should be overridden and uppercased
            assert config.schema == "OVERRIDE_SCHEMA"

        finally:
            os.chdir(original_cwd)

    def test_raises_error_when_no_profiles_yml(self, tmp_path):
        """Test that appropriate error is raised when profiles.yml not found."""
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)

            # Create a minimal dbt_project.yml
            dbt_project = tmp_path / "dbt_project.yml"
            dbt_project.write_text("name: test\nprofile: test_profile\n")

            with pytest.raises(Exception) as exc_info:
                build_snowflake_config(
                    database="TEST_DB",
                    schema="TEST_SCHEMA",
                )

            # Should raise a click exception with helpful message
            assert "profiles.yml" in str(exc_info.value).lower()

        finally:
            os.chdir(original_cwd)

    def test_specific_target(self, temp_project_dir):
        """Test using a specific target."""
        original_cwd = Path.cwd()
        try:
            os.chdir(temp_project_dir)

            config = build_snowflake_config(
                target="prod",
                database="PROD_DB",
                schema="PROD_SCHEMA",
            )

            assert config.target_name == "prod"
            assert config.role == "PROD_ROLE"

        finally:
            os.chdir(original_cwd)


class TestBuildSnowflakeConfigDbtCloud:
    """Test build_snowflake_config behavior for dbt Cloud users."""

    @patch("snowflake_semantic_tools.interfaces.cli.utils.DbtClient")
    @patch("snowflake_semantic_tools.interfaces.cli.utils.SnowflakeConfig.from_dbt_profile")
    def test_shows_dbt_cloud_message_when_no_profile(self, mock_from_profile, mock_dbt_client):
        """Test that dbt Cloud users get appropriate error message."""
        from snowflake_semantic_tools.infrastructure.dbt import DbtType

        # Simulate no profiles.yml found
        mock_from_profile.side_effect = DbtProfileNotFoundError(["/fake/path"])

        # Simulate dbt Cloud CLI detected
        mock_client_instance = MagicMock()
        mock_client_instance.dbt_type = DbtType.CLOUD_CLI
        mock_dbt_client.return_value = mock_client_instance

        with pytest.raises(Exception) as exc_info:
            build_snowflake_config(database="DB", schema="SCHEMA")

        error_msg = str(exc_info.value)
        assert "dbt Cloud" in error_msg or "profiles.yml" in error_msg
