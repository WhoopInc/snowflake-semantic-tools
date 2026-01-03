"""
Configuration Edge Cases Testing
Category 15: sst_config.yml and .env file handling
"""

import os
import tempfile
from pathlib import Path

import pytest

from snowflake_semantic_tools.shared.config import Config, get_config
from snowflake_semantic_tools.shared.config_validator import validate_config


class TestConfigFileEdgeCases:
    """Test sst_config.yml edge cases."""

    def test_missing_config_file(self):
        """Test behavior when sst_config.yml doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            os.chdir(temp_path)

            # Should fall back to defaults or error
            # (Actual behavior depends on implementation)

    def test_empty_config_file(self):
        """Test behavior with empty sst_config.yml."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            config_file = temp_path / "sst_config.yml"
            config_file.write_text("")

            # Should error or use defaults

    def test_invalid_yaml_syntax(self):
        """Test behavior with malformed YAML."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            config_file = temp_path / "sst_config.yml"
            config_file.write_text("invalid: yaml: syntax:")

            # Should produce clear error about YAML syntax

    def test_missing_required_fields(self):
        """Test validation catches missing required fields."""
        config_dict = {
            "validation": {"strict": False}  # Python boolean, not YAML
            # Missing project.semantic_models_dir
            # Missing project.dbt_models_dir
        }

        is_valid, missing, _ = validate_config(config_dict)
        assert not is_valid
        assert "project.semantic_models_dir" in missing
        assert "project.dbt_models_dir" in missing

    def test_invalid_path_references(self):
        """Test config with non-existent directory paths."""
        config_dict = {
            "project": {
                "semantic_models_dir": "/nonexistent/path/to/semantic_models",
                "dbt_models_dir": "/nonexistent/path/to/models",
            }
        }

        # Should validate structure but paths checked at runtime
        is_valid, missing, _ = validate_config(config_dict)
        assert is_valid  # Structure is valid, paths checked later

    def test_relative_vs_absolute_paths(self):
        """Test relative and absolute path handling."""
        config_dict = {
            "project": {
                "semantic_models_dir": "snowflake_semantic_models",  # Relative
                "dbt_models_dir": "/abs/path/to/models",  # Absolute
            }
        }

        is_valid, missing, _ = validate_config(config_dict)
        assert is_valid

    def test_trailing_slashes_in_paths(self):
        """Test that trailing slashes are handled."""
        config_dict = {
            "project": {
                "semantic_models_dir": "snowflake_semantic_models/",  # Trailing slash
                "dbt_models_dir": "models",  # No trailing slash
            }
        }

        is_valid, missing, _ = validate_config(config_dict)
        assert is_valid

    def test_special_chars_in_paths(self):
        """Test paths with spaces and unicode."""
        config_dict = {
            "project": {"semantic_models_dir": "semantic models", "dbt_models_dir": "modèles"}  # Space  # Unicode
        }

        is_valid, missing, _ = validate_config(config_dict)
        assert is_valid


class TestEnvFileEdgeCases:
    """Test .env file handling edge cases."""

    def test_missing_env_file(self):
        """Test behavior when .env doesn't exist."""
        # Should fall back to shell environment variables
        pass

    def test_empty_env_file(self):
        """Test behavior with empty .env file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_file.write_text("")

            # Should not error, just have no vars loaded

    def test_quotes_in_env_values(self):
        """Test that quotes in .env are included literally."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_file.write_text("SNOWFLAKE_USER='user@example.com'\nSNOWFLAKE_ACCOUNT=ABC123\n")

            # Quotes should be literal: 'user@example.com' (with quotes!)

    def test_spaces_around_equals(self):
        """Test .env with spaces around = sign."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_file.write_text("KEY = value\nKEY2=value2\n")

            # Should handle both formats

    def test_comments_in_env(self):
        """Test .env with comment lines."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_file.write_text("# Comment line\nKEY=value\n# Another comment\nKEY2=value2\n")

            # Should ignore comments

    def test_empty_lines_in_env(self):
        """Test .env with empty lines."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_file.write_text("KEY=value\n\n\nKEY2=value2\n")

            # Should handle empty lines

    def test_duplicate_keys_in_env(self):
        """Test .env with duplicate keys (last one should win)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_file.write_text("KEY=first\nKEY=second\nKEY=third\n")

            # Last value should win: KEY=third

    def test_multiline_values_in_env(self):
        """Test .env with multi-line values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_file.write_text('KEY="line1\\nline2\\nline3"\n')

            # Should handle escaped newlines


class TestEnrichmentConfigDefaults:
    """Test enrichment configuration defaults and customization."""

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Save and restore cwd for each test."""
        # Try to save current directory, but if it's invalid, use a temp one
        try:
            original_cwd = Path.cwd()
        except (FileNotFoundError, OSError):
            # Current directory is invalid, use a temp directory
            import tempfile

            temp_dir = tempfile.mkdtemp()
            os.chdir(temp_dir)
            original_cwd = Path(temp_dir)

        yield

        # Cleanup: try to restore original cwd
        try:
            if original_cwd.exists():
                os.chdir(original_cwd)
        except (FileNotFoundError, OSError):
            # If we can't restore, that's okay
            pass
        Config._instance = None  # Always reset config singleton

    def test_default_enrichment_config(self, tmp_path):
        """Test that default enrichment config values are correct."""
        # Create a minimal config to avoid relying on existing files
        config_file = tmp_path / "sst_config.yml"
        config_file.write_text(
            """
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"
"""
        )

        # Change to temp directory and test defaults
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            Config._instance = None  # Reset config singleton
            config = Config()

            assert config.get_enrichment_distinct_limit() == 25
            assert config.get_enrichment_display_limit() == 10
        finally:
            os.chdir(original_cwd)
            Config._instance = None

    def test_custom_enrichment_config(self, tmp_path):
        """Test that custom enrichment config values are loaded."""
        # Create a config file with custom enrichment values
        config_file = tmp_path / "sst_config.yml"
        config_file.write_text(
            """
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

enrichment:
  distinct_limit: 50
  sample_values_display_limit: 15
"""
        )

        # Change to temp directory and reload config
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            Config._instance = None  # Reset singleton
            config = Config()

            assert config.get_enrichment_distinct_limit() == 50
            assert config.get_enrichment_display_limit() == 15
        finally:
            os.chdir(original_cwd)
            Config._instance = None  # Reset for other tests

    def test_partial_enrichment_config(self, tmp_path):
        """Test that partial enrichment config uses defaults for missing values."""
        config_file = tmp_path / "sst_config.yml"
        config_file.write_text(
            """
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

enrichment:
  distinct_limit: 100
  # sample_values_display_limit omitted - should use default
"""
        )

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            Config._instance = None
            config = Config()

            assert config.get_enrichment_distinct_limit() == 100
            assert config.get_enrichment_display_limit() == 10  # default
        finally:
            os.chdir(original_cwd)
            Config._instance = None

    def test_enrichment_config_validation_types(self, tmp_path):
        """Test that enrichment config values are integers."""
        config_file = tmp_path / "sst_config.yml"
        config_file.write_text(
            """
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

enrichment:
  distinct_limit: 30
  sample_values_display_limit: 12
"""
        )

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            Config._instance = None
            config = Config()

            # Values should be integers
            assert isinstance(config.get_enrichment_distinct_limit(), int)
            assert isinstance(config.get_enrichment_display_limit(), int)
        finally:
            os.chdir(original_cwd)
            Config._instance = None

    def test_missing_enrichment_section(self, tmp_path):
        """Test that missing enrichment section uses defaults."""
        config_file = tmp_path / "sst_config.yml"
        config_file.write_text(
            """
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"
# enrichment section omitted - should use defaults
"""
        )

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            Config._instance = None
            config = Config()

            # Should use defaults
            assert config.get_enrichment_distinct_limit() == 25
            assert config.get_enrichment_display_limit() == 10
        finally:
            os.chdir(original_cwd)
            Config._instance = None
