"""
Tests for Configuration Utilities

Tests the reusable config helper functions that provide clean,
testable access to SST configuration.
"""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from snowflake_semantic_tools.shared.config_utils import (
    get_enrichment_limits,
    get_exclusion_patterns,
    get_exclusion_summary,
    get_project_paths,
    get_synonym_config,
    is_strict_mode,
)


class TestGetExclusionPatterns:
    """Test exclusion pattern retrieval and merging."""

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_no_exclusions(self, mock_get_config):
        """Test when no exclusions configured."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = None
        mock_get_config.return_value = mock_config

        result = get_exclusion_patterns()
        assert result is None

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_config_exclusions_only(self, mock_get_config):
        """Test with only config file exclusions."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = ["models/amplitude/*", "models/analytics_mart/*"]
        mock_get_config.return_value = mock_config

        result = get_exclusion_patterns()
        assert result == ["models/amplitude/*", "models/analytics_mart/*"]

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_cli_exclusions_only(self, mock_get_config):
        """Test with only CLI exclusions."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = None
        mock_get_config.return_value = mock_config

        result = get_exclusion_patterns(cli_exclude="temp,backup")
        assert result == ["temp", "backup"]

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_merged_exclusions(self, mock_get_config):
        """Test merging config and CLI exclusions."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = ["models/amplitude/*"]
        mock_get_config.return_value = mock_config

        result = get_exclusion_patterns(cli_exclude="temp,experimental")
        assert result == ["models/amplitude/*", "temp", "experimental"]

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_deduplication(self, mock_get_config):
        """Test that duplicates are removed."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = ["temp", "backup"]
        mock_get_config.return_value = mock_config

        result = get_exclusion_patterns(cli_exclude="temp,staging")
        assert result == ["temp", "backup", "staging"]  # 'temp' only appears once

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_order_preservation(self, mock_get_config):
        """Test that order is preserved (config first, then CLI)."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = ["a", "b"]
        mock_get_config.return_value = mock_config

        result = get_exclusion_patterns(cli_exclude="c,d")
        assert result == ["a", "b", "c", "d"]

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_whitespace_handling(self, mock_get_config):
        """Test that whitespace in CLI input is trimmed."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = []
        mock_get_config.return_value = mock_config

        result = get_exclusion_patterns(cli_exclude=" temp , backup , staging ")
        assert result == ["temp", "backup", "staging"]


class TestGetEnrichmentLimits:
    """Test enrichment limit retrieval."""

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_default_limits(self, mock_get_config):
        """Test default limits when not configured."""
        mock_config = Mock()
        mock_config.get.return_value = {}
        mock_get_config.return_value = mock_config

        result = get_enrichment_limits()
        assert result["distinct_limit"] == 25
        assert result["sample_values_display_limit"] == 10

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_custom_limits(self, mock_get_config):
        """Test custom limits from config."""
        mock_config = Mock()
        mock_config.get.return_value = {"distinct_limit": 50, "sample_values_display_limit": 20}
        mock_get_config.return_value = mock_config

        result = get_enrichment_limits()
        assert result["distinct_limit"] == 50
        assert result["sample_values_display_limit"] == 20


class TestGetSynonymConfig:
    """Test synonym configuration retrieval."""

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_default_synonym_config(self, mock_get_config):
        """Test default synonym config."""
        mock_config = Mock()
        mock_config.get.return_value = {}
        mock_get_config.return_value = mock_config

        result = get_synonym_config()
        assert result["model"] == "mistral-large2"  # Universal default
        assert result["max_count"] == 4

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_custom_synonym_config(self, mock_get_config):
        """Test custom synonym config."""
        mock_config = Mock()
        mock_config.get.return_value = {"synonym_model": "claude-4-sonnet", "synonym_max_count": 6}
        mock_get_config.return_value = mock_config

        result = get_synonym_config()
        assert result["model"] == "claude-4-sonnet"
        assert result["max_count"] == 6


class TestGetProjectPaths:
    """Test project path resolution."""

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    @patch("snowflake_semantic_tools.shared.config_utils.Path.cwd")
    def test_default_paths(self, mock_cwd, mock_get_config):
        """Test default project paths."""
        mock_cwd.return_value = Path("/project")
        mock_config = Mock()
        mock_config.get.return_value = {}
        mock_get_config.return_value = mock_config

        result = get_project_paths()
        assert result["dbt_models_dir"] == Path("/project/models")
        assert result["semantic_models_dir"] == Path("/project/snowflake_semantic_models")
        assert result["manifest_path"] == Path("/project/target/manifest.json")

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    @patch("snowflake_semantic_tools.shared.config_utils.Path.cwd")
    def test_custom_paths(self, mock_cwd, mock_get_config):
        """Test custom project paths from config."""
        mock_cwd.return_value = Path("/project")
        mock_config = Mock()
        mock_config.get.return_value = {
            "dbt_models_dir": "my_models",
            "semantic_models_dir": "my_semantics",
            "manifest_path": "custom/manifest.json",
        }
        mock_get_config.return_value = mock_config

        result = get_project_paths()
        assert result["dbt_models_dir"] == Path("/project/my_models")
        assert result["semantic_models_dir"] == Path("/project/my_semantics")
        assert result["manifest_path"] == Path("/project/custom/manifest.json")


class TestIsStrictMode:
    """Test strict mode detection."""

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_strict_mode_false_default(self, mock_get_config):
        """Test strict mode defaults to false."""
        mock_config = Mock()
        mock_config.get.return_value = {}
        mock_get_config.return_value = mock_config

        assert is_strict_mode() is False

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_strict_mode_enabled(self, mock_get_config):
        """Test strict mode when enabled in config."""
        mock_config = Mock()
        mock_config.get.return_value = {"strict": True}
        mock_get_config.return_value = mock_config

        assert is_strict_mode() is True


class TestGetExclusionSummary:
    """Test exclusion summary generation."""

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_summary_with_both(self, mock_get_config):
        """Test summary with both config and CLI exclusions."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = ["models/amplitude/*"]
        mock_get_config.return_value = mock_config

        result = get_exclusion_summary(cli_exclude="temp,backup")

        assert result["config_patterns"] == ["models/amplitude/*"]
        assert result["cli_patterns"] == ["temp", "backup"]
        assert result["total_patterns"] == ["models/amplitude/*", "temp", "backup"]
        assert result["has_exclusions"] is True
        assert result["total_count"] == 3

    @patch("snowflake_semantic_tools.shared.config_utils.get_config")
    def test_summary_no_exclusions(self, mock_get_config):
        """Test summary with no exclusions."""
        mock_config = Mock()
        mock_config.get_exclude_dirs.return_value = None
        mock_get_config.return_value = mock_config

        result = get_exclusion_summary()

        assert result["config_patterns"] == []
        assert result["cli_patterns"] == []
        assert result["total_patterns"] == []
        assert result["has_exclusions"] is False
        assert result["total_count"] == 0
