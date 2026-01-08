"""
Tests for file_utils module.

Tests the get_dbt_model_paths function that reads model paths from dbt_project.yml.
"""

import os
from pathlib import Path

import pytest
import yaml

from snowflake_semantic_tools.shared.config import Config
from snowflake_semantic_tools.shared.utils.file_utils import get_dbt_model_paths


class TestGetDbtModelPaths:
    """Test get_dbt_model_paths function."""

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Save and restore cwd for each test."""
        try:
            original_cwd = Path.cwd()
        except (FileNotFoundError, OSError):
            import tempfile

            temp_dir = tempfile.mkdtemp()
            os.chdir(temp_dir)
            original_cwd = Path(temp_dir)

        yield

        try:
            if original_cwd.exists():
                os.chdir(original_cwd)
        except (FileNotFoundError, OSError):
            pass
        Config._instance = None

    def test_reads_single_model_path(self, tmp_path):
        """Test reading single model-paths value from dbt_project.yml."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text(
            """
name: 'test_project'
profile: 'test'
model-paths: ["models"]
"""
        )

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        assert len(result) == 1
        assert result[0] == tmp_path / "models"

    def test_reads_multiple_model_paths(self, tmp_path):
        """Test reading multiple model-paths values from dbt_project.yml."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text(
            """
name: 'test_project'
profile: 'test'
model-paths: ["models", "marts", "staging"]
"""
        )

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        assert len(result) == 3
        assert tmp_path / "models" in result
        assert tmp_path / "marts" in result
        assert tmp_path / "staging" in result

    def test_custom_model_path(self, tmp_path):
        """Test reading custom model path from dbt_project.yml."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text(
            """
name: 'test_project'
profile: 'test'
model-paths: ["dbt_models"]
"""
        )

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        assert len(result) == 1
        assert result[0] == tmp_path / "dbt_models"

    def test_no_dbt_project_yml_fallback(self, tmp_path):
        """Test fallback to default ["models"] when dbt_project.yml doesn't exist."""
        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        assert len(result) == 1
        assert result[0] == tmp_path / "models"

    def test_dbt_project_without_model_paths(self, tmp_path):
        """Test fallback when dbt_project.yml exists but has no model-paths."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text(
            """
name: 'test_project'
profile: 'test'
"""
        )

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        assert len(result) == 1
        assert result[0] == tmp_path / "models"

    def test_invalid_yaml_fallback(self, tmp_path):
        """Test fallback when dbt_project.yml has invalid YAML."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text("invalid: yaml: syntax:")

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        # Should fall back to default
        assert len(result) == 1
        assert result[0] == tmp_path / "models"

    def test_empty_dbt_project_yml(self, tmp_path):
        """Test fallback when dbt_project.yml is empty."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text("")

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        # Should fall back to default
        assert len(result) == 1
        assert result[0] == tmp_path / "models"

    def test_returns_absolute_paths(self, tmp_path):
        """Test that returned paths are absolute."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text(
            """
name: 'test_project'
model-paths: ["models"]
"""
        )

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        assert len(result) == 1
        assert result[0].is_absolute()

    def test_nested_model_paths(self, tmp_path):
        """Test reading nested model paths from dbt_project.yml."""
        dbt_project = tmp_path / "dbt_project.yml"
        dbt_project.write_text(
            """
name: 'test_project'
profile: 'test'
model-paths: ["src/models", "src/marts"]
"""
        )

        os.chdir(tmp_path)
        result = get_dbt_model_paths()

        assert len(result) == 2
        assert tmp_path / "src/models" in result
        assert tmp_path / "src/marts" in result
