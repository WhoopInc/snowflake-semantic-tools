"""
Tests for file_utils module.

Tests the get_dbt_model_paths function that reads model paths from dbt_project.yml.
Tests wildcard pattern expansion functionality.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from snowflake_semantic_tools.shared.config import Config
from snowflake_semantic_tools.shared.utils.file_utils import (
    _convert_to_sql_files,
    expand_path_pattern,
    get_dbt_model_paths,
    resolve_wildcard_path_for_enrich,
)


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


class TestExpandPathPattern:
    """Test expand_path_pattern function for wildcard expansion."""

    def test_no_wildcards_existing_file(self, tmp_path):
        """Test that non-wildcard path to existing file returns that file."""
        test_file = tmp_path / "test.yml"
        test_file.write_text("test")
        os.chdir(tmp_path)

        result = expand_path_pattern("test.yml")

        assert len(result) == 1
        assert result[0] == test_file.resolve()
        assert result[0].is_absolute()

    def test_no_wildcards_nonexistent_file(self, tmp_path):
        """Test that non-wildcard path to non-existent file returns empty list."""
        os.chdir(tmp_path)

        result = expand_path_pattern("nonexistent.yml")

        assert result == []

    def test_no_wildcards_existing_directory(self, tmp_path):
        """Test that non-wildcard path to existing directory returns that directory."""
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        os.chdir(tmp_path)

        result = expand_path_pattern("test_dir")

        assert len(result) == 1
        assert result[0] == test_dir.resolve()

    def test_wildcard_prefix_matching(self, tmp_path):
        """Test wildcard pattern matching files with shared prefix."""
        # Create test files
        (tmp_path / "profound_answers.yml").write_text("test")
        (tmp_path / "profound_citations.yml").write_text("test")
        (tmp_path / "profound_sentiment.yml").write_text("test")
        (tmp_path / "other_file.yml").write_text("test")
        os.chdir(tmp_path)

        result = expand_path_pattern("profound_*")

        assert len(result) == 3
        file_names = [p.name for p in result]
        assert "profound_answers.yml" in file_names
        assert "profound_citations.yml" in file_names
        assert "profound_sentiment.yml" in file_names
        assert "other_file.yml" not in file_names

    def test_wildcard_with_extension(self, tmp_path):
        """Test wildcard pattern with explicit extension."""
        (tmp_path / "test1.yml").write_text("test")
        (tmp_path / "test2.yml").write_text("test")
        (tmp_path / "test1.sql").write_text("test")
        os.chdir(tmp_path)

        result = expand_path_pattern("test*.yml")

        assert len(result) == 2
        file_names = [p.name for p in result]
        assert "test1.yml" in file_names
        assert "test2.yml" in file_names
        assert "test1.sql" not in file_names

    def test_wildcard_subdirectory(self, tmp_path):
        """Test wildcard pattern matching files in subdirectory."""
        subdir = tmp_path / "models" / "analytics" / "marketing" / "_intermediate"
        subdir.mkdir(parents=True)
        (subdir / "file1.yml").write_text("test")
        (subdir / "file2.yml").write_text("test")
        os.chdir(tmp_path)

        result = expand_path_pattern("models/analytics/marketing/_intermediate/*")

        assert len(result) == 2
        file_names = [p.name for p in result]
        assert "file1.yml" in file_names
        assert "file2.yml" in file_names

    def test_wildcard_matches_multiple_extensions(self, tmp_path):
        """Test that wildcard without extension matches files with common extensions and prefix."""
        (tmp_path / "model1.yml").write_text("test")
        (tmp_path / "model2.yaml").write_text("test")
        (tmp_path / "model3.sql").write_text("test")
        (tmp_path / "model4.txt").write_text("test")
        os.chdir(tmp_path)

        result = expand_path_pattern("model*")

        # Should match .yml, .yaml, .sql files, and also files with same prefix
        assert len(result) >= 3
        file_names = [p.name for p in result]
        assert "model1.yml" in file_names
        assert "model2.yaml" in file_names
        assert "model3.sql" in file_names
        # The function also matches by prefix, so .txt will be included
        assert "model4.txt" in file_names

    def test_wildcard_nonexistent_base_directory(self, tmp_path):
        """Test that wildcard pattern with non-existent base directory returns empty list."""
        os.chdir(tmp_path)

        result = expand_path_pattern("nonexistent_dir/*")

        assert result == []

    def test_wildcard_no_matches(self, tmp_path):
        """Test that wildcard pattern with no matches returns empty list."""
        os.chdir(tmp_path)

        result = expand_path_pattern("nonexistent_*")

        assert result == []

    def test_wildcard_returns_absolute_paths(self, tmp_path):
        """Test that expanded paths are absolute."""
        (tmp_path / "test.yml").write_text("test")
        os.chdir(tmp_path)

        result = expand_path_pattern("test.yml")

        assert len(result) == 1
        assert result[0].is_absolute()

    def test_wildcard_deduplicates_matches(self, tmp_path):
        """Test that duplicate matches are removed."""
        (tmp_path / "test.yml").write_text("test")
        os.chdir(tmp_path)

        # Create a symlink or duplicate scenario if possible
        result = expand_path_pattern("test*")

        # Should only return each file once
        assert len(result) == 1

    def test_wildcard_no_duplicate_when_glob_matches_files(self, tmp_path):
        """Test that prefix search doesn't duplicate files already matched by glob."""
        # Create files that would match both glob pattern and prefix search
        (tmp_path / "prefix_file.txt").write_text("test")
        (tmp_path / "prefix_file.yml").write_text("test")
        (tmp_path / "prefix_file.sql").write_text("test")
        os.chdir(tmp_path)

        # Pattern without extension - glob might match .txt, prefix search would match all
        result = expand_path_pattern("prefix_*")

        # Should not have duplicates even if both glob and prefix search match same files
        result_names = [p.name for p in result]
        assert len(result_names) == len(set(result_names)), "Found duplicate files in results"
        # Should find the .yml, .yaml, and .sql files (common extensions)
        assert "prefix_file.yml" in result_names
        assert "prefix_file.sql" in result_names


class TestConvertToSqlFiles:
    """Test _convert_to_sql_files function."""

    def test_converts_yaml_to_sql(self, tmp_path):
        """Test that YAML files are converted to corresponding SQL files."""
        yaml_file = tmp_path / "model.yml"
        sql_file = tmp_path / "model.sql"
        yaml_file.write_text("test")
        sql_file.write_text("SELECT 1")
        seen_stems = set()

        result = _convert_to_sql_files([yaml_file], seen_stems)

        assert len(result) == 1
        assert result[0] == str(sql_file)
        assert "model" in seen_stems

    def test_keeps_sql_files(self, tmp_path):
        """Test that SQL files are kept as-is."""
        sql_file = tmp_path / "model.sql"
        sql_file.write_text("SELECT 1")
        seen_stems = set()

        result = _convert_to_sql_files([sql_file], seen_stems)

        assert len(result) == 1
        assert result[0] == str(sql_file)
        assert "model" in seen_stems

    def test_deduplicates_by_stem(self, tmp_path):
        """Test that files with same stem are deduplicated."""
        yaml_file = tmp_path / "model.yml"
        sql_file = tmp_path / "model.sql"
        yaml_file.write_text("test")
        sql_file.write_text("SELECT 1")
        seen_stems = set()

        # Add both YAML and SQL for same model
        result = _convert_to_sql_files([yaml_file, sql_file], seen_stems)

        # Should only return SQL file once
        assert len(result) == 1
        assert result[0] == str(sql_file)

    def test_skips_missing_sql_file(self, tmp_path):
        """Test that YAML without corresponding SQL is skipped."""
        yaml_file = tmp_path / "model.yml"
        yaml_file.write_text("test")
        seen_stems = set()

        result = _convert_to_sql_files([yaml_file], seen_stems)

        assert len(result) == 0
        assert "model" not in seen_stems

    def test_handles_multiple_files(self, tmp_path):
        """Test conversion of multiple files."""
        (tmp_path / "model1.yml").write_text("test")
        (tmp_path / "model1.sql").write_text("SELECT 1")
        (tmp_path / "model2.sql").write_text("SELECT 2")
        (tmp_path / "model3.yml").write_text("test")
        seen_stems = set()

        result = _convert_to_sql_files(
            [
                tmp_path / "model1.yml",
                tmp_path / "model2.sql",
                tmp_path / "model3.yml",
            ],
            seen_stems,
        )

        assert len(result) == 2
        assert str(tmp_path / "model1.sql") in result
        assert str(tmp_path / "model2.sql") in result
        assert str(tmp_path / "model3.sql") not in result  # No SQL file exists


class TestResolveWildcardPathForEnrich:
    """Test resolve_wildcard_path_for_enrich function."""

    def test_single_file_returns_model_files(self, tmp_path):
        """Test that single file pattern returns model_files."""
        yaml_file = tmp_path / "model.yml"
        sql_file = tmp_path / "model.sql"
        yaml_file.write_text("test")
        sql_file.write_text("SELECT 1")
        os.chdir(tmp_path)

        mock_output = MagicMock()
        target_path, model_files = resolve_wildcard_path_for_enrich("model.yml", mock_output)

        assert target_path is None
        assert model_files is not None
        assert len(model_files) == 1
        assert str(sql_file) in model_files

    def test_multiple_files_returns_model_files(self, tmp_path):
        """Test that multiple file pattern returns model_files list."""
        (tmp_path / "model1.yml").write_text("test")
        (tmp_path / "model1.sql").write_text("SELECT 1")
        (tmp_path / "model2.yml").write_text("test")
        (tmp_path / "model2.sql").write_text("SELECT 2")
        os.chdir(tmp_path)

        mock_output = MagicMock()
        target_path, model_files = resolve_wildcard_path_for_enrich("model*", mock_output)

        assert target_path is None
        assert model_files is not None
        assert len(model_files) == 2

    def test_single_directory_returns_target_path(self, tmp_path):
        """Test that single directory pattern returns target_path."""
        test_dir = tmp_path / "models"
        test_dir.mkdir()
        os.chdir(tmp_path)

        mock_output = MagicMock()
        target_path, model_files = resolve_wildcard_path_for_enrich("models", mock_output)

        assert target_path is not None
        assert model_files is None
        assert "models" in target_path

    def test_no_matches_raises_error(self, tmp_path):
        """Test that pattern with no matches raises ValueError."""
        os.chdir(tmp_path)

        mock_output = MagicMock()
        with pytest.raises(ValueError, match="No files found matching pattern"):
            resolve_wildcard_path_for_enrich("nonexistent_*", mock_output)

    def test_wildcard_prefix_matching(self, tmp_path):
        """Test wildcard prefix pattern matching."""
        (tmp_path / "profound_answers.yml").write_text("test")
        (tmp_path / "profound_answers.sql").write_text("SELECT 1")
        (tmp_path / "profound_citations.yml").write_text("test")
        (tmp_path / "profound_citations.sql").write_text("SELECT 2")
        os.chdir(tmp_path)

        mock_output = MagicMock()
        target_path, model_files = resolve_wildcard_path_for_enrich("profound_*", mock_output)

        assert target_path is None
        assert model_files is not None
        assert len(model_files) == 2

    def test_directory_with_files_returns_model_files(self, tmp_path):
        """Test that directory pattern with files returns model_files."""
        test_dir = tmp_path / "models" / "analytics"
        test_dir.mkdir(parents=True)
        (test_dir / "model1.yml").write_text("test")
        (test_dir / "model1.sql").write_text("SELECT 1")
        (test_dir / "model2.yml").write_text("test")
        (test_dir / "model2.sql").write_text("SELECT 2")
        os.chdir(tmp_path)

        mock_output = MagicMock()
        target_path, model_files = resolve_wildcard_path_for_enrich("models/analytics/*", mock_output)

        assert target_path is None
        assert model_files is not None
        assert len(model_files) == 2
