"""
Unit tests for YAML formatting service.
"""

from pathlib import Path

import pytest
from ruamel.yaml import YAML

from snowflake_semantic_tools.services.format_yaml import FormattingConfig, YAMLFormattingService


@pytest.fixture
def formatter():
    """Create a formatter instance."""
    config = FormattingConfig(dry_run=False, check_only=False)
    return YAMLFormattingService(config)


@pytest.fixture
def yaml_handler():
    """Create a YAML handler."""
    yaml = YAML()
    yaml.preserve_quotes = True
    return yaml


class TestFieldOrdering:
    """Test field ordering functionality."""

    def test_model_field_ordering(self, formatter):
        """Test that model fields are reordered correctly."""
        model = {"columns": [], "name": "test_model", "config": {}, "description": "Test model", "meta": {}}

        formatter._format_model(model)

        # Check order
        keys = list(model.keys())
        assert keys == ["name", "description", "meta", "config", "columns"]

    def test_column_field_ordering(self, formatter):
        """Test that column fields are reordered correctly."""
        column = {"meta": {}, "description": "Test column", "data_tests": [], "name": "test_col"}

        formatter._format_column(column)

        # Check order
        keys = list(column.keys())
        assert keys == ["name", "description", "data_tests", "meta"]

    def test_sst_field_ordering(self, formatter):
        """Test that SST metadata fields are reordered correctly."""
        column = {
            "name": "test_col",
            "meta": {
                "sst": {
                    "is_enum": True,
                    "sample_values": ["a", "b"],
                    "column_type": "dimension",
                    "synonyms": [],
                    "data_type": "text",
                }
            },
        }

        formatter._format_column(column)

        # Check sst order
        sst_keys = list(column["meta"]["sst"].keys())
        assert sst_keys == ["column_type", "data_type", "synonyms", "sample_values", "is_enum"]


class TestBlankLineHandling:
    """Test blank line adjustment."""

    def test_adds_blank_line_before_column(self, formatter):
        """Test that blank lines are added before new columns."""
        content = """columns:
  - name: col1
    description: First
  - name: col2
    description: Second"""

        result = formatter._adjust_blank_lines(content)

        assert "\n\n  - name: col2" in result

    def test_removes_excessive_blank_lines(self, formatter):
        """Test that excessive blank lines are removed."""
        content = """name: test


description: test"""

        result = formatter._adjust_blank_lines(content)

        # Should have only one blank line
        assert "\n\n\n" not in result
        assert "\n\ndescription:" in result

    def test_ensures_single_trailing_newline(self, formatter):
        """Test that file ends with exactly one newline."""
        content = """name: test
description: test


"""

        result = formatter._adjust_blank_lines(content)

        assert result.endswith("\n")
        assert not result.endswith("\n\n")


class TestFormattingService:
    """Test the complete formatting service."""

    def test_format_preserves_content(self, formatter, tmp_path, yaml_handler):
        """Test that formatting preserves actual content."""
        # Create test file
        test_file = tmp_path / "test.yml"
        content = {
            "version": 2,
            "models": [
                {
                    "columns": [
                        {
                            "name": "id",
                            "description": "ID column",
                            "meta": {"sst": {"column_type": "dimension", "data_type": "number"}},
                        }
                    ],
                    "name": "test_model",
                    "description": "Test model",
                }
            ],
        }

        with open(test_file, "w") as f:
            yaml_handler.dump(content, f)

        # Format
        needs_formatting = formatter._format_file(test_file)

        # Read back
        with open(test_file, "r") as f:
            result = yaml_handler.load(f)

        # Check content preserved
        assert result["models"][0]["name"] == "test_model"
        assert result["models"][0]["columns"][0]["name"] == "id"
        assert result["models"][0]["columns"][0]["meta"]["sst"]["column_type"] == "dimension"

    def test_dry_run_doesnt_modify_file(self, tmp_path, yaml_handler):
        """Test that dry-run mode doesn't modify files."""
        config = FormattingConfig(dry_run=True, check_only=False)
        formatter = YAMLFormattingService(config)

        # Create test file with bad ordering
        test_file = tmp_path / "test.yml"
        content = {"version": 2, "models": [{"columns": [], "name": "test_model", "description": "Test"}]}

        with open(test_file, "w") as f:
            yaml_handler.dump(content, f)

        original_content = test_file.read_text()

        # Format in dry-run mode
        formatter._format_file(test_file)

        # File should be unchanged
        assert test_file.read_text() == original_content

    def test_check_only_mode(self, tmp_path, yaml_handler):
        """Test that check-only mode reports but doesn't modify."""
        config = FormattingConfig(dry_run=False, check_only=True)
        formatter = YAMLFormattingService(config)

        # Create test file with bad ordering
        test_file = tmp_path / "test.yml"
        content = {"version": 2, "models": [{"columns": [], "name": "test_model", "description": "Test"}]}

        with open(test_file, "w") as f:
            yaml_handler.dump(content, f)

        original_content = test_file.read_text()

        # Format in check-only mode
        needs_formatting = formatter._format_file(test_file)

        # Should report needs formatting
        assert needs_formatting

        # File should be unchanged
        assert test_file.read_text() == original_content


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_handles_empty_file(self, formatter, tmp_path):
        """Test that empty files are handled gracefully."""
        test_file = tmp_path / "empty.yml"
        test_file.write_text("")

        # Should not raise
        needs_formatting = formatter._format_file(test_file)
        assert not needs_formatting

    def test_handles_file_without_models(self, formatter, tmp_path, yaml_handler):
        """Test that files without models are handled."""
        test_file = tmp_path / "no_models.yml"
        content = {"version": 2}

        with open(test_file, "w") as f:
            yaml_handler.dump(content, f)

        # Should not raise
        needs_formatting = formatter._format_file(test_file)
        assert not needs_formatting

    def test_handles_columns_without_meta(self, formatter):
        """Test that columns without meta are handled."""
        column = {"name": "test_col", "description": "Test"}

        # Should not raise
        formatter._format_column(column)

        # Order should still be correct
        keys = list(column.keys())
        assert keys[0] == "name"
        assert keys[1] == "description"


class TestFormatPath:
    """Test the format_path method."""

    def test_format_single_file(self, formatter, tmp_path, yaml_handler):
        """Test formatting a single file."""
        test_file = tmp_path / "test.yml"
        content = {"version": 2, "models": [{"columns": [], "name": "test", "description": "Test"}]}

        with open(test_file, "w") as f:
            yaml_handler.dump(content, f)

        stats = formatter.format_path(test_file)

        assert stats["files_processed"] == 1
        assert stats["files_formatted"] >= 0
        assert stats["errors"] == 0

    def test_format_directory(self, formatter, tmp_path, yaml_handler):
        """Test formatting all files in a directory."""
        # Create multiple files
        for i in range(3):
            test_file = tmp_path / f"test{i}.yml"
            content = {"version": 2, "models": [{"columns": [], "name": f"test{i}", "description": "Test"}]}

            with open(test_file, "w") as f:
                yaml_handler.dump(content, f)

        stats = formatter.format_path(tmp_path)

        assert stats["files_processed"] == 3
        assert stats["errors"] == 0
