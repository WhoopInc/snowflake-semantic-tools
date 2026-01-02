"""
Test YAML Error Handling (Issue #8)

Tests that YAML parsing errors fail loudly with helpful error messages
instead of being silently ignored.
"""

import pytest
import tempfile
from pathlib import Path

from snowflake_semantic_tools.core.parsing.parser import Parser, ParsingCriticalError
from snowflake_semantic_tools.core.parsing.parsers.error_handler import (
    format_yaml_error,
    _get_yaml_error_suggestion,
)
import yaml


class TestYAMLErrorHandling:
    """Test that YAML errors are caught and reported properly."""

    @pytest.fixture
    def parser(self):
        """Create Parser instance."""
        return Parser(enable_template_resolution=True)

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_unquoted_colon_raises_parsing_error(self, parser, temp_dir):
        """Test that unquoted colons in descriptions raise ParsingCriticalError."""
        # Create a metrics file with unquoted colon (invalid YAML)
        bad_yaml = """
snowflake_metrics:
  - name: test_metric
    tables:
      - customers
    description: Use this metric like this: SUM(amount) for totals
    expr: SUM(amount)
"""
        metrics_file = temp_dir / "bad_metrics.yml"
        metrics_file.write_text(bad_yaml)

        # Create a minimal dbt file
        dbt_yaml = """
version: 2
models:
  - name: customers
    description: Customer table
"""
        dbt_file = temp_dir / "dbt_models.yml"
        dbt_file.write_text(dbt_yaml)

        # Parser should raise ParsingCriticalError due to YAML error
        with pytest.raises(ParsingCriticalError) as exc_info:
            parser.parse_all_files(dbt_files=[dbt_file], semantic_files=[metrics_file])

        # Verify error contains useful information
        assert len(exc_info.value.errors) > 0
        error_msg = exc_info.value.errors[0]
        assert "bad_metrics.yml" in error_msg
        assert "line" in error_msg.lower()

    def test_error_message_includes_file_and_line(self, parser, temp_dir):
        """Test that error messages include file path and line number."""
        # Create invalid YAML
        bad_yaml = """
snowflake_metrics:
  - name: test
    description: problem: here: multiple colons
"""
        metrics_file = temp_dir / "test_metrics.yml"
        metrics_file.write_text(bad_yaml)

        dbt_file = temp_dir / "dbt.yml"
        dbt_file.write_text("version: 2\nmodels: []")

        with pytest.raises(ParsingCriticalError) as exc_info:
            parser.parse_all_files(dbt_files=[dbt_file], semantic_files=[metrics_file])

        error_msg = exc_info.value.errors[0]
        # Should contain filename
        assert "test_metrics.yml" in error_msg
        # Should contain line reference
        assert "line" in error_msg.lower()

    def test_valid_yaml_parses_successfully(self, parser, temp_dir):
        """Test that valid YAML with quoted/multiline descriptions works."""
        # Valid YAML using multiline syntax
        good_yaml = """
snowflake_metrics:
  - name: test_metric
    tables:
      - customers
    description: |-
      Use this metric like this: SUM(amount) for totals.
      Filter by date: use WHERE clause.
    expr: SUM(amount)
"""
        metrics_file = temp_dir / "good_metrics.yml"
        metrics_file.write_text(good_yaml)

        dbt_yaml = """
version: 2
models:
  - name: customers
    description: Customer table
"""
        dbt_file = temp_dir / "dbt.yml"
        dbt_file.write_text(dbt_yaml)

        # Should NOT raise any errors
        result = parser.parse_all_files(dbt_files=[dbt_file], semantic_files=[metrics_file])

        # Verify metrics were collected
        assert len(parser.metrics_catalog) == 1
        assert parser.metrics_catalog[0]["name"] == "test_metric"

    def test_errors_tracked_in_error_tracker(self, parser, temp_dir):
        """Test that errors are added to the error tracker."""
        bad_yaml = """
snowflake_metrics:
  - name: test
    description: bad: yaml: here
"""
        metrics_file = temp_dir / "metrics.yml"
        metrics_file.write_text(bad_yaml)

        dbt_file = temp_dir / "dbt.yml"
        dbt_file.write_text("version: 2\nmodels: []")

        with pytest.raises(ParsingCriticalError):
            parser.parse_all_files(dbt_files=[dbt_file], semantic_files=[metrics_file])

        # Verify errors were tracked
        errors = parser.error_tracker.get_all_errors()
        assert len(errors) > 0
        assert any("[metrics]" in e for e in errors)


class TestYAMLErrorSuggestions:
    """Test that error suggestions are helpful and accurate."""

    def test_suggestion_for_unquoted_colon(self):
        """Test suggestion for 'mapping values are not allowed' error."""
        error_str = "mapping values are not allowed here"
        suggestion = _get_yaml_error_suggestion(error_str)

        assert "colon" in suggestion.lower()
        assert "description: |-" in suggestion
        assert 'description: "' in suggestion

    def test_suggestion_for_template_syntax(self):
        """Test suggestion for template-related errors."""
        error_str = "found unhashable key"
        suggestion = _get_yaml_error_suggestion(error_str)

        assert "template" in suggestion.lower()
        assert "{{ table" in suggestion

    def test_no_suggestion_for_unknown_error(self):
        """Test that unknown errors return empty suggestion."""
        error_str = "some random error message"
        suggestion = _get_yaml_error_suggestion(error_str)

        assert suggestion == ""

    def test_format_yaml_error_includes_suggestion(self):
        """Test that format_yaml_error includes suggestions."""
        # Create a real YAML error
        bad_yaml = "key: value: another"
        try:
            yaml.safe_load(bad_yaml)
        except yaml.YAMLError as e:
            error_msg = format_yaml_error(e, Path("test.yml"))

            # Should include basic info
            assert "test.yml" in error_msg
            assert "line" in error_msg.lower()

            # Should include suggestion for colon error
            assert "Suggestion" in error_msg
            assert "description: |-" in error_msg


class TestErrorTrackerIntegration:
    """Test ErrorTracker functionality with parser integration."""

    def test_errors_cleared_on_reset(self):
        """Test that parser reset clears errors."""
        parser = Parser()
        parser.error_tracker.add_error("Test error")

        assert parser.error_tracker.get_error_count() == 1

        parser._reset_state()

        assert parser.error_tracker.get_error_count() == 0

    def test_multiple_errors_tracked(self, tmp_path):
        """Test that multiple YAML errors are all tracked."""
        parser = Parser()

        # Create two bad files
        bad1 = tmp_path / "bad1.yml"
        bad1.write_text("snowflake_metrics:\n  - description: a: b: c")

        bad2 = tmp_path / "bad2.yml"
        bad2.write_text("snowflake_metrics:\n  - description: x: y: z")

        dbt = tmp_path / "dbt.yml"
        dbt.write_text("version: 2\nmodels: []")

        with pytest.raises(ParsingCriticalError) as exc_info:
            parser.parse_all_files(dbt_files=[dbt], semantic_files=[bad1, bad2])

        # Both files should have errors tracked
        errors = exc_info.value.errors
        assert len(errors) >= 2
        assert any("bad1.yml" in e for e in errors)
        assert any("bad2.yml" in e for e in errors)
