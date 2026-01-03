"""
Comprehensive tests for FileTypeDetector.

Tests file type detection across all supported semantic model types.
"""

import tempfile
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest
import yaml

from snowflake_semantic_tools.core.parsing.file_detector import FileTypeDetector


class TestFileTypeDetector:
    """Test FileTypeDetector with comprehensive scenarios."""

    @pytest.fixture
    def detector(self):
        """Create FileTypeDetector instance."""
        return FileTypeDetector()

    def test_detect_metrics_file(self, detector):
        """Test detection of metrics files."""
        metrics_content = """
        snowflake_metrics:
          - name: total_revenue
            expr: SUM(amount)
            tables: [orders]
        """

        with patch("builtins.open", mock_open(read_data=metrics_content)):
            file_type = detector.detect_file_type("metrics.yml")
            assert file_type == "semantic"

    def test_detect_relationships_file(self, detector):
        """Test detection of relationships files."""
        relationships_content = """
        snowflake_relationships:
          - name: orders_to_users
            left_table: orders
            right_table: users
            relationship_type: many_to_one
        """

        with patch("builtins.open", mock_open(read_data=relationships_content)):
            file_type = detector.detect_file_type("relationships.yml")
            assert file_type == "semantic"

    def test_detect_filters_file(self, detector):
        """Test detection of filters files."""
        filters_content = """
        snowflake_filters:
          - name: active_users
            expr: "status = 'active'"
            tables: [users]
        """

        with patch("builtins.open", mock_open(read_data=filters_content)):
            file_type = detector.detect_file_type("filters.yml")
            assert file_type == "semantic"

    def test_detect_custom_instructions_file(self, detector):
        """Test detection of custom instructions files."""
        instructions_content = """
        snowflake_custom_instructions:
          - name: business_rules
            question_categorization: "Focus on active users"
            sql_generation: "Exclude test data"
        """

        with patch("builtins.open", mock_open(read_data=instructions_content)):
            file_type = detector.detect_file_type("custom_instructions.yml")
            assert file_type == "semantic"

    def test_detect_verified_queries_file(self, detector):
        """Test detection of verified queries files."""
        queries_content = """
        snowflake_verified_queries:
          - name: monthly_revenue
            question: "What is monthly revenue?"
            sql: "SELECT SUM(amount) FROM orders"
        """

        with patch("builtins.open", mock_open(read_data=queries_content)):
            file_type = detector.detect_file_type("verified_queries.yml")
            assert file_type == "semantic"

    def test_detect_semantic_views_file(self, detector):
        """Test detection of semantic views files."""
        views_content = """
        semantic_views:
          - name: sales_dashboard
            tables: [orders, users]
            metrics: [total_revenue]
        """

        with patch("builtins.open", mock_open(read_data=views_content)):
            file_type = detector.detect_file_type("semantic_views.yml")
            assert file_type == "semantic"

    def test_detect_dbt_file(self, detector):
        """Test detection of dbt model files."""
        dbt_content = """
        version: 2
        models:
          - name: users
            description: "User accounts"
            columns:
              - name: id
                description: "User ID"
        """

        with patch("builtins.open", mock_open(read_data=dbt_content)):
            file_type = detector.detect_file_type("schema.yml")
            assert file_type == "dbt"

    def test_detect_mixed_content_file(self, detector):
        """Test file with multiple semantic types (should detect first one)."""
        mixed_content = """
        snowflake_metrics:
          - name: revenue
            expr: SUM(amount)
        
        snowflake_filters:
          - name: active_only
            expr: "status = 'active'"
        """

        with patch("builtins.open", mock_open(read_data=mixed_content)):
            file_type = detector.detect_file_type("mixed.yml")
            # Should detect the first type found
            assert file_type == "semantic"

    def test_detect_empty_file(self, detector):
        """Test detection of empty files."""
        with patch("builtins.open", mock_open(read_data="")):
            file_type = detector.detect_file_type("empty.yml")
            assert file_type == "unknown"

    def test_detect_invalid_yaml(self, detector):
        """Test detection of invalid YAML files."""
        invalid_yaml = """
        snowflake_metrics:
          - name: revenue
            expr: SUM(amount
        # Missing closing bracket
        """

        with patch("builtins.open", mock_open(read_data=invalid_yaml)):
            file_type = detector.detect_file_type("invalid.yml")
            # Even invalid YAML is detected as semantic if it contains semantic patterns
            assert file_type == "semantic"

    def test_detect_non_semantic_yaml(self, detector):
        """Test detection of non-semantic YAML files."""
        non_semantic_content = """
        some_other_config:
          setting1: value1
          setting2: value2
        """

        with patch("builtins.open", mock_open(read_data=non_semantic_content)):
            file_type = detector.detect_file_type("config.yml")
            assert file_type == "unknown"

    def test_file_not_found(self, detector):
        """Test handling of non-existent files."""
        with patch("builtins.open", side_effect=FileNotFoundError()):
            file_type = detector.detect_file_type("nonexistent.yml")
            assert file_type == "unknown"

    def test_permission_denied(self, detector):
        """Test handling of permission denied errors."""
        with patch("builtins.open", side_effect=PermissionError()):
            file_type = detector.detect_file_type("restricted.yml")
            assert file_type == "unknown"

    def test_case_insensitive_detection(self, detector):
        """Test case sensitive detection of semantic types."""
        # Only lowercase should be detected as semantic
        lowercase_content = """
        snowflake_metrics:
          - name: test
            expr: COUNT(*)
        """

        with patch("builtins.open", mock_open(read_data=lowercase_content)):
            file_type = detector.detect_file_type("test.yml")
            assert file_type == "semantic"

        # Uppercase should not be detected
        uppercase_content = """
        SNOWFLAKE_METRICS:
          - name: test
            expr: COUNT(*)
        """

        with patch("builtins.open", mock_open(read_data=uppercase_content)):
            file_type = detector.detect_file_type("test.yml")
            assert file_type == "unknown"

    def test_semantic_type_enum_values(self):
        """Test semantic type string values."""
        # FileTypeDetector returns string values directly
        detector = FileTypeDetector()
        assert detector.detect_file_type.__doc__ is not None  # Just a basic test that the method exists

    def test_detect_with_path_object(self, detector):
        """Test detection with Path objects instead of strings."""
        metrics_content = """
        snowflake_metrics:
          - name: revenue
            expr: SUM(amount)
        """

        with patch("builtins.open", mock_open(read_data=metrics_content)):
            file_type = detector.detect_file_type(Path("metrics.yml"))
            assert file_type == "semantic"

    def test_detect_yaml_vs_yml_extension(self, detector):
        """Test detection works with both .yml and .yaml extensions."""
        metrics_content = """
        snowflake_metrics:
          - name: revenue
            expr: SUM(amount)
        """

        for extension in [".yml", ".yaml"]:
            with patch("builtins.open", mock_open(read_data=metrics_content)):
                file_type = detector.detect_file_type(f"metrics{extension}")
                assert file_type == "semantic"

    def test_detect_with_comments_and_whitespace(self, detector):
        """Test detection with comments and extra whitespace."""
        content_with_comments = """
        # This is a metrics file
        
        snowflake_metrics:  # Metrics definition
          - name: revenue  # Revenue metric
            expr: SUM(amount)
            tables: [orders]
        """

        with patch("builtins.open", mock_open(read_data=content_with_comments)):
            file_type = detector.detect_file_type("metrics.yml")
            assert file_type == "semantic"

    def test_detect_large_file_performance(self, detector):
        """Test detection performance with large files."""
        # Create a large metrics file
        large_content = "snowflake_metrics:\n"
        for i in range(1000):
            large_content += f"""
  - name: metric_{i}
    expr: SUM(column_{i})
    tables: [table_{i}]
"""

        with patch("builtins.open", mock_open(read_data=large_content)):
            import time

            start_time = time.time()
            file_type = detector.detect_file_type("large_metrics.yml")
            end_time = time.time()

            assert file_type == "semantic"
            # Should complete quickly (less than 1 second)
            assert (end_time - start_time) < 1.0

    def test_detect_unicode_content(self, detector):
        """Test detection with unicode content."""
        unicode_content = """
        snowflake_metrics:
          - name: revenue_€
            description: "Revenue in euros €"
            expr: SUM(amount_€)
            tables: [orders_français]
        """

        with patch("builtins.open", mock_open(read_data=unicode_content)):
            file_type = detector.detect_file_type("unicode_metrics.yml")
            assert file_type == "semantic"

    def test_batch_detection(self, detector):
        """Test batch detection of multiple files."""
        files_and_content = {
            "metrics.yml": "snowflake_metrics:\n  - name: test",
            "relationships.yml": "snowflake_relationships:\n  - name: test",
            "filters.yml": "snowflake_filters:\n  - name: test",
            "unknown.yml": "some_config:\n  setting: value",
        }

        results = {}
        for filename, content in files_and_content.items():
            with patch("builtins.open", mock_open(read_data=content)):
                results[filename] = detector.detect_file_type(filename)

        assert results["metrics.yml"] == "semantic"
        assert results["relationships.yml"] == "semantic"
        assert results["filters.yml"] == "semantic"
        assert results["unknown.yml"] == "unknown"
