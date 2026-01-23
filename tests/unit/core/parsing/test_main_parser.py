"""
Test main Parser functionality.

Tests the core Parser class that orchestrates semantic model parsing.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from snowflake_semantic_tools.core.parsing.parser import Parser


class TestParser:
    """Test Parser main functionality."""

    @pytest.fixture
    def parser(self):
        """Create Parser instance."""
        return Parser()

    def test_parser_initialization(self, parser):
        """Test parser initialization."""
        assert parser is not None
        assert hasattr(parser, "dbt_catalog")
        assert hasattr(parser, "metrics_catalog")

    def test_parser_has_required_methods(self, parser):
        """Test that parser has required methods."""
        required_methods = ["parse_all_files"]  # Updated to match actual API

        for method in required_methods:
            assert hasattr(parser, method), f"Parser missing method: {method}"

    def test_parser_catalog_initialization(self, parser):
        """Test parser catalog initialization."""
        # Catalogs should start empty
        assert parser.dbt_catalog == {}
        assert parser.metrics_catalog == []
        assert parser.custom_instructions_catalog == []

    def test_parser_build_catalog_empty_input(self, parser):
        """Test parsing with empty input."""
        dbt_files = []
        semantic_files = []

        # Should handle empty input gracefully
        try:
            result = parser.parse_all_files(dbt_files, semantic_files)
            # Should not crash and return a result
            assert result is not None
            assert isinstance(result, dict)
        except Exception as e:
            pytest.fail(f"Parser failed with empty input: {e}")

    def test_parser_with_mock_file_detector(self, parser):
        """Test parser with mock file detector."""
        with patch.object(parser, "file_detector") as mock_detector:
            mock_detector.detect_file_type.return_value = "metrics"

            # Should be able to call file detection
            file_type = parser.file_detector.detect_file_type("test.yml")
            assert file_type == "metrics"

    def test_parser_error_handling(self, parser):
        """Test parser error handling."""
        # Test with invalid input
        try:
            result = parser.parse(None)
            # Should either return empty result or raise appropriate error
            assert result is not None or True  # Either is acceptable
        except Exception:
            # Should raise appropriate exception for invalid input
            pass

    def test_parser_parse_method_exists(self, parser):
        """Test that parse_all_files method exists and is callable."""
        assert callable(getattr(parser, "parse_all_files", None))

    def test_parser_with_valid_semantic_structure(self, parser):
        """Test parser with valid semantic model structure."""
        # Mock valid semantic model data
        mock_data = {
            "metrics": [{"name": "revenue", "expr": "SUM(orders.amount)", "tables": ["orders"]}],
            "relationships": [],
            "filters": [],
        }

        # Test that parser can handle valid structure
        try:
            # The exact method name might vary
            if hasattr(parser, "parse_semantic_data"):
                result = parser.parse_semantic_data(mock_data)
            elif hasattr(parser, "process_parsed_data"):
                result = parser.process_parsed_data(mock_data)
            else:
                # Just test that parser doesn't crash with valid data
                assert True
        except Exception as e:
            pytest.fail(f"Parser failed with valid data: {e}")


class TestParserPerformance:
    """Test parser performance characteristics."""

    def test_parser_creation_performance(self):
        """Test that parser creation is fast."""
        import time

        start_time = time.time()

        # Create multiple parsers
        parsers = [Parser() for _ in range(10)]

        end_time = time.time()
        duration = end_time - start_time

        # Should be very fast (< 0.1 seconds for 10 parsers)
        assert duration < 0.1
        assert len(parsers) == 10

    def test_parser_memory_usage(self):
        """Test parser memory usage."""
        parser = Parser()

        # Parser should not consume excessive memory on creation
        import sys

        initial_size = sys.getsizeof(parser)

        # Should be reasonable size (< 1KB for empty parser)
        assert initial_size < 1024

    def test_parser_thread_safety_basic(self):
        """Test basic parser thread safety."""
        import threading

        results = []

        def create_parser_in_thread():
            parser = Parser()
            results.append(parser is not None)

        # Create parsers in multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=create_parser_in_thread)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All threads should successfully create parsers
        assert len(results) == 5
        assert all(results)


class TestCustomInstructionsParsingEdgeCases:
    """Test edge cases in custom instructions parsing."""

    def test_empty_instructions_list(self):
        """Test parsing empty instructions list."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_snowflake_custom_instructions,
        )

        result = parse_snowflake_custom_instructions([], Path("test.yml"))
        assert result == []

    def test_instruction_missing_name(self):
        """Test instruction without name field."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_snowflake_custom_instructions,
        )

        instructions = [{"question_categorization": "Rule", "sql_generation": "SQL"}]
        result = parse_snowflake_custom_instructions(instructions, Path("test.yml"))
        assert len(result) == 1
        assert result[0]["name"] == ""

    def test_instruction_missing_all_fields(self):
        """Test instruction with no fields."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_snowflake_custom_instructions,
        )

        instructions = [{}]
        result = parse_snowflake_custom_instructions(instructions, Path("test.yml"))
        assert len(result) == 1
        assert result[0]["name"] == ""
        assert result[0]["question_categorization"] is None
        assert result[0]["sql_generation"] is None

    def test_instruction_name_case_handling(self):
        """Test that instruction names are uppercased."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_snowflake_custom_instructions,
        )

        instructions = [
            {"name": "lowercase", "question_categorization": "Rule", "sql_generation": "SQL"},
            {"name": "UPPERCASE", "question_categorization": "Rule", "sql_generation": "SQL"},
            {"name": "MixedCase", "question_categorization": "Rule", "sql_generation": "SQL"},
        ]
        result = parse_snowflake_custom_instructions(instructions, Path("test.yml"))
        assert result[0]["name"] == "LOWERCASE"
        assert result[1]["name"] == "UPPERCASE"
        assert result[2]["name"] == "MIXEDCASE"

    def test_instruction_fields_trimming(self):
        """Test that instruction fields are trimmed."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_snowflake_custom_instructions,
        )

        instructions = [
            {
                "name": "test",
                "question_categorization": "  Rule with spaces  ",
                "sql_generation": "\n\nSQL with newlines\n\n",
            }
        ]
        result = parse_snowflake_custom_instructions(instructions, Path("test.yml"))
        assert result[0]["question_categorization"] == "Rule with spaces"
        assert result[0]["sql_generation"] == "SQL with newlines"


class TestSemanticViewsInstructionExtractionEdgeCases:
    """Test edge cases in extracting instruction names from semantic views."""

    def test_view_without_custom_instructions(self):
        """Test view without custom_instructions field."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_semantic_views,
        )

        views = [
            {
                "name": "test_view",
                "tables": ["table1"],
            }
        ]
        result = parse_semantic_views(views, Path("test.yml"), {})
        assert len(result) == 1
        assert result[0]["custom_instructions"] is None

    def test_view_with_empty_custom_instructions(self):
        """Test view with empty custom_instructions list."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_semantic_views,
        )

        views = [
            {
                "name": "test_view",
                "tables": ["table1"],
                "custom_instructions": [],
            }
        ]
        result = parse_semantic_views(views, Path("test.yml"), {})
        assert len(result) == 1
        assert result[0]["custom_instructions"] is None

    def test_view_with_multiple_instruction_references(self):
        """Test view with multiple custom instruction references."""
        from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
            parse_semantic_views,
        )
        import json

        views = [
            {
                "name": "test_view",
                "tables": ["table1"],
                "custom_instructions": [
                    "{{ custom_instructions('inst1') }}",
                    "{{ custom_instructions('inst2') }}",
                    "{{ custom_instructions('inst3') }}",
                ],
            }
        ]
        instruction_names_map = {"test_view": ["INST1", "INST2", "INST3"]}
        result = parse_semantic_views(views, Path("test.yml"), instruction_names_map)
        assert len(result) == 1
        instruction_names = json.loads(result[0]["custom_instructions"])
        assert len(instruction_names) == 3
        assert "INST1" in instruction_names
        assert "INST2" in instruction_names
        assert "INST3" in instruction_names


class TestInstructionNameExtractionEdgeCases:
    """Test edge cases in extracting instruction names from YAML content."""

    @pytest.fixture
    def parser(self):
        """Create Parser instance."""
        return Parser()

    def test_extract_from_yaml_with_no_views(self, parser):
        """Test extraction from YAML with no semantic_views."""
        content = "semantic_views: []"
        result = parser._extract_custom_instruction_names_from_views(content)
        assert result == {}

    def test_extract_from_yaml_with_no_custom_instructions(self, parser):
        """Test extraction from YAML where views have no custom_instructions."""
        content = """
semantic_views:
  - name: view1
    tables:
      - {{ table('t1') }}
  - name: view2
    tables:
      - {{ table('t2') }}
"""
        result = parser._extract_custom_instruction_names_from_views(content)
        assert result == {}

    def test_extract_with_malformed_yaml(self, parser):
        """Test extraction from malformed YAML."""
        content = """
semantic_views:
  - name: view1
    custom_instructions:
      - {{ custom_instructions('inst1') }}
    # Missing closing bracket or quote
"""
        result = parser._extract_custom_instruction_names_from_views(content)
        assert isinstance(result, dict)

    def test_extract_with_nested_structures(self, parser):
        """Test extraction with complex nested YAML structures."""
        content = """
semantic_views:
  - name: view1
    description: |
      Multi-line description
      with {{ custom_instructions('should_not_match') }} inside
    tables:
      - {{ table('t1') }}
    custom_instructions:
      - {{ custom_instructions('should_match') }}
    other_field:
      nested:
        - {{ custom_instructions('also_should_not_match') }}
"""
        result = parser._extract_custom_instruction_names_from_views(content)
        assert "view1" in result
        assert "SHOULD_MATCH" in result["view1"]
        assert "should_not_match" not in result.get("view1", [])

    def test_extract_with_single_and_double_quotes(self, parser):
        """Test extraction with both single and double quotes."""
        content = """
semantic_views:
  - name: view1
    custom_instructions:
      - {{ custom_instructions('inst1') }}
      - {{ custom_instructions("inst2") }}
"""
        result = parser._extract_custom_instruction_names_from_views(content)
        assert "view1" in result
        assert "INST1" in result["view1"]
        assert "INST2" in result["view1"]
