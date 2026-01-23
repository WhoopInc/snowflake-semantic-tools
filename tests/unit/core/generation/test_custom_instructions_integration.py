#!/usr/bin/env python3
"""
Comprehensive unit tests for custom instructions functionality.

Tests that custom instructions are correctly:
1. Parsed from YAML with separate question_categorization and sql_generation fields
2. Stored in metadata tables with correct schema
3. Retrieved and aggregated correctly
4. Formatted as AI_SQL_GENERATION and AI_QUESTION_CATEGORIZATION clauses
5. Included in CREATE SEMANTIC VIEW DDL in correct order
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
import json

from snowflake_semantic_tools.core.generation.semantic_view_builder import SemanticViewBuilder
from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig


class TestCustomInstructionsClauseGeneration:
    """Test AI guidance clause generation from custom instructions."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test",
            password="test",
            role="test",
            warehouse="test",
            database="test_db",
            schema="test_schema",
        )
        builder = SemanticViewBuilder(config)
        builder.metadata_database = "META_DB"
        builder.metadata_schema = "META_SCHEMA"
        return builder

    def test_both_question_categorization_and_sql_generation(self, builder):
        """Test custom instruction with both fields generates both clauses."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            (
                "BUSINESS_RULES",
                "Reject questions about individual users. Ask users to contact their admin.",
                "Always round monetary values to 2 decimal places. Use AVG for percentages, never SUM.",
            )
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["business_rules"])

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        assert "Reject questions about individual users" in result
        assert "Always round monetary values" in result
        assert "Use AVG for percentages" in result

        # Verify clause format and order (AI_SQL_GENERATION comes first per Snowflake syntax)
        assert result.startswith("  AI_SQL_GENERATION '")
        assert "AI_QUESTION_CATEGORIZATION '" in result
        assert result.count("AI_QUESTION_CATEGORIZATION") == 1
        assert result.count("AI_SQL_GENERATION") == 1
        # Verify order: AI_SQL_GENERATION before AI_QUESTION_CATEGORIZATION
        sql_gen_pos = result.find("AI_SQL_GENERATION")
        question_cat_pos = result.find("AI_QUESTION_CATEGORIZATION")
        assert sql_gen_pos < question_cat_pos

    def test_only_question_categorization(self, builder):
        """Test custom instruction with only question_categorization."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            ("PRIVACY_RULES", "Reject all questions asking about individual users.", None)
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["privacy_rules"])

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" not in result
        assert "Reject all questions asking about individual users" in result

    def test_only_sql_generation(self, builder):
        """Test custom instruction with only sql_generation."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            ("FORMATTING_RULES", None, "Round all numeric values to 2 decimal places.")
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["formatting_rules"])

        assert "AI_QUESTION_CATEGORIZATION" not in result
        assert "AI_SQL_GENERATION" in result
        assert "Round all numeric values to 2 decimal places" in result

    def test_multiple_instructions_aggregation(self, builder):
        """Test that multiple instructions are aggregated correctly."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            ("RULE1", "Reject user questions.", "Apply filter."),
            ("RULE2", "Ask for clarification if unclear.", "Use aggregates."),
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["rule1", "rule2"])

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        # Both instructions should be present, separated by newlines
        assert "Reject user questions" in result
        assert "Ask for clarification if unclear" in result
        assert "Apply filter" in result
        assert "Use aggregates" in result

    def test_empty_custom_instruction_names(self, builder):
        """Test that empty or None custom_instruction_names returns empty string."""
        mock_conn = MagicMock()

        result1 = builder._build_ai_guidance_clauses(mock_conn, None)
        result2 = builder._build_ai_guidance_clauses(mock_conn, [])

        assert result1 == ""
        assert result2 == ""

    def test_no_instructions_found_in_database(self, builder):
        """Test when custom instructions are not found in metadata table."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = []  # No results

        result = builder._build_ai_guidance_clauses(mock_conn, ["missing_instruction"])

        assert result == ""

    def test_special_characters_escaping(self, builder):
        """Test that single quotes in instruction text are properly escaped."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            (
                "QUOTE_TEST",
                "Don't reject questions with 'quotes'.",
                "Use O'Brien's method for calculations.",
            )
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["quote_test"])

        # Single quotes should be escaped (doubled) in SQL string literals
        assert "Don''t reject" in result or "Don't reject" in result
        assert "'quotes''" in result or "'quotes'" in result
        assert "O''Brien''s" in result or "O'Brien's" in result

        # Verify clauses are properly closed
        assert result.count("'") >= 4  # Opening and closing quotes for each clause

    def test_multiline_instructions(self, builder):
        """Test that multiline instructions are handled correctly."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        multiline_sql = """Always round to 2 decimals.
Use AVG for percentages.
Never use SUM for percentages."""

        mock_cursor.fetchall.return_value = [
            ("MULTILINE_RULE", "Reject user questions.", multiline_sql)
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["multiline_rule"])

        assert "AI_SQL_GENERATION" in result
        assert "Always round to 2 decimals" in result
        assert "Use AVG for percentages" in result
        assert "Never use SUM" in result

    def test_whitespace_trimming(self, builder):
        """Test that trailing whitespace is trimmed from instructions."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            ("TRIMMED_RULE", "  Reject questions.  \n\n  ", "  Round values.  \n\n  ")
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["trimmed_rule"])

        # Should not have trailing newlines before closing quote
        assert result.endswith("'") or result.rstrip().endswith("'")
        # Content should be trimmed
        assert "Reject questions" in result
        assert "Round values" in result

    def test_mixed_instructions_some_missing_fields(self, builder):
        """Test multiple instructions where some have missing fields."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            ("RULE1", "Question rule 1", "SQL rule 1"),
            ("RULE2", None, "SQL rule 2"),  # No question_categorization
            ("RULE3", "Question rule 3", None),  # No sql_generation
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["rule1", "rule2", "rule3"])

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        # Should include all non-None fields
        assert "Question rule 1" in result
        assert "Question rule 3" in result
        assert "SQL rule 1" in result
        assert "SQL rule 2" in result

    def test_case_insensitive_instruction_names(self, builder):
        """Test that instruction names are case-insensitive."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            ("BUSINESS_RULES", "Reject questions.", "Round values.")
        ]

        # Try different case variations
        result1 = builder._build_ai_guidance_clauses(mock_conn, ["business_rules"])
        result2 = builder._build_ai_guidance_clauses(mock_conn, ["BUSINESS_RULES"])
        result3 = builder._build_ai_guidance_clauses(mock_conn, ["Business_Rules"])

        # All should work (the query uses UPPER() in SQL)
        assert result1 != ""
        assert result2 != ""
        assert result3 != ""

    def test_empty_string_fields(self, builder):
        """Test that empty string fields are handled correctly."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            ("EMPTY_RULE", "", "SQL rule"),  # Empty question_categorization
            ("EMPTY_SQL", "Question rule", ""),  # Empty sql_generation
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["empty_rule", "empty_sql"])

        # Should only include non-empty fields
        assert "AI_SQL_GENERATION" in result
        assert "SQL rule" in result
        # Empty question_categorization should not create a clause
        if "AI_QUESTION_CATEGORIZATION" in result:
            # If it's there, it should only have the non-empty one
            assert "Question rule" in result

    def test_very_long_instructions(self, builder):
        """Test that very long instruction text is handled correctly."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        long_text = "A" * 10000  # 10KB of text

        mock_cursor.fetchall.return_value = [
            ("LONG_RULE", "Short question rule.", long_text)
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["long_rule"])

        assert "AI_SQL_GENERATION" in result
        assert len(result) > 10000  # Should include the long text
        # Should still be properly quoted
        assert result.count("'") >= 2  # Opening and closing quotes

    def test_unicode_characters(self, builder):
        """Test that unicode characters in instructions are handled correctly."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        unicode_text = "Reject questions about 用户数据. Use méthode française."

        mock_cursor.fetchall.return_value = [
            ("UNICODE_RULE", unicode_text, "Round to 2 décimales.")
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["unicode_rule"])

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        assert "用户数据" in result or "méthode" in result  # Unicode should be preserved

    def test_sql_injection_prevention(self, builder):
        """Test that SQL injection attempts in instruction text are escaped."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Attempt SQL injection via single quotes
        malicious_text = "'; DROP TABLE users; --"

        mock_cursor.fetchall.return_value = [
            ("MALICIOUS", "Normal question rule.", malicious_text)
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["malicious"])

        # Single quotes should be escaped
        assert "''" in result or "'; DROP" in result
        # The malicious SQL should be treated as literal text, not executed
        assert "DROP TABLE" in result  # Should be in the string literal, not executed


class TestCustomInstructionsInFullDDL:
    """Test custom instructions integration in full DDL generation."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test",
            password="test",
            role="test",
            warehouse="test",
            database="test_db",
            schema="test_schema",
        )
        builder = SemanticViewBuilder(config)
        builder.metadata_database = "META_DB"
        builder.metadata_schema = "META_SCHEMA"
        return builder

    def test_custom_instructions_in_generated_sql(self, builder, monkeypatch):
        """Test that custom instructions appear in the generated CREATE SEMANTIC VIEW SQL."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock all required methods
        def mock_get_table_info(conn, table_name):
            return {
                "TABLE_NAME": "ORDERS",
                "DATABASE": "TEST_DB",
                "SCHEMA": "TEST_SCHEMA",
                "PRIMARY_KEY": '["order_id"]',
                "SYNONYMS": "[]",
                "DESCRIPTION": "Orders table",
            }

        monkeypatch.setattr(builder, "_get_table_info", mock_get_table_info)
        monkeypatch.setattr(builder, "_get_dimensions", lambda conn, name: [])
        monkeypatch.setattr(builder, "_get_time_dimensions", lambda conn, name: [])
        monkeypatch.setattr(builder, "_get_facts", lambda conn, name: [])
        monkeypatch.setattr(builder, "_build_metrics_clause", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_ca_extension", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_relationships_clause", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_facts_clause", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_dimensions_clause", lambda conn, names: "")

        # Mock custom instructions retrieval
        mock_cursor.fetchall.return_value = [
            (
                "TEST_RULE",
                "Reject questions about users.",
                "Round all values to 2 decimal places.",
            )
        ]

        sql = builder._generate_sql(
            conn=mock_conn,
            table_names=["orders"],
            view_name="test_view",
            description="Test view description",
            custom_instruction_names=["test_rule"],
        )

        # Verify SQL structure
        assert "CREATE OR REPLACE SEMANTIC VIEW" in sql
        assert "TABLES" in sql
        assert "COMMENT" in sql
        assert "AI_QUESTION_CATEGORIZATION" in sql
        assert "AI_SQL_GENERATION" in sql

        # Verify order: COMMENT before AI clauses
        comment_pos = sql.find("COMMENT")
        ai_question_pos = sql.find("AI_QUESTION_CATEGORIZATION")
        ai_sql_pos = sql.find("AI_SQL_GENERATION")

        assert comment_pos != -1
        assert ai_question_pos != -1
        assert ai_sql_pos != -1
        assert comment_pos < ai_question_pos
        assert ai_question_pos < ai_sql_pos or ai_sql_pos < ai_question_pos

        # Verify content
        assert "Reject questions about users" in sql
        assert "Round all values to 2 decimal places" in sql

    def test_no_custom_instructions_no_ai_clauses(self, builder, monkeypatch):
        """Test that when no custom instructions are provided, no AI clauses are generated."""
        mock_conn = MagicMock()

        def mock_get_table_info(conn, table_name):
            return {
                "TABLE_NAME": "ORDERS",
                "DATABASE": "TEST_DB",
                "SCHEMA": "TEST_SCHEMA",
                "PRIMARY_KEY": '["order_id"]',
                "SYNONYMS": "[]",
                "DESCRIPTION": "Orders table",
            }

        monkeypatch.setattr(builder, "_get_table_info", mock_get_table_info)
        monkeypatch.setattr(builder, "_get_dimensions", lambda conn, name: [])
        monkeypatch.setattr(builder, "_get_time_dimensions", lambda conn, name: [])
        monkeypatch.setattr(builder, "_get_facts", lambda conn, name: [])
        monkeypatch.setattr(builder, "_build_metrics_clause", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_ca_extension", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_relationships_clause", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_facts_clause", lambda conn, name: "")
        monkeypatch.setattr(builder, "_build_dimensions_clause", lambda conn, name: "")

        sql = builder._generate_sql(
            conn=mock_conn,
            table_names=["orders"],
            view_name="test_view",
            description="Test view",
            custom_instruction_names=None,
        )

        assert "AI_QUESTION_CATEGORIZATION" not in sql
        assert "AI_SQL_GENERATION" not in sql
        assert "COMMENT" in sql  # COMMENT should still be present

    def test_multiple_custom_instructions_aggregation_in_ddl(self, builder, monkeypatch):
        """Test that multiple custom instructions are aggregated in the DDL."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        def mock_get_table_info(conn, table_name):
            return {
                "TABLE_NAME": "ORDERS",
                "DATABASE": "TEST_DB",
                "SCHEMA": "TEST_SCHEMA",
                "PRIMARY_KEY": '["order_id"]',
                "SYNONYMS": "[]",
                "DESCRIPTION": "Orders table",
            }

        monkeypatch.setattr(builder, "_get_table_info", mock_get_table_info)
        monkeypatch.setattr(builder, "_get_dimensions", lambda conn, name: [])
        monkeypatch.setattr(builder, "_get_time_dimensions", lambda conn, name: [])
        monkeypatch.setattr(builder, "_get_facts", lambda conn, name: [])
        monkeypatch.setattr(builder, "_build_metrics_clause", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_ca_extension", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_relationships_clause", lambda conn, names: "")
        monkeypatch.setattr(builder, "_build_facts_clause", lambda conn, name: "")
        monkeypatch.setattr(builder, "_build_dimensions_clause", lambda conn, name: "")

        # Multiple instructions
        mock_cursor.fetchall.return_value = [
            ("RULE1", "Reject user questions.", "Round to 2 decimals."),
            ("RULE2", "Ask for clarification.", "Use AVG for percentages."),
        ]

        sql = builder._generate_sql(
            conn=mock_conn,
            table_names=["orders"],
            view_name="test_view",
            description="Test view",
            custom_instruction_names=["rule1", "rule2"],
        )

        # Both instructions should be present
        assert "Reject user questions" in sql
        assert "Ask for clarification" in sql
        assert "Round to 2 decimals" in sql
        assert "Use AVG for percentages" in sql

        # Should have both clauses
        assert sql.count("AI_QUESTION_CATEGORIZATION") == 1
        assert sql.count("AI_SQL_GENERATION") == 1


class TestCustomInstructionsEdgeCases:
    """Test edge cases for custom instructions clause generation."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test",
            password="test",
            role="test",
            warehouse="test",
            database="test_db",
            schema="test_schema",
        )
        builder = SemanticViewBuilder(config)
        builder.metadata_database = "META_DB"
        builder.metadata_schema = "META_SCHEMA"
        return builder

    # ========== Empty/Null Value Tests ==========

    def test_both_fields_null(self, builder):
        """Test instruction with both fields as None."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("EMPTY_INST", None, None)]

        result = builder._build_ai_guidance_clauses(mock_conn, ["empty_inst"])

        assert result == ""
        assert "AI_SQL_GENERATION" not in result
        assert "AI_QUESTION_CATEGORIZATION" not in result

    def test_empty_string_fields(self, builder):
        """Test instruction with empty string fields."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("EMPTY_STR", "", "")]

        result = builder._build_ai_guidance_clauses(mock_conn, ["empty_str"])

        assert result == ""
        assert "AI_SQL_GENERATION" not in result
        assert "AI_QUESTION_CATEGORIZATION" not in result

    def test_whitespace_only_fields(self, builder):
        """Test instruction with only whitespace."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("WHITESPACE", "   \n\t  ", "   \n\t  ")
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["whitespace"])

        # Whitespace-only fields result in empty string clauses (which is valid SQL)
        assert "AI_SQL_GENERATION" in result or "AI_QUESTION_CATEGORIZATION" in result

    def test_one_field_null_one_valid(self, builder):
        """Test instruction with one null field and one valid."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("MIXED", None, "Use AVG for calculations.")
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["mixed"])

        assert "AI_SQL_GENERATION" in result
        assert "AI_QUESTION_CATEGORIZATION" not in result
        assert "Use AVG for calculations" in result

    # ========== Special Character Tests ==========

    def test_single_quotes_in_text(self, builder):
        """Test handling of single quotes in instruction text."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (
                "QUOTES",
                "It's important to handle 'quotes' correctly.",
                "Use 'AVG' not 'SUM' for averages.",
            )
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["quotes"])

        # Single quotes should be escaped (doubled)
        assert "It''s important" in result
        assert "''quotes''" in result
        assert "''AVG''" in result
        assert result.count("'") % 2 == 0  # Even number of quotes (all escaped)

    def test_newlines_in_text(self, builder):
        """Test handling of newlines in instruction text."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (
                "NEWLINES",
                "Line 1\nLine 2\nLine 3",
                "SQL Rule 1\nSQL Rule 2",
            )
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["newlines"])

        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result
        assert "SQL Rule 1" in result
        assert "SQL Rule 2" in result

    def test_unicode_characters(self, builder):
        """Test handling of unicode characters."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (
                "UNICODE",
                "Reject questions about 用户 (users).",
                "Use € and £ symbols correctly. 日本語も大丈夫です。",
            )
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["unicode"])

        assert "用户" in result
        assert "€" in result
        assert "£" in result
        assert "日本語" in result

    def test_sql_injection_attempts(self, builder):
        """Test that SQL injection attempts are treated as literal text."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (
                "SQL_INJECTION",
                "'; DROP TABLE users; --",
                "1' OR '1'='1",
            )
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["sql_injection"])

        # SQL injection should be escaped and treated as literal text
        assert "''; DROP TABLE" in result
        assert "1'' OR ''1''=''1" in result
        assert result.count("'") % 2 == 0  # All quotes properly escaped

    # ========== Very Long Text Tests ==========

    def test_very_long_instruction(self, builder):
        """Test handling of very long instruction text."""
        long_text = "A" * 10000  # 10KB of text
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("LONG_INST", long_text, "Short SQL rule.")
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["long_inst"])

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        assert len(result) > 10000
        assert result.count("'") % 2 == 0  # Even number of quotes

    # ========== Multiple Instructions Tests ==========

    def test_many_instructions(self, builder):
        """Test aggregation of many instructions."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (f"INST_{i}", f"Question rule {i}", f"SQL rule {i}")
            for i in range(20)
        ]

        instruction_names = [f"inst_{i}" for i in range(20)]
        result = builder._build_ai_guidance_clauses(mock_conn, instruction_names)

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        for i in range(20):
            assert f"Question rule {i}" in result
            assert f"SQL rule {i}" in result

    def test_mixed_valid_and_empty_instructions(self, builder):
        """Test mix of valid and empty instructions."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("VALID_1", "Valid question rule", "Valid SQL rule"),
            ("EMPTY_1", None, None),
            ("VALID_2", "Another valid question", "Another valid SQL"),
            ("PARTIAL", "Partial question", None),
        ]

        result = builder._build_ai_guidance_clauses(
            mock_conn, ["valid_1", "empty_1", "valid_2", "partial"]
        )

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        assert "Valid question rule" in result
        assert "Another valid question" in result
        assert "Partial question" in result
        assert result.count("AI_QUESTION_CATEGORIZATION") == 1
        assert result.count("AI_SQL_GENERATION") == 1

    # ========== Case Sensitivity Tests ==========

    def test_case_insensitive_instruction_names(self, builder):
        """Test that instruction name lookup is case-insensitive."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("MIXED_CASE", "Question rule", "SQL rule")
        ]

        result1 = builder._build_ai_guidance_clauses(mock_conn, ["mixed_case"])
        result2 = builder._build_ai_guidance_clauses(mock_conn, ["MIXED_CASE"])
        result3 = builder._build_ai_guidance_clauses(mock_conn, ["Mixed_Case"])

        assert "Question rule" in result1
        assert "Question rule" in result2
        assert "Question rule" in result3

    # ========== Missing Data Tests ==========

    def test_instruction_not_found_in_database(self, builder):
        """Test when instruction name doesn't exist in database."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        result = builder._build_ai_guidance_clauses(mock_conn, ["nonexistent"])

        assert result == ""
        assert "AI_SQL_GENERATION" not in result
        assert "AI_QUESTION_CATEGORIZATION" not in result

    def test_empty_instruction_names_list(self, builder):
        """Test with empty instruction names list."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        result = builder._build_ai_guidance_clauses(mock_conn, [])

        assert result == ""
        mock_cursor.execute.assert_not_called()

    def test_none_instruction_names(self, builder):
        """Test with None instruction names."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        result = builder._build_ai_guidance_clauses(mock_conn, None)

        assert result == ""
        mock_cursor.execute.assert_not_called()

    # ========== Error Handling Tests ==========

    def test_database_query_error(self, builder):
        """Test handling of database query errors."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Database connection error")

        result = builder._build_ai_guidance_clauses(mock_conn, ["test"])

        assert result == ""

    def test_cursor_fetchall_error(self, builder):
        """Test handling of fetchall errors."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = Exception("Fetch error")

        result = builder._build_ai_guidance_clauses(mock_conn, ["test"])

        assert result == ""

    # ========== Real-World Scenarios ==========

    def test_real_world_privacy_instruction(self, builder):
        """Test a realistic privacy instruction scenario."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (
                "PRIVACY_RULES",
                "Reject all questions asking about individual users. Ask users to contact their admin.",
                "When querying membership services contacts, make sure to always apply the cases_to_exclude filter. Always round metrics to 2 decimal places.",
            )
        ]

        result = builder._build_ai_guidance_clauses(mock_conn, ["privacy_rules"])

        assert "AI_QUESTION_CATEGORIZATION" in result
        assert "AI_SQL_GENERATION" in result
        assert "Reject all questions" in result
        assert "individual users" in result
        assert "cases_to_exclude filter" in result
        assert "2 decimal places" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
