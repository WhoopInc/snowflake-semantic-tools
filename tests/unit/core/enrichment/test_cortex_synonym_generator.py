#!/usr/bin/env python3
"""
Unit Tests for CortexSynonymGenerator

Tests the Cortex-powered synonym generation functionality.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from snowflake_semantic_tools.core.enrichment.cortex_synonym_generator import CortexSynonymGenerator


@pytest.fixture
def mock_snowflake_client():
    """Create a mock Snowflake client."""
    client = Mock()
    return client


@pytest.fixture
def synonym_generator(mock_snowflake_client):
    """Create a CortexSynonymGenerator instance."""
    return CortexSynonymGenerator(snowflake_client=mock_snowflake_client, model="gpt-4o", max_synonyms=4)


class TestCortexSynonymGenerator:
    """Test suite for CortexSynonymGenerator."""

    def test_init(self, mock_snowflake_client):
        """Test initialization with default parameters."""
        generator = CortexSynonymGenerator(snowflake_client=mock_snowflake_client, model="gpt-4o", max_synonyms=4)

        assert generator.snowflake_client == mock_snowflake_client
        assert generator.model == "gpt-4o"
        assert generator.max_synonyms == 4

    def test_init_custom_model(self, mock_snowflake_client):
        """Test initialization with custom model."""
        generator = CortexSynonymGenerator(
            snowflake_client=mock_snowflake_client, model="mistral-large", max_synonyms=3
        )

        assert generator.model == "mistral-large"
        assert generator.max_synonyms == 3

    def test_generate_table_synonyms_skips_existing(self, synonym_generator):
        """Test that existing synonyms are preserved."""
        existing = ["existing synonym", "another one"]

        result = synonym_generator.generate_table_synonyms(
            table_name="test_table", description="Test description", column_info=[], existing_synonyms=existing
        )

        assert result == existing

    def test_generate_table_synonyms_success(self, synonym_generator, mock_snowflake_client):
        """Test successful synonym generation with structured output."""
        # Mock Cortex structured response
        mock_df = pd.DataFrame(
            {"RESPONSE": ['{"synonyms": ["member analytics", "subscription data", "user metrics"]}']}
        )
        mock_snowflake_client.execute_query.return_value = mock_df

        result = synonym_generator.generate_table_synonyms(
            table_name="churn_details",
            description="Member churn analysis",
            column_info=[
                {"name": "user_id", "description": "User identifier"},
                {"name": "churn_date", "description": "Date of churn"},
            ],
        )

        assert len(result) == 3
        assert "member analytics" in result
        assert "subscription data" in result
        assert "user metrics" in result

    def test_cortex_invalid_json_returns_empty(self, synonym_generator, mock_snowflake_client):
        """Test that invalid JSON returns empty list (no fallback)."""
        # Mock invalid JSON response (shouldn't happen with structured outputs)
        mock_df = pd.DataFrame({"RESPONSE": ["Invalid response"]})
        mock_snowflake_client.execute_query.return_value = mock_df

        result = synonym_generator.generate_table_synonyms(table_name="test_table", description="Test", column_info=[])

        # Should return empty (no fallback)
        assert result == []

    def test_cortex_empty_response_raises_error(self, synonym_generator, mock_snowflake_client):
        """Test that empty Cortex response raises RuntimeError (fail loudly)."""
        mock_df = pd.DataFrame()
        mock_snowflake_client.execute_query.return_value = mock_df

        with pytest.raises(RuntimeError) as exc_info:
            synonym_generator.generate_table_synonyms(table_name="test_table", description="Test", column_info=[])

        # Should fail loudly with clear error
        assert "empty response" in str(exc_info.value).lower()

    def test_cortex_exception_raises_error(self, synonym_generator, mock_snowflake_client):
        """Test that Cortex exceptions are raised (fail loudly)."""
        mock_snowflake_client.execute_query.side_effect = Exception("Cortex error")

        with pytest.raises(RuntimeError) as exc_info:
            synonym_generator.generate_table_synonyms(table_name="error_table", description="Test", column_info=[])

        # Should fail loudly
        assert "error_table" in str(exc_info.value) or "Cortex error" in str(exc_info.value)

    def test_build_table_synonym_prompt(self, synonym_generator):
        """Test table synonym prompt construction."""
        column_info = [
            {"name": "user_id", "description": "User ID", "sample_values": [1, 2, 3]},
            {"name": "date", "description": "Date", "sample_values": ["2024-01-01"]},
        ]

        prompt = synonym_generator._build_table_synonym_prompt(
            "churn_details", "Member churn tracking", synonym_generator._build_column_context(column_info)
        )

        assert "churn_details" in prompt
        assert "Member churn tracking" in prompt
        assert "user_id" in prompt
        assert "synonyms" in prompt  # References the array in structured output

    def test_build_batch_column_synonym_prompt(self, synonym_generator):
        """Test batch column synonym prompt construction."""
        columns = [
            {
                "name": "user_id",
                "description": "User ID",
                "meta": {"sst": {"data_type": "NUMBER", "sample_values": [1, 2, 3]}},
            },
            {
                "name": "email",
                "description": "Email address",
                "meta": {"sst": {"data_type": "VARCHAR", "sample_values": ["a@b.com"]}},
            },
        ]

        prompt = synonym_generator._build_batch_column_synonym_prompt("users", columns, "User table")

        assert "user_id" in prompt
        assert "email" in prompt
        assert "JSON object" in prompt
        assert "EACH column" in prompt

    def test_synonym_count_limit(self, synonym_generator, mock_snowflake_client):
        """Test that synonyms are limited to max_synonyms via JSON schema."""
        # Mock structured response - schema should enforce max
        mock_df = pd.DataFrame({"RESPONSE": ['{"synonyms": ["syn1", "syn2", "syn3", "syn4"]}']})
        mock_snowflake_client.execute_query.return_value = mock_df

        result = synonym_generator.generate_table_synonyms(table_name="test", description="Test", column_info=[])

        # Should be limited to max_synonyms (4)
        assert len(result) <= 4

    def test_sql_escaping(self, synonym_generator, mock_snowflake_client):
        """Test that single quotes are properly escaped in prompts."""
        mock_df = pd.DataFrame({"RESPONSE": ['["test"]']})
        mock_snowflake_client.execute_query.return_value = mock_df

        # Description with single quotes
        synonym_generator.generate_table_synonyms(
            table_name="test", description="User's membership data", column_info=[]
        )

        # Check that query was called with escaped quotes
        called_query = mock_snowflake_client.execute_query.call_args[0][0]
        assert "''" in called_query  # Should have escaped quotes
        assert "User''s" in called_query

    def test_batch_column_synonyms_success(self, synonym_generator, mock_snowflake_client):
        """Test batch column synonym generation."""
        # Mock batch response
        mock_df = pd.DataFrame(
            {"RESPONSE": ['{"col1": ["synonym 1", "synonym 2"], "col2": ["synonym a", "synonym b"]}']}
        )
        mock_snowflake_client.execute_query.return_value = mock_df

        columns = [
            {"name": "col1", "description": "Column 1", "meta": {"sst": {"data_type": "VARCHAR"}}},
            {"name": "col2", "description": "Column 2", "meta": {"sst": {"data_type": "NUMBER"}}},
        ]

        result = synonym_generator.generate_column_synonyms_batch(table_name="test_table", columns=columns)

        assert "col1" in result
        assert "col2" in result
        assert len(result["col1"]) == 2
        assert len(result["col2"]) == 2
        assert "synonym 1" in result["col1"]

    def test_batch_column_synonyms_markdown_fence(self, synonym_generator, mock_snowflake_client):
        """Test that markdown code fences are stripped from Cortex response."""
        # Mock response with markdown code fence (common Cortex behavior)
        mock_df = pd.DataFrame({"RESPONSE": ['```json\n{"col1": ["synonym 1"], "col2": ["synonym 2"]}\n```']})
        mock_snowflake_client.execute_query.return_value = mock_df

        columns = [
            {"name": "col1", "description": "Column 1", "meta": {"sst": {"data_type": "VARCHAR"}}},
            {"name": "col2", "description": "Column 2", "meta": {"sst": {"data_type": "NUMBER"}}},
        ]

        result = synonym_generator.generate_column_synonyms_batch(table_name="test_table", columns=columns)

        # Should successfully parse despite markdown fence
        assert "col1" in result
        assert "col2" in result
        assert result["col1"] == ["synonym 1"]
        assert result["col2"] == ["synonym 2"]

    def test_extract_json_with_preamble_text(self, synonym_generator):
        """Test JSON extraction when LLM adds preamble text."""
        response = 'Here is the JSON you requested:\n\n{"col1": ["syn1"]}'
        result = synonym_generator._extract_json_from_response(response)
        assert result == '{"col1": ["syn1"]}'

    def test_extract_json_with_trailing_text(self, synonym_generator):
        """Test JSON extraction when LLM adds trailing explanation."""
        response = '{"col1": ["syn1"]}\n\nI hope this helps!'
        result = synonym_generator._extract_json_from_response(response)
        assert result == '{"col1": ["syn1"]}'

    def test_extract_json_handles_array_response(self, synonym_generator):
        """Test JSON extraction for array responses."""
        response = '["synonym1", "synonym2", "synonym3"]'
        result = synonym_generator._extract_json_from_response(response)
        assert result == '["synonym1", "synonym2", "synonym3"]'

    def test_extract_json_empty_response(self, synonym_generator):
        """Test JSON extraction handles empty response."""
        assert synonym_generator._extract_json_from_response("") == ""
        assert synonym_generator._extract_json_from_response(None) == ""


class TestCortexVerification:
    """Test Cortex access verification and error handling."""

    @pytest.fixture
    def mock_snowflake_client(self):
        """Create a mock Snowflake client."""
        client = Mock()
        return client

    def test_verify_cortex_access_success(self, mock_snowflake_client):
        """Test successful Cortex verification."""
        mock_df = pd.DataFrame({"RESPONSE": ["Hello!"]})
        mock_snowflake_client.execute_query.return_value = mock_df

        generator = CortexSynonymGenerator(mock_snowflake_client)
        assert generator._cortex_verified is False

        # Trigger verification via _execute_cortex
        generator._execute_cortex("test prompt")

        assert generator._cortex_verified is True

    def test_verify_cortex_access_permission_error(self, mock_snowflake_client):
        """Test Cortex permission error provides helpful message."""
        mock_snowflake_client.execute_query.side_effect = Exception("access denied to function")

        generator = CortexSynonymGenerator(mock_snowflake_client)

        with pytest.raises(RuntimeError) as exc_info:
            generator._verify_cortex_access()

        error_msg = str(exc_info.value)
        assert "permission error" in error_msg.lower()
        assert "CORTEX_USER" in error_msg

    def test_verify_cortex_access_model_not_found(self, mock_snowflake_client):
        """Test Cortex model not found error provides helpful message."""
        mock_snowflake_client.execute_query.side_effect = Exception("model not found")

        generator = CortexSynonymGenerator(mock_snowflake_client)

        with pytest.raises(RuntimeError) as exc_info:
            generator._verify_cortex_access()

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()
        assert "mistral-large2" in error_msg  # Default model
        assert "docs.snowflake.com" in error_msg  # Link to availability docs

    def test_verify_cortex_access_generic_error(self, mock_snowflake_client):
        """Test Cortex generic error provides connection guidance."""
        mock_snowflake_client.execute_query.side_effect = Exception("some other error")

        generator = CortexSynonymGenerator(mock_snowflake_client)

        with pytest.raises(RuntimeError) as exc_info:
            generator._verify_cortex_access()

        error_msg = str(exc_info.value)
        assert "connection failed" in error_msg.lower()

    def test_verify_cortex_access_only_called_once(self, mock_snowflake_client):
        """Test Cortex verification is only performed once."""
        mock_df = pd.DataFrame({"RESPONSE": ["Hello!"]})
        mock_snowflake_client.execute_query.return_value = mock_df

        generator = CortexSynonymGenerator(mock_snowflake_client)
        generator._verify_cortex_access()
        generator._verify_cortex_access()
        generator._verify_cortex_access()

        # Should only have called execute_query once for verification
        # (subsequent calls skip because _cortex_verified is True)
        assert mock_snowflake_client.execute_query.call_count == 1

    def test_generate_synonyms_raises_on_cortex_error(self, mock_snowflake_client):
        """Test that synonym generation fails loudly on Cortex error."""
        mock_snowflake_client.execute_query.side_effect = Exception("access denied")

        generator = CortexSynonymGenerator(mock_snowflake_client)

        # Should raise RuntimeError, not silently return empty
        with pytest.raises(RuntimeError) as exc_info:
            generator.generate_table_synonyms(table_name="test", description="Test table", column_info=[])

        # Error should contain helpful information
        error_msg = str(exc_info.value)
        assert "permission error" in error_msg.lower() or "access" in error_msg.lower()

    def test_verify_cortex_model_unavailable_error(self, mock_snowflake_client):
        """Test that 'unavailable' error provides helpful cross-region guidance."""
        mock_snowflake_client.execute_query.side_effect = Exception('Model "openai-gpt-4.1" is unavailable')

        # Test with an OpenAI model to trigger cross-region guidance
        generator = CortexSynonymGenerator(mock_snowflake_client, model="openai-gpt-4.1")

        with pytest.raises(RuntimeError) as exc_info:
            generator._verify_cortex_access()

        error_msg = str(exc_info.value)
        # Should include model name
        assert "openai-gpt-4.1" in error_msg
        # Should include cross-region inference guidance for OpenAI models
        assert "cross-region" in error_msg.lower()
        assert "CORTEX_ENABLED_CROSS_REGION" in error_msg
        # Should suggest universally available models
        assert "mistral-large2" in error_msg
        # Should include link to availability docs
        assert "docs.snowflake.com" in error_msg
        assert "availability" in error_msg.lower()

    def test_verify_cortex_non_openai_model_unavailable(self, mock_snowflake_client):
        """Test that non-OpenAI model unavailable error doesn't mention cross-region."""
        mock_snowflake_client.execute_query.side_effect = Exception('Model "some-custom-model" is unavailable')

        generator = CortexSynonymGenerator(mock_snowflake_client, model="some-custom-model")

        with pytest.raises(RuntimeError) as exc_info:
            generator._verify_cortex_access()

        error_msg = str(exc_info.value)
        # Should include model name
        assert "some-custom-model" in error_msg
        # Should NOT include cross-region guidance (not an OpenAI model)
        assert "CORTEX_ENABLED_CROSS_REGION" not in error_msg
        # Should still suggest universally available models
        assert "mistral-large2" in error_msg

    def test_default_model_is_mistral_large2(self, mock_snowflake_client):
        """Test that the default model is mistral-large2 (universally available)."""
        generator = CortexSynonymGenerator(mock_snowflake_client)
        assert generator.model == "mistral-large2"

    def test_build_model_unavailable_error_openai_model(self, mock_snowflake_client):
        """Test _build_model_unavailable_error for OpenAI models includes cross-region info."""
        generator = CortexSynonymGenerator(mock_snowflake_client, model="gpt-4.1")

        error_msg = generator._build_model_unavailable_error("test error")

        assert "gpt-4.1" in error_msg
        assert "cross-region" in error_msg.lower()
        assert "AZURE_US" in error_msg

    def test_build_model_unavailable_error_non_openai_model(self, mock_snowflake_client):
        """Test _build_model_unavailable_error for non-OpenAI models excludes cross-region info."""
        generator = CortexSynonymGenerator(mock_snowflake_client, model="llama3.1-70b")

        error_msg = generator._build_model_unavailable_error("test error")

        assert "llama3.1-70b" in error_msg
        # Should not include cross-region for non-OpenAI models
        assert "CORTEX_ENABLED_CROSS_REGION" not in error_msg
        # But should still suggest alternatives
        assert "mistral-large2" in error_msg


class TestAvoidSynonyms:
    """Test avoid_synonyms parameter for duplicate prevention."""

    @pytest.fixture
    def mock_snowflake_client(self):
        """Create a mock Snowflake client."""
        client = Mock()
        return client

    @pytest.fixture
    def synonym_generator(self, mock_snowflake_client):
        """Create a CortexSynonymGenerator instance."""
        return CortexSynonymGenerator(snowflake_client=mock_snowflake_client, model="mistral-large2", max_synonyms=4)

    def test_avoid_synonyms_included_in_prompt(self, synonym_generator, mock_snowflake_client):
        """Test that avoid_synonyms list is included in the prompt."""
        mock_df = pd.DataFrame({"RESPONSE": ['["new synonym 1", "new synonym 2"]']})
        mock_snowflake_client.execute_query.return_value = mock_df

        synonym_generator.generate_table_synonyms(
            table_name="test_table",
            description="Test description",
            column_info=[],
            avoid_synonyms=["order details", "purchase records"],
        )

        # Check that the query was called with a prompt containing the avoid list
        called_query = mock_snowflake_client.execute_query.call_args[0][0]
        assert "AVOID THESE SYNONYMS" in called_query
        assert "order details" in called_query
        assert "purchase records" in called_query

    def test_avoid_synonyms_empty_list_no_section(self, synonym_generator, mock_snowflake_client):
        """Test that empty avoid_synonyms doesn't add AVOID section to prompt."""
        mock_df = pd.DataFrame({"RESPONSE": ['["synonym 1"]']})
        mock_snowflake_client.execute_query.return_value = mock_df

        synonym_generator.generate_table_synonyms(
            table_name="test_table",
            description="Test description",
            column_info=[],
            avoid_synonyms=[],
        )

        # Check that the query was called without AVOID section
        called_query = mock_snowflake_client.execute_query.call_args[0][0]
        assert "AVOID THESE SYNONYMS" not in called_query

    def test_post_generation_filter_removes_duplicates(self, synonym_generator, mock_snowflake_client):
        """Test that post-generation filter removes synonyms from avoid list."""
        # Simulate LLM returning a synonym that's in the avoid list
        mock_df = pd.DataFrame({"RESPONSE": ['["order details", "new unique synonym", "purchase records"]']})
        mock_snowflake_client.execute_query.return_value = mock_df

        result = synonym_generator.generate_table_synonyms(
            table_name="test_table",
            description="Test description",
            column_info=[],
            avoid_synonyms=["order details", "purchase records"],  # These should be filtered
        )

        # Only "new unique synonym" should remain
        assert "new unique synonym" in result
        assert "order details" not in result
        assert "purchase records" not in result
        assert len(result) == 1

    def test_post_generation_filter_case_insensitive(self, synonym_generator, mock_snowflake_client):
        """Test that post-generation filter is case-insensitive."""
        mock_df = pd.DataFrame({"RESPONSE": ['["Order Details", "new synonym"]']})
        mock_snowflake_client.execute_query.return_value = mock_df

        result = synonym_generator.generate_table_synonyms(
            table_name="test_table",
            description="Test description",
            column_info=[],
            avoid_synonyms=["order details"],  # Lowercase
        )

        # "Order Details" should be filtered despite case difference
        assert "new synonym" in result
        assert len(result) == 1

    def test_none_avoid_synonyms_works(self, synonym_generator, mock_snowflake_client):
        """Test that None avoid_synonyms doesn't cause errors."""
        mock_df = pd.DataFrame({"RESPONSE": ['["synonym 1", "synonym 2"]']})
        mock_snowflake_client.execute_query.return_value = mock_df

        result = synonym_generator.generate_table_synonyms(
            table_name="test_table",
            description="Test description",
            column_info=[],
            avoid_synonyms=None,  # Explicitly None
        )

        assert len(result) == 2
        assert "synonym 1" in result
        assert "synonym 2" in result
