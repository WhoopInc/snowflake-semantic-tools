"""
Unit tests for sample value retrieval.

Tests the get_sample_values method to ensure:
- No ORDER BY for better diversity
- Nulls are filtered in Python
- Correct limit is applied after filtering
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from snowflake_semantic_tools.infrastructure.snowflake.metadata_manager import MetadataManager


@pytest.fixture
def mock_snowflake_client():
    """Create a mock SnowflakeClient."""
    client = Mock(spec=SnowflakeClient)
    return client


def test_get_sample_values_filters_nulls():
    """Test that null values are filtered out in Python."""
    # Create mock MetadataManager
    manager = MetadataManager.__new__(MetadataManager)
    
    # Mock execute_query to return data with nulls
    mock_df = pd.DataFrame({
        'STATUS': ['active', None, 'inactive', 'pending', None, 'archived']
    })
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('users', 'public', 'status', 'analytics', limit=10)
    
    # Should filter out the 2 None values
    assert len(result) == 4
    assert 'active' in result
    assert 'inactive' in result
    assert 'pending' in result
    assert 'archived' in result
    assert 'None' not in result


def test_get_sample_values_respects_limit_after_filtering():
    """Test that limit is applied after filtering nulls."""
    manager = MetadataManager.__new__(MetadataManager)
    
    # Mock execute_query to return 6 values (1 null + 5 non-null)
    # With limit=3, should fetch 4 values (limit+1)
    mock_df = pd.DataFrame({
        'STATUS': ['active', None, 'inactive', 'pending']
    })
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('users', 'public', 'status', 'analytics', limit=3)
    
    # Should return exactly 3 non-null values
    assert len(result) == 3
    assert 'active' in result
    assert 'inactive' in result
    assert 'pending' in result


def test_get_sample_values_empty_dataframe():
    """Test handling of empty dataframe."""
    manager = MetadataManager.__new__(MetadataManager)
    
    mock_df = pd.DataFrame()
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('table', 'schema', 'col', 'db', limit=10)
    
    assert result == []


def test_get_sample_values_all_nulls():
    """Test handling when all values are null."""
    manager = MetadataManager.__new__(MetadataManager)
    
    mock_df = pd.DataFrame({'COL': [None, None, None]})
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('table', 'schema', 'col', 'db', limit=10)
    
    assert result == []


def test_get_sample_values_filters_empty_strings():
    """Test that empty strings are also filtered out."""
    manager = MetadataManager.__new__(MetadataManager)
    
    mock_df = pd.DataFrame({
        'STATUS': ['active', '', '   ', 'inactive', None]
    })
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('users', 'public', 'status', 'analytics', limit=10)
    
    # Should filter out empty string, whitespace, and None
    assert len(result) == 2
    assert 'active' in result
    assert 'inactive' in result
    assert '' not in result
    assert '   ' not in result


def test_get_sample_values_truncates_long_values():
    """Test that values exceeding 1000 characters are truncated."""
    manager = MetadataManager.__new__(MetadataManager)
    
    # Create a very long string (base64-like data)
    long_value = 'a' * 1500  # 1500 characters
    short_value = 'short'
    
    mock_df = pd.DataFrame({
        'DATA': [long_value, short_value]
    })
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('table', 'schema', 'data', 'db', limit=10)
    
    # Long value should be truncated (metadata manager uses max_length=500)
    assert len(result) == 2
    assert len(result[0]) == 503  # 500 + '...'
    assert result[0].endswith('...')
    assert result[1] == 'short'


def test_get_sample_values_sanitizes_jinja_characters():
    """Test that Jinja template characters are sanitized to prevent dbt errors."""
    manager = MetadataManager.__new__(MetadataManager)
    
    # Create values with Jinja template characters that break dbt compilation
    jinja_values = [
        '{{@aiql query data}}',
        '{%- set var = value -%}',
        '{# this is a comment #}',
        '{{{message}}}',  # Handlebars triple braces
        'normal value'
    ]
    
    mock_df = pd.DataFrame({'DATA': jinja_values})
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('table', 'schema', 'data', 'db', limit=10)
    
    # Jinja characters should be sanitized with spaces
    assert len(result) == 5
    assert result[0] == '{ {@aiql query data} }'
    assert result[1] == '{ %- set var = value -% }'
    assert result[2] == '{ # this is a comment # }'
    assert result[3] == '{ {{message} }}'  # Triple braces: outer braces separated
    assert result[4] == 'normal value'


def test_get_sample_values_sanitizes_yaml_breaking_characters():
    """Test that YAML-breaking characters at the start of values are sanitized."""
    manager = MetadataManager.__new__(MetadataManager)
    
    # Create values that break YAML parsing when at the start of unquoted list items
    yaml_breaking_values = [
        '>redirect output',
        '|pipe character',
        '&anchor reference',
        '*alias reference',
        '@mention user',
        '`backtick code`',
        'normal value'
    ]
    
    mock_df = pd.DataFrame({'DATA': yaml_breaking_values})
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('table', 'schema', 'data', 'db', limit=10)
    
    # Values starting with YAML-breaking characters should have a space prepended
    assert len(result) == 7
    assert result[0] == ' >redirect output'  # Space added to prevent block scalar
    assert result[1] == ' |pipe character'   # Space added to prevent literal block
    assert result[2] == ' &anchor reference' # Space added to prevent anchor
    assert result[3] == ' *alias reference'  # Space added to prevent alias
    assert result[4] == ' @mention user'     # Space added to prevent parsing error
    assert result[5] == ' `backtick code`'   # Space added to prevent backtick error
    assert result[6] == 'normal value'       # No change


def test_get_sample_values_combined_sanitization():
    """Test that both Jinja and YAML sanitization work together."""
    manager = MetadataManager.__new__(MetadataManager)
    
    # Test values with both types of problematic characters
    combined_values = [
        '{{#assign "cta"}}https://example.com',  # Jinja in middle
        '>{{ variable }}',                        # YAML char at start + Jinja
        '@user {{name}}',                         # Both @ and Jinja
        'normal value'
    ]
    
    mock_df = pd.DataFrame({'DATA': combined_values})
    
    with patch.object(manager, '_execute_query', return_value=mock_df):
        result = manager.get_sample_values('table', 'schema', 'data', 'db', limit=10)
    
    # Both sanitizations should be applied
    assert len(result) == 4
    assert result[0] == '{ { #assign "cta"} }https://example.com'  # {{ and {# both sanitized
    assert result[1] == ' >{ { variable } }'                       # Space + Jinja sanitized
    assert result[2] == ' @user { {name} }'                        # Space + Jinja sanitized
    assert result[3] == 'normal value'
