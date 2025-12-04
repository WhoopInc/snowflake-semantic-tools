"""
Test backslash escaping in sample values to prevent Unicode escape sequence errors.
"""

import pytest
from snowflake_semantic_tools.infrastructure.snowflake.metadata_manager import MetadataManager
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer


class MockConnectionManager:
    """Mock connection manager for testing."""
    pass


class MockConfig:
    """Mock config for testing."""
    pass


def test_sanitize_value_escapes_backslashes():
    """Test that backslashes are properly escaped in sample values."""
    # Create metadata manager instance
    manager = MetadataManager(MockConnectionManager(), MockConfig())
    
    # Test cases with backslash sequences that cause parsing errors
    # Using raw strings for clarity - sanitizer doubles backslashes
    test_cases = [
        # Newline sequences (backslashes doubled)
        (r"Hello\nWorld", r"Hello\\nWorld"),
        # Tab sequences (backslashes doubled)
        (r"Column1\tColumn2", r"Column1\\tColumn2"),
        # Path-like sequences
        (r"\user\path", r"\\user\\path"),
        # Unicode BOM sequence - REMOVED entirely by sanitizer (prevents Unicode errors)
        (r"\uFEFF text", " text"),  # \uFEFF is removed!
        # Windows paths
        (r"C:\Users\file.txt", r"C:\\Users\\file.txt"),
        # Backslash at end
        (r"path\end", r"path\\end"),
        # Mixed actual newlines (preserved as-is)
        ("Line1\nLine2\nLine3", "Line1\nLine2\nLine3"),
    ]
    
    for input_val, expected_output in test_cases:
        result = CharacterSanitizer.sanitize_for_yaml_value(input_val)
        assert result == expected_output, (
            f"Failed to escape backslashes correctly.\n"
            f"Input: {repr(input_val)}\n"
            f"Expected: {repr(expected_output)}\n"
            f"Got: {repr(result)}"
        )


def test_sanitize_value_preserves_other_sanitization():
    """Test that backslash escaping doesn't break other sanitization."""
    # Use CharacterSanitizer directly (the canonical sanitization method)
    
    # Test Jinja character sanitization still works
    result = CharacterSanitizer.sanitize_for_yaml_value("{{ variable }}")
    assert result == "{ { variable } }"
    
    # Test combined: backslashes + Jinja
    result = CharacterSanitizer.sanitize_for_yaml_value("Path: C:\\Users\\{{ name }}")
    assert result == "Path: C:\\\\Users\\\\{ { name } }"
    
    # Test YAML-breaking start characters
    result = CharacterSanitizer.sanitize_for_yaml_value(">Greater than")
    assert result.startswith(" ")
    
    # Test truncation still works
    long_string = "A" * 1500
    result = CharacterSanitizer.sanitize_for_yaml_value(long_string, max_length=1000)
    assert len(result) <= 1003  # 1000 + "..."
    assert result.endswith("...")


def test_sanitize_value_real_world_examples():
    """Test with real-world problematic values from analytics-dbt."""
    # Use CharacterSanitizer directly (canonical sanitization)
    
    # Windows path with raw backslashes
    path = r"C:\Users\Documents\file.txt"
    result = CharacterSanitizer.sanitize_for_yaml_value(path)
    assert result == r"C:\\Users\\Documents\\file.txt"
    
    # Regex pattern
    regex = r"Match: \d+"
    result = CharacterSanitizer.sanitize_for_yaml_value(regex)
    assert result == r"Match: \\d+"
    
    # String with actual newlines (not escape sequences)
    multiline = "Line 1\nLine 2\nLine 3"
    result = CharacterSanitizer.sanitize_for_yaml_value(multiline)
    assert result == "Line 1\nLine 2\nLine 3"  # Actual newlines preserved


def test_backslash_in_yaml_context():
    """Test that escaped backslashes work in YAML context."""
    # Use CharacterSanitizer directly
    
    # Simulate values that would be written to YAML (using raw strings)
    test_cases = [
        (r"User typed: \n for newline", r"User typed: \\n for newline"),
        (r"Regex pattern: \d+", r"Regex pattern: \\d+"),
        (r"Windows path: C:\Program Files\App", r"Windows path: C:\\Program Files\\App"),
    ]
    
    for input_val, expected in test_cases:
        result = CharacterSanitizer.sanitize_for_yaml_value(input_val)
        assert result == expected, (
            f"Backslash escaping failed.\n"
            f"Input: {repr(input_val)}\n"
            f"Expected: {repr(expected)}\n"
            f"Got: {repr(result)}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

