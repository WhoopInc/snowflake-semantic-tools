#!/usr/bin/env python3
"""
Test intelligent truncation for embedding vectors and other long data.
"""

import unittest

from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer


class TestIntelligentTruncation(unittest.TestCase):
    """Test intelligent truncation for different data types."""

    def test_embedding_vector_truncation(self):
        """Test that embedding vectors are intelligently truncated."""
        # Create a long embedding vector (simulating real data)
        embedding_vector = "[" + ",".join([f"{i*0.001:.6f}" for i in range(1000)]) + "]"

        # Test with default max_length=500
        result = CharacterSanitizer.sanitize_for_yaml_value(embedding_vector)

        # Should be truncated and indicate it's an embedding vector
        self.assertIn("... [embedding vector truncated]", result)
        self.assertTrue(len(result) <= 500)
        self.assertTrue(result.startswith("["))

    def test_regular_text_truncation(self):
        """Test that regular text is truncated normally."""
        # Create long regular text
        long_text = "This is a very long text that should be truncated normally. " * 20

        result = CharacterSanitizer.sanitize_for_yaml_value(long_text)

        # Should be truncated with standard "..."
        self.assertIn("...", result)
        self.assertNotIn("embedding vector truncated", result)
        # Allow some extra characters for sanitization
        self.assertTrue(len(result) <= 550)  # More lenient than 500

    def test_character_sanitizer_embedding_truncation(self):
        """Test CharacterSanitizer embedding vector truncation."""
        embedding_vector = "[" + ",".join([f"{i*0.001:.6f}" for i in range(1000)]) + "]"

        result = CharacterSanitizer.sanitize_for_yaml_value(embedding_vector)

        self.assertIn("... [embedding vector truncated]", result)
        self.assertTrue(len(result) <= 500)

    def test_short_values_preserved(self):
        """Test that short values are not truncated."""

        short_text = "Short text"
        result = CharacterSanitizer.sanitize_for_yaml_value(short_text)

        self.assertEqual(result, short_text)
        self.assertNotIn("...", result)

    def test_custom_max_length(self):
        """Test custom max_length parameter."""

        long_text = "A" * 1000
        result = CharacterSanitizer.sanitize_for_yaml_value(long_text, max_length=100)

        # Allow some extra characters for sanitization
        self.assertTrue(len(result) <= 150)  # More lenient than 100
        self.assertIn("...", result)

    def test_non_embedding_array_truncation(self):
        """Test that non-embedding arrays are truncated normally."""

        # Array without decimal points (not an embedding)
        array_data = "[" + ",".join([str(i) for i in range(1000)]) + "]"

        result = CharacterSanitizer.sanitize_for_yaml_value(array_data)

        # Should be truncated normally, not as embedding
        self.assertIn("...", result)
        self.assertNotIn("embedding vector truncated", result)


if __name__ == "__main__":
    unittest.main()
