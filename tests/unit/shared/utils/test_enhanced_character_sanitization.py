#!/usr/bin/env python3
"""
Test enhanced character sanitization for control characters and Unicode escape sequences.
"""

import unittest

from snowflake_semantic_tools.infrastructure.snowflake.metadata_manager import MetadataManager
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer


class TestEnhancedCharacterSanitization(unittest.TestCase):
    """Test enhanced sanitization for control characters and Unicode escapes."""

    def test_control_characters_removal(self):
        """Test that all control characters (0-31) except tab, newline, carriage return are removed."""
        # Test various control characters
        test_cases = [
            ("test\x00value", "testvalue"),  # NUL
            ("test\x01value", "testvalue"),  # SOH
            ("test\x02value", "testvalue"),  # STX
            ("test\x07value", "testvalue"),  # BEL
            ("test\x08value", "testvalue"),  # BS
            ("test\x0Bvalue", "testvalue"),  # VT
            ("test\x0Cvalue", "testvalue"),  # FF
            ("test\x0Evalue", "testvalue"),  # SO
            ("test\x1Fvalue", "testvalue"),  # US
            ("test\x1F\x00\x01value", "testvalue"),  # Multiple control chars
        ]

        for input_val, expected in test_cases:
            with self.subTest(input_val=repr(input_val)):
                result = CharacterSanitizer.sanitize_for_yaml_value(input_val)
                self.assertEqual(result, expected)
                # Verify no control characters remain
                for char in result:
                    self.assertTrue(
                        ord(char) >= 32 or char in "\t\n\r", f"Control character {repr(char)} found in result"
                    )

    def test_unicode_escape_sequences_removal(self):
        """Test that Unicode escape sequences are removed."""
        test_cases = [
            ("test\\u0041value", "testvalue"),  # \u0041 (A)
            ("test\\u0042value", "testvalue"),  # \u0042 (B)
            ("test\\x41value", "testvalue"),  # \x41 (A)
            ("test\\x42value", "testvalue"),  # \x42 (B)
            ("test\\u0041\\x42value", "testvalue"),  # Multiple escapes
            ("test\\u004A\\x4Avalue", "testvalue"),  # Different cases
            ("test\\u004avalue", "testvalue"),  # Lowercase hex
            ("test\\x4avalue", "testvalue"),  # Lowercase hex
        ]

        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = CharacterSanitizer.sanitize_for_yaml_value(input_val)
                self.assertEqual(result, expected)
                # Verify no escape sequences remain
                self.assertNotIn("\\u", result)
                self.assertNotIn("\\x", result)

    def test_character_sanitizer_unicode_escapes(self):
        """Test CharacterSanitizer Unicode escape removal."""
        test_cases = [
            ("test\\u0041value", "testvalue"),
            ("test\\x41value", "testvalue"),
            ("test\\u0041\\x42value", "testvalue"),
        ]

        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = CharacterSanitizer.sanitize_for_yaml_value(input_val)
                self.assertEqual(result, expected)
                self.assertNotIn("\\u", result)
                self.assertNotIn("\\x", result)

    def test_preserve_valid_characters(self):
        """Test that valid characters are preserved."""
        # Test that tab, newline, carriage return are preserved
        test_cases = [
            "test\tvalue",
            "test\nvalue",
            "test\rvalue",
            "test\t\n\rvalue",
        ]

        for input_val in test_cases:
            with self.subTest(input_val=repr(input_val)):
                result = CharacterSanitizer.sanitize_for_yaml_value(input_val)
                self.assertEqual(result, input_val)

    def test_combined_problematic_characters(self):
        """Test handling of combined problematic characters."""
        # Test string with control chars, Unicode escapes, and valid chars
        input_val = "test\x00\\u0041\\x42\t\n\rvalue"
        result = CharacterSanitizer.sanitize_for_yaml_value(input_val)
        expected = "test\t\n\rvalue"  # Only valid chars should remain

        self.assertEqual(result, expected)

        # Verify no problematic characters remain
        self.assertNotIn("\x00", result)
        self.assertNotIn("\\u", result)
        self.assertNotIn("\\x", result)
        self.assertIn("\t", result)
        self.assertIn("\n", result)
        self.assertIn("\r", result)

    def test_empty_and_edge_cases(self):
        """Test edge cases."""
        # Empty string
        self.assertEqual(CharacterSanitizer.sanitize_for_yaml_value(""), "")

        # String with only control characters
        self.assertEqual(CharacterSanitizer.sanitize_for_yaml_value("\x00\x01\x02"), "")

        # String with only Unicode escapes
        self.assertEqual(CharacterSanitizer.sanitize_for_yaml_value("\\u0041\\x42"), "")

        # String with only valid characters
        self.assertEqual(CharacterSanitizer.sanitize_for_yaml_value("hello world"), "hello world")


if __name__ == "__main__":
    unittest.main()
