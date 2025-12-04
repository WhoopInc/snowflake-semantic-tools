#!/usr/bin/env python3
"""
Test NUL character sanitization in sample values.

This test verifies that NUL characters (\x00) and other control characters
are properly filtered out from sample values to prevent parsing errors
in downstream tools like Hightouch.
"""

import unittest
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer


class TestNULCharacterSanitization(unittest.TestCase):
    """Test NUL character sanitization."""

    def test_metadata_manager_sanitize_value_nul_characters(self):
        """Test that MetadataManager zeros NUL characters (uses CharacterSanitizer)."""
        # Test NUL character (\x00)
        test_value = "test\x00value"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "testvalue")
        self.assertNotIn('\x00', sanitized)
        
        # Test multiple NUL characters
        test_value = "test\x00\x00value\x00"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "testvalue")
        self.assertNotIn('\x00', sanitized)
        
        # Test NUL with other control characters
        test_value = "test\x01\x02\x00value"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "testvalue")
        self.assertNotIn('\x00', sanitized)
        self.assertNotIn('\x01', sanitized)
        self.assertNotIn('\x02', sanitized)
        
        # Test that valid characters are preserved
        test_value = "test\t\n\rvalue"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "test\t\n\rvalue")
        
        # Test empty string
        sanitized = CharacterSanitizer.sanitize_for_yaml_value("")
        self.assertEqual(sanitized, "")
        
        # Test string with only control characters
        test_value = "\x00\x01\x02"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "")

    def test_character_sanitizer_nul_characters(self):
        """Test that CharacterSanitizer removes NUL characters."""
        # Test NUL character (\x00)
        test_value = "test\x00value"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "testvalue")
        self.assertNotIn('\x00', sanitized)
        
        # Test multiple NUL characters
        test_value = "test\x00\x00value\x00"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "testvalue")
        self.assertNotIn('\x00', sanitized)
        
        # Test NUL with other control characters
        test_value = "test\x01\x02\x00value"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "testvalue")
        self.assertNotIn('\x00', sanitized)
        self.assertNotIn('\x01', sanitized)
        self.assertNotIn('\x02', sanitized)
        
        # Test that valid characters are preserved
        test_value = "test\t\n\rvalue"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "test\t\n\rvalue")
        
        # Test empty string
        sanitized = CharacterSanitizer.sanitize_for_yaml_value("")
        self.assertEqual(sanitized, "")
        
        # Test string with only control characters
        test_value = "\x00\x01\x02"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "")

    def test_control_character_filtering(self):
        """Test filtering of various control characters."""
        
        # Test all control characters (0-31 except \t, \n, \r)
        control_chars = ''.join(chr(i) for i in range(32) if chr(i) not in '\t\n\r')
        test_value = f"test{control_chars}value"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        
        # Should only contain the test and value parts
        self.assertEqual(sanitized, "testvalue")
        
        # Verify no control characters remain
        for char in control_chars:
            self.assertNotIn(char, sanitized)

    def test_preserve_valid_characters(self):
        """Test that valid characters are preserved."""
        
        # Test printable ASCII characters
        test_value = "Hello, World! 123 @#$%^&*()_+-=[]{}|;':\",./<>?"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, test_value)
        
        # Test Unicode characters
        test_value = "Hello 世界 🌍"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, test_value)
        
        # Test whitespace characters that should be preserved
        test_value = "test\t\n\rvalue"
        sanitized = CharacterSanitizer.sanitize_for_yaml_value(test_value)
        self.assertEqual(sanitized, "test\t\n\rvalue")


if __name__ == '__main__':
    unittest.main()
