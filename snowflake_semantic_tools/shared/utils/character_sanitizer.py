"""
Comprehensive character sanitization for SQL, YAML, and Jinja contexts.

This module provides unified sanitization for all contexts where metadata
values might be embedded in generated SQL or YAML files.
"""

import re
from typing import List, Optional


class CharacterSanitizer:
    """
    Comprehensive character sanitization for metadata values.

    Handles sanitization for different contexts:
    - SQL strings (COMMENT clauses, WITH SYNONYMS)
    - YAML values (sample_values, descriptions)
    - Jinja templates (prevents dbt compilation errors)
    """

    # Characters that break SQL string literals
    SQL_BREAKING_CHARS = {
        "'": "",  # Single quotes (remove entirely)
        "'": "",  # Smart quote left
        "'": "",  # Smart quote right
        '"': "",  # Double quotes (remove entirely)
        "\\": "\\\\",  # Backslashes (escape for SQL)
        # Note: Semicolons are DATA, not problematic - they're useful delimiters
    }

    # SQL injection patterns to remove/neutralize (more precise)
    SQL_INJECTION_PATTERNS = [
        r"--.*$",  # SQL comments
        r"/\*.*?\*/",  # Block comments
        r"(?i)\b(OR|AND)\s+\d+\s*=\s*\d+",  # OR 1=1, AND 1=1
        r"(?i)\b(OR|AND)\s+\d+\s*>\s*\d+",  # OR 1>0, AND 1>0
    ]

    # YAML-breaking characters (already handled in metadata_manager.py)
    YAML_BREAKING_STARTS = [">", "|", "&", "*", "@", "`"]

    # Jinja-breaking characters (already handled in metadata_manager.py)
    JINJA_BREAKING_CHARS = {
        "{{": "{ {",
        "}}": "} }",
        "{%": "{ %",
        "%}": "% }",
        "{#": "{ #",
        "#}": "# }",
        "{{{": "{ { {",
        "}}}": "} } }",
    }

    @classmethod
    def sanitize_for_sql_string(cls, value: str) -> str:
        """
        Sanitize value for use in SQL string literals (COMMENT clauses).

        Args:
            value: String to sanitize

        Returns:
            SQL-safe string with single quotes escaped
        """
        if not value:
            return ""

        # Escape single quotes (SQL standard: ' becomes '')
        sanitized = str(value).replace("'", "''")

        # Remove other SQL-breaking characters
        for char, replacement in cls.SQL_BREAKING_CHARS.items():
            if char != "'":  # Already handled above
                sanitized = sanitized.replace(char, replacement)

        # Remove SQL injection patterns
        for pattern in cls.SQL_INJECTION_PATTERNS:
            sanitized = re.sub(pattern, "", sanitized, flags=re.MULTILINE)

        return sanitized.strip()

    @classmethod
    def sanitize_for_synonyms(cls, value: str) -> str:
        """
        Sanitize value for use in WITH SYNONYMS clause.

        Apostrophes cause parse errors even when escaped, so remove them entirely.

        Args:
            value: Synonym to sanitize

        Returns:
            Synonym-safe string with apostrophes removed
        """
        if not value:
            return ""

        sanitized = str(value).strip()

        # Remove all apostrophe types (they break WITH SYNONYMS)
        for char in ["'", "'", "'"]:
            sanitized = sanitized.replace(char, "")

        # Remove double quotes (can cause issues in SQL)
        sanitized = sanitized.replace('"', "")

        # Note: Semicolons are DATA (useful delimiters) - don't remove them!

        # Remove SQL injection patterns
        for pattern in cls.SQL_INJECTION_PATTERNS:
            sanitized = re.sub(pattern, "", sanitized, flags=re.MULTILINE)

        return sanitized.strip()

    @classmethod
    def sanitize_for_yaml_value(cls, value: str, max_length: int = 500) -> str:
        """
        Sanitize value for use in YAML files (sample_values, descriptions).

        Conservative approach: Only remove characters that ACTUALLY break YAML/dbt,
        preserve data integrity for legitimate values (SKUs, codes, IDs, etc.).

        Args:
            value: String to sanitize
            max_length: Maximum length before truncation

        Returns:
            YAML-safe string (will be quoted by YAML serializer if needed)
        """
        if not value:
            return ""

        val_str = str(value)

        # ONLY remove truly problematic characters:
        # 1. NUL bytes and actual control characters (0x00-0x1F except tab/newline/CR)
        # These cause parsing errors in downstream tools like Hightouch
        val_str = "".join(char for char in val_str if ord(char) >= 32 or char in "\t\n\r")

        # 2. Remove actual Unicode escape sequences in the string (not Unicode characters themselves!)
        # These are literal backslash-u patterns like "\u0041" that cause YAML parsing errors
        val_str = re.sub(r"\\u[0-9a-fA-F]{4}", "", val_str)
        val_str = re.sub(r"\\x[0-9a-fA-F]{2}", "", val_str)

        # 3. Escape remaining backslashes to prevent creating new escape sequences
        val_str = val_str.replace("\\", "\\\\")

        # 4. Sanitize Jinja-breaking characters (these break dbt compilation)
        for jinja_char, replacement in cls.JINJA_BREAKING_CHARS.items():
            val_str = val_str.replace(jinja_char, replacement)

        # 5. Handle YAML-breaking characters at START only (YAML serializer handles the rest)
        for char in cls.YAML_BREAKING_STARTS:
            if val_str.startswith(char):
                val_str = " " + val_str

        # NOTE: Do NOT remove SQL patterns, quotes, or dashes from sample values!
        # These are legitimate data (SKUs, codes, etc.) and YAML quoting handles them.
        # ruamel.yaml will automatically quote strings with special characters.

        # Intelligent truncation for different data types
        if len(val_str) > max_length:
            # Check if this looks like an embedding vector (array of floats)
            if val_str.startswith("[") and "," in val_str and "." in val_str:
                # For embedding vectors, show first few values and indicate truncation
                val_str = val_str[: max_length - 50] + "... [embedding vector truncated]"
            else:
                # For regular text, standard truncation
                val_str = val_str[:max_length] + "..."

        return val_str

    @classmethod
    def sanitize_synonym_list(cls, synonyms: List[str]) -> List[str]:
        """
        Sanitize a list of synonyms for WITH SYNONYMS clause.

        Args:
            synonyms: List of synonym strings

        Returns:
            List of sanitized synonyms (empty strings filtered out)
        """
        if not synonyms:
            return []

        sanitized = []
        for syn in synonyms:
            if syn and isinstance(syn, str):
                cleaned = cls.sanitize_for_synonyms(syn)
                if cleaned:  # Only add if still has content after cleaning
                    sanitized.append(cleaned)

        return sanitized

    @classmethod
    def validate_synonyms(cls, synonyms: List[str], context_name: str = "synonyms") -> List[str]:
        """
        Validate synonyms and return list of issues.

        Args:
            synonyms: List of synonym strings to validate
            context_name: Context for error messages

        Returns:
            List of validation error messages
        """
        errors = []

        if not synonyms:
            return errors

        for i, syn in enumerate(synonyms):
            if not syn or not isinstance(syn, str):
                continue

            # Check for apostrophes
            if any(char in syn for char in ["'", "'", "'"]):
                errors.append(f"{context_name}[{i}] contains apostrophe: '{syn}'")

            # Check for double quotes
            if '"' in syn:
                errors.append(f"{context_name}[{i}] contains double quote: '{syn}'")

            # Check for SQL injection patterns
            for pattern in cls.SQL_INJECTION_PATTERNS:
                if re.search(pattern, syn, re.IGNORECASE):
                    errors.append(f"{context_name}[{i}] contains SQL injection pattern: '{syn}'")

        return errors


# Convenience functions for backward compatibility
def sanitize_sql_string(value: str) -> str:
    """Sanitize value for SQL string literals."""
    return CharacterSanitizer.sanitize_for_sql_string(value)


def sanitize_synonyms(value: str) -> str:
    """Sanitize synonym for WITH SYNONYMS clause."""
    return CharacterSanitizer.sanitize_for_synonyms(value)


def sanitize_yaml_value(value: str, max_length: int = 1000) -> str:
    """Sanitize value for YAML files."""
    return CharacterSanitizer.sanitize_for_yaml_value(value, max_length)


def sanitize_synonym_list(synonyms: List[str]) -> List[str]:
    """Sanitize list of synonyms."""
    return CharacterSanitizer.sanitize_synonym_list(synonyms)


def validate_synonyms(synonyms: List[str], context_name: str = "synonyms") -> List[str]:
    """Validate synonyms and return error messages."""
    return CharacterSanitizer.validate_synonyms(synonyms, context_name)
