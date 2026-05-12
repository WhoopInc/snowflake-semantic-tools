"""
Source Reader

Reads source files and locates entity definitions for diagnostic rendering.
Provides line-level context for error messages with caching to avoid redundant reads.
"""

import re
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Tuple


class SourceReader:
    """Reads YAML source files and locates entities by name."""

    @staticmethod
    @lru_cache(maxsize=64)
    def _read_file(file_path: str) -> Optional[List[str]]:
        try:
            path = Path(file_path)
            if ".." in path.parts:
                return None
            return path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            return None

    @classmethod
    def get_source_lines(
        cls, file_path: str, line: int, context_before: int = 1, context_after: int = 1
    ) -> List[Tuple[int, str]]:
        """
        Get source lines around a given line number.

        Args:
            file_path: Path to the source file
            line: 1-based line number
            context_before: Number of lines to show before
            context_after: Number of lines to show after

        Returns:
            List of (line_number, line_content) tuples
        """
        lines = cls._read_file(file_path)
        if not lines:
            return []

        start = max(0, line - 1 - context_before)
        end = min(len(lines), line + context_after)

        return [(i + 1, lines[i]) for i in range(start, end)]

    @classmethod
    def find_entity_line(cls, file_path: str, entity_name: str, entity_type: str = "name") -> Optional[int]:
        """
        Find the line number where an entity is defined in a YAML file.

        Searches for patterns like:
            - name: entity_name
            - name: "entity_name"

        Args:
            file_path: Path to the YAML file
            entity_name: Name of the entity to find
            entity_type: The YAML key to search for (default: "name")

        Returns:
            1-based line number, or None if not found
        """
        lines = cls._read_file(file_path)
        if not lines:
            return None

        pattern = re.compile(rf"^\s*-?\s*{entity_type}\s*:\s*[\"']?{re.escape(entity_name)}[\"']?\s*$", re.IGNORECASE)

        for i, line in enumerate(lines):
            if pattern.match(line):
                return i + 1

        return None

    @classmethod
    def find_field_line(cls, file_path: str, entity_name: str, field_name: str) -> Optional[int]:
        """
        Find the line number of a specific field within an entity definition.

        Searches for the entity first, then looks for the field within its block.

        Args:
            file_path: Path to the YAML file
            entity_name: Name of the parent entity
            field_name: Name of the field to find

        Returns:
            1-based line number, or None if not found
        """
        lines = cls._read_file(file_path)
        if not lines:
            return None

        entity_line = cls.find_entity_line(file_path, entity_name)
        if not entity_line:
            return None

        field_pattern = re.compile(rf"^\s+{re.escape(field_name)}\s*:")

        for i in range(entity_line, min(len(lines), entity_line + 50)):
            if field_pattern.match(lines[i]):
                return i + 1
            if i > entity_line and re.match(r"^\s*-\s+name\s*:", lines[i]):
                break

        return None

    @classmethod
    def find_template_column(cls, line_content: str, template_arg: str) -> Optional[int]:
        """
        Find the column offset of a template argument within a line.

        Args:
            line_content: The source line text
            template_arg: The argument to find (e.g., 'orders_v2')

        Returns:
            0-based column offset, or None if not found
        """
        match = re.search(re.escape(template_arg), line_content)
        return match.start() if match else None

    @classmethod
    def clear_cache(cls):
        cls._read_file.cache_clear()
