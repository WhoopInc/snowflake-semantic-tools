"""
YAML Formatting Service

Formats dbt YAML files to ensure consistent structure, ordering, and style.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from ruamel.yaml import YAML

from snowflake_semantic_tools.shared.events import FormatCompleted, FormatStarted, fire_event
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


@dataclass
class FormattingConfig:
    """Configuration for YAML formatting."""

    dry_run: bool = False
    check_only: bool = False
    force: bool = False  # Always write files, even if content appears unchanged


class YAMLFormattingService:
    """Service for formatting dbt YAML files."""

    # Standard field ordering for dbt models
    MODEL_FIELD_ORDER = ["name", "description", "meta", "config", "data_tests", "columns"]

    # Standard field ordering for columns
    COLUMN_FIELD_ORDER = ["name", "description", "data_tests", "meta"]

    # Standard field ordering for SST metadata (also used for legacy genie migration)
    SST_FIELD_ORDER = ["column_type", "data_type", "synonyms", "sample_values", "is_enum", "privacy_category"]

    def __init__(self, config: FormattingConfig):
        """Initialize formatting service."""
        self.config = config
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.default_style = None  # Don't preserve scalar styles
        self.yaml.map_indent = 2
        self.yaml.sequence_indent = 4
        self.yaml.sequence_dash_offset = 2
        self.yaml.width = 4096

    def format_path(self, path: Path) -> Dict[str, int]:
        """
        Format YAML files at the given path.

        Args:
            path: File or directory to format

        Returns:
            Dict with formatting statistics
        """
        if path.is_file():
            files = [path]
        else:
            files = list(path.rglob("*.yml")) + list(path.rglob("*.yaml"))

        stats = {"files_processed": 0, "files_formatted": 0, "files_needing_formatting": 0, "errors": 0}

        for file in files:
            try:
                stats["files_processed"] += 1
                needs_formatting = self._format_file(file)

                if needs_formatting:
                    stats["files_needing_formatting"] += 1
                    if not self.config.check_only:
                        stats["files_formatted"] += 1

            except Exception as e:
                logger.error(f"Error formatting {file}: {e}")
                stats["errors"] += 1

        return stats

    def _format_file(self, file_path: Path) -> bool:
        """
        Format a single YAML file.

        Returns:
            True if file needed formatting, False otherwise
        """
        # Read original content
        with open(file_path, "r", encoding="utf-8") as f:
            original_content = f.read()

        # Parse YAML
        content = self.yaml.load(original_content)

        if not content or "models" not in content:
            return False

        # Format models
        for model in content["models"]:
            self._format_model(model)

        # Write formatted content to string
        from io import StringIO

        stream = StringIO()
        self.yaml.dump(content, stream)
        formatted_content = stream.getvalue()

        # Post-process for blank lines
        formatted_content = self._adjust_blank_lines(formatted_content)

        # Check if content changed
        needs_formatting = original_content != formatted_content

        # Force mode always writes, even if content appears unchanged
        if needs_formatting or self.config.force:
            if self.config.dry_run:
                logger.info(f"Would format: {file_path}")
            elif self.config.check_only:
                logger.info(f"Needs formatting: {file_path}")
            else:
                # Write formatted content
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(formatted_content)
                logger.info(f"Formatted: {file_path}")
            return True

        return False

    def _format_model(self, model: Dict[str, Any]):
        """Format a model dictionary in place."""
        # Format multi-line description BEFORE reordering
        if "description" in model:
            model["description"] = self._format_description(model["description"])

        # Format list fields (synonyms, etc.) in both sst and legacy genie sections
        if "meta" in model:
            for section_name in ["sst", "genie"]:  # Handle both for migration
                if section_name in model["meta"]:
                    section = model["meta"][section_name]
                    if "synonyms" in section:
                        section["synonyms"] = self._format_list(section["synonyms"])

        # Reorder model fields AFTER formatting description
        self._reorder_dict(model, self.MODEL_FIELD_ORDER)

        # Format columns
        if "columns" in model and isinstance(model["columns"], list):
            for column in model["columns"]:
                self._format_column(column)

    def _format_column(self, column: Dict[str, Any]):
        """Format a column dictionary in place."""
        # Format multi-line description BEFORE reordering
        if "description" in column:
            column["description"] = self._format_description(column["description"])

        # Format list fields in SST metadata (and legacy genie for migration)
        if "meta" in column:
            for section_name in ["sst", "genie"]:  # Handle both for migration
                if section_name in column["meta"]:
                    section = column["meta"][section_name]
                    if "synonyms" in section:
                        section["synonyms"] = self._format_list(section["synonyms"])
                    if "sample_values" in section:
                        section["sample_values"] = self._format_list(section["sample_values"])

        # Reorder column fields AFTER formatting description
        self._reorder_dict(column, self.COLUMN_FIELD_ORDER)

        # Format SST/genie metadata with proper field ordering
        if "meta" in column:
            for section_name in ["sst", "genie"]:  # Handle both for migration
                if section_name in column["meta"]:
                    self._reorder_dict(column["meta"][section_name], self.SST_FIELD_ORDER)

    def _format_list(self, list_value: Any) -> Any:
        """
        Format list to consistent style.

        Rule:
        - Empty list → [] (inline)
        - Any items (1+) → Multi-line format (one item per line)

        Args:
            list_value: List to format (or None)

        Returns:
            Formatted list (inline [] for empty, multi-line for items)
        """
        # Handle None or non-list values
        if list_value is None:
            return []

        if not isinstance(list_value, list):
            # If it's not a list, return as-is (validation will catch this)
            return list_value

        # Empty list → inline format []
        if len(list_value) == 0:
            return []

        # Non-empty list → return as-is (ruamel.yaml will format as multi-line by default)
        # ruamel.yaml automatically formats lists with items as multi-line
        return list_value

    def _format_description(self, description: Any, max_width: int = 80) -> Any:
        """
        Format description preserving intentional structure.

        PRESERVES:
        - Intentional line breaks (paragraphs, bullets, sections)
        - List structure (-, *, etc.)
        - Section headers

        ONLY normalizes:
        - Multiple consecutive spaces within a line
        - Trailing whitespace

        Args:
            description: Description text to format (string or scalar string)
            max_width: Maximum characters per line (default 80)

        Returns:
            Formatted description (string or literal scalar for multi-line)
        """
        if not description:
            return description

        # Convert any scalar string type to plain string first
        if hasattr(description, "__str__"):
            desc = str(description).strip()
        else:
            return description

        # Check if description has intentional structure (newlines)
        has_newlines = "\n" in desc

        # If short single-line description, return as-is
        if not has_newlines and len(desc) <= max_width:
            return desc

        # If has intentional newlines (bullets, sections, paragraphs), preserve them
        if has_newlines:
            # Only clean up trailing spaces on each line, preserve structure
            import re

            from ruamel.yaml.scalarstring import LiteralScalarString

            lines = desc.split("\n")
            cleaned_lines = [line.rstrip() for line in lines]
            # Remove excessive blank lines (max 1 consecutive)
            result_lines = []
            prev_blank = False
            for line in cleaned_lines:
                is_blank = not line.strip()
                if is_blank and prev_blank:
                    continue  # Skip consecutive blank lines
                result_lines.append(line)
                prev_blank = is_blank

            formatted_text = "\n".join(result_lines)
            return LiteralScalarString(formatted_text)

        # Single-line description that needs wrapping
        # Only wrap if no intentional structure
        import re

        from ruamel.yaml.scalarstring import LiteralScalarString

        # Normalize multiple spaces within the single line
        desc = re.sub(r" +", " ", desc)

        # Word wrap for readability
        lines = []
        current_line = []
        current_length = 0

        words = desc.split()

        for word in words:
            word_length = len(word)
            space_needed = 1 if current_line else 0
            total_length = current_length + space_needed + word_length

            if total_length > max_width and current_line:
                lines.append(" ".join(current_line))
                current_line = [word]
                current_length = word_length
            else:
                current_line.append(word)
                current_length = total_length

        if current_line:
            lines.append(" ".join(current_line))

        formatted_text = "\n".join(lines)
        return LiteralScalarString(formatted_text)

    def _reorder_dict(self, d: Dict[str, Any], field_order: List[str]):
        """Reorder dictionary keys according to field_order."""
        if not isinstance(d, dict):
            return

        # Get all keys
        all_keys = list(d.keys())

        # Separate ordered and unordered keys
        ordered_keys = [k for k in field_order if k in all_keys]
        other_keys = [k for k in all_keys if k not in field_order]

        # Create new ordered dict
        new_order = ordered_keys + other_keys

        # Reorder in place
        for key in new_order:
            d[key] = d.pop(key)

    def _adjust_blank_lines(self, content: str) -> str:
        """
        Adjust blank lines for readability.

        Rules:
        - NO blank line after 'models:' header
        - Add blank line AFTER each column definition (before next - name:)
        - Remove blank lines between meta/config, columns:/first column
        - Remove excessive consecutive blank lines (max 1)
        """
        lines = content.split("\n")
        formatted_lines = []

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Skip blank lines in specific locations
            if stripped == "":
                if formatted_lines and i + 1 < len(lines):
                    prev_line = formatted_lines[-1].strip()
                    next_line = lines[i + 1].strip()

                    # Remove blank line between 'models:' and first '- name:'
                    if prev_line == "models:" and next_line.startswith("- name:"):
                        continue  # Skip this blank line

                    # Also remove if we're right after models: (ruamel adds this)
                    if prev_line == "models:":
                        continue  # Skip this blank line

                    # Remove blank line between meta block and config
                    # Check if we're after an SST metadata block and before config
                    if next_line.startswith("config:"):
                        # Skip this blank line (no blank line between meta and config)
                        continue

                    # Remove blank line between columns: and first - name:
                    if prev_line == "columns:" and next_line.startswith("- name:"):
                        continue  # Skip this blank line

                # Skip excessive blank lines (more than 1 consecutive)
                if formatted_lines and formatted_lines[-1].strip() == "":
                    continue  # Skip this blank line

            formatted_lines.append(line)

            # Add blank line AFTER each column definition ends (before next - name:)
            # Only add if current line is not blank, not 'columns:', and next line is a new column
            if i + 1 < len(lines) and stripped != "" and stripped != "columns:":
                next_line = lines[i + 1].strip()
                # If next line is a new column, add blank line after current line
                if next_line.startswith("- name:"):
                    formatted_lines.append("")

        # Ensure file ends with single newline
        while formatted_lines and formatted_lines[-1].strip() == "":
            formatted_lines.pop()

        result = "\n".join(formatted_lines) + "\n"

        # Final pass: remove unwanted blank lines that ruamel.yaml adds
        result = result.replace("models:\n\n  - name:", "models:\n  - name:")
        # Remove blank line between description and meta
        result = result.replace("description: |-\n          ", "description: |-\n          ").replace(
            "\n\n        meta:", "\n        meta:"
        )
        # Also handle single-line descriptions
        import re

        result = re.sub(r"(description: .+)\n\n(        meta:)", r"\1\n\2", result)

        return result
