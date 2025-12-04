"""
YAML Sanitization Service

Comprehensive service for sanitizing metadata in YAML files.
Removes problematic characters from synonyms, descriptions, and sample values.

Design:
- Reusable service called by multiple commands (format, enrich)
- Uses CharacterSanitizer for consistency
- Tracks all changes for reporting
- Supports dry-run mode
- Enterprise-grade architecture

This ensures consistency between:
- Manual edits by analysts
- Auto-generated content from enrichment
- Validated content in deployment
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ruamel.yaml import YAML

from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer

logger = get_logger(__name__)


@dataclass
class SanitizationChange:
    """Represents a single sanitization change."""

    field_type: str  # 'table_synonym', 'column_description', 'sample_value', etc.
    location: str  # 'table: CUSTOMERS' or 'column: USER_ID in CUSTOMERS'
    original: str
    sanitized: str

    def __str__(self) -> str:
        return f"{self.location} {self.field_type}: '{self.original}' → '{self.sanitized}'"


@dataclass
class SanitizationResult:
    """
    Results from sanitizing a file or model.

    Tracks all changes made for reporting and dry-run preview.
    """

    changes: List[SanitizationChange] = field(default_factory=list)
    files_modified: int = 0

    def add_change(self, field_type: str, location: str, original: str, sanitized: str):
        """Add a sanitization change."""
        if original != sanitized:  # Only track actual changes
            self.changes.append(SanitizationChange(field_type, location, original, sanitized))

    @property
    def has_changes(self) -> bool:
        """Check if any changes were made."""
        return len(self.changes) > 0

    @property
    def change_count(self) -> int:
        """Total number of changes."""
        return len(self.changes)

    def get_changes_by_type(self) -> Dict[str, int]:
        """Get count of changes by field type."""
        counts = {}
        for change in self.changes:
            counts[change.field_type] = counts.get(change.field_type, 0) + 1
        return counts

    def merge(self, other: "SanitizationResult"):
        """Merge another result into this one."""
        self.changes.extend(other.changes)
        self.files_modified += other.files_modified


class YAMLSanitizationService:
    """
    Centralized service for sanitizing YAML metadata files.

    Provides comprehensive sanitization of all metadata fields:
    - Table synonyms (removes apostrophes, quotes)
    - Column synonyms (removes apostrophes, quotes)
    - Table descriptions (removes control chars, Unicode escapes, Jinja chars)
    - Column descriptions (removes control chars, Unicode escapes, Jinja chars)
    - Sample values (removes problematic characters from manual entries)

    Used by:
    - Format command (user-initiated cleanup via --sanitize flag)
    - Enrichment service (auto-sanitize on write)
    - Any future commands needing consistent sanitization

    Design principles:
    - Uses CharacterSanitizer for all cleaning (DRY)
    - Tracks all changes for reporting
    - Supports dry-run mode (preview without changes)
    - Non-destructive (only cleans, doesn't remove fields)
    """

    def __init__(self):
        """Initialize sanitization service."""
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.default_flow_style = False

    def sanitize_file(self, file_path: Path, dry_run: bool = False) -> SanitizationResult:
        """
        Sanitize a single YAML file.

        Args:
            file_path: Path to YAML file
            dry_run: If True, don't write changes (preview only)

        Returns:
            SanitizationResult with all changes made
        """
        result = SanitizationResult()

        try:
            # Load YAML file
            with open(file_path, "r") as f:
                data = self.yaml.load(f)

            if not data or "models" not in data:
                logger.debug(f"No models found in {file_path}")
                return result

            # Sanitize each model
            file_changed = False
            for model in data["models"]:
                model_result = self.sanitize_model(model)
                result.merge(model_result)
                if model_result.has_changes:
                    file_changed = True

            # Write back if changes made (unless dry_run)
            if file_changed and not dry_run:
                with open(file_path, "w") as f:
                    self.yaml.dump(data, f)
                result.files_modified = 1
                logger.info(f"Sanitized {file_path}: {result.change_count} changes")
            elif file_changed and dry_run:
                logger.info(f"[DRY RUN] Would sanitize {file_path}: {result.change_count} changes")

            return result

        except Exception as e:
            logger.error(f"Failed to sanitize {file_path}: {e}")
            return result

    def sanitize_model(self, model: Dict[str, Any]) -> SanitizationResult:
        """
        Sanitize all fields in a model dictionary.

        Args:
            model: Model dictionary from YAML

        Returns:
            SanitizationResult with all changes
        """
        result = SanitizationResult()
        model_name = model.get("name", "unknown")

        # Sanitize table-level description
        # Note: Descriptions use sanitize_for_yaml_value which handles control chars,
        # Unicode escapes, and Jinja - but KEEPS apostrophes (they're data, get escaped in SQL)
        if "description" in model and model["description"]:
            original = str(model["description"])
            sanitized = CharacterSanitizer.sanitize_for_yaml_value(original, max_length=10000)
            if sanitized != original:
                result.add_change("table_description", f"table: {model_name}", original, sanitized)
                model["description"] = sanitized

        # Sanitize table-level synonyms (in meta.sst or meta.genie)
        for section_name in ["sst", "genie"]:
            if "meta" in model and section_name in model["meta"]:
                section = model["meta"][section_name]
                if "synonyms" in section and isinstance(section["synonyms"], list):
                    original_synonyms = section["synonyms"]
                    sanitized_synonyms = CharacterSanitizer.sanitize_synonym_list(original_synonyms)
                    if sanitized_synonyms != original_synonyms:
                        for orig, clean in zip(original_synonyms, sanitized_synonyms):
                            if orig != clean:
                                result.add_change("table_synonym", f"table: {model_name}", orig, clean)
                        section["synonyms"] = sanitized_synonyms

        # Sanitize column-level fields
        if "columns" in model:
            for column in model["columns"]:
                column_result = self._sanitize_column(column, model_name)
                result.merge(column_result)

        return result

    def _sanitize_column(self, column: Dict[str, Any], table_name: str) -> SanitizationResult:
        """
        Sanitize all fields in a column dictionary.

        Args:
            column: Column dictionary from YAML
            table_name: Parent table name for context

        Returns:
            SanitizationResult with column changes
        """
        result = SanitizationResult()
        column_name = column.get("name", "unknown")
        location = f"column: {column_name} in {table_name}"

        # Sanitize column description
        # Note: Descriptions use sanitize_for_yaml_value which handles control chars,
        # Unicode escapes, and Jinja - but KEEPS apostrophes (they're data, get escaped in SQL)
        if "description" in column and column["description"]:
            original = str(column["description"])
            sanitized = CharacterSanitizer.sanitize_for_yaml_value(original, max_length=10000)
            if sanitized != original:
                result.add_change("column_description", location, original, sanitized)
                column["description"] = sanitized

        # Sanitize column-level synonyms (in meta.sst or meta.genie)
        for section_name in ["sst", "genie"]:
            if "meta" not in column or section_name not in column["meta"]:
                continue

            section = column["meta"][section_name]

            # Synonyms
            if "synonyms" in section and isinstance(section["synonyms"], list):
                original_synonyms = section["synonyms"]
                sanitized_synonyms = CharacterSanitizer.sanitize_synonym_list(original_synonyms)
                if sanitized_synonyms != original_synonyms:
                    for orig, clean in zip(original_synonyms, sanitized_synonyms):
                        if orig != clean:
                            result.add_change("column_synonym", location, orig, clean)
                    section["synonyms"] = sanitized_synonyms

            # Sample values (preserve data integrity!)
            # Only remove truly problematic characters (NUL, control chars, Jinja syntax)
            # YAML quoting handles quotes, dashes, and other special characters
            if "sample_values" in section and isinstance(section["sample_values"], list):
                original_values = section["sample_values"]
                sanitized_values = []

                for val in original_values:
                    if val is None:
                        sanitized_values.append(val)
                        continue

                    original_str = str(val)
                    # Use YAML value sanitization (preserves data, only removes control chars/Jinja)
                    sanitized_str = CharacterSanitizer.sanitize_for_yaml_value(original_str)

                    if sanitized_str != original_str:
                        result.add_change("sample_value", location, original_str, sanitized_str)

                    sanitized_values.append(sanitized_str)

                if sanitized_values != original_values:
                    section["sample_values"] = sanitized_values

        return result

    def sanitize_directory(self, directory: Path, dry_run: bool = False, recursive: bool = True) -> SanitizationResult:
        """
        Sanitize all YAML files in a directory.

        Args:
            directory: Directory containing YAML files
            dry_run: If True, preview changes without writing
            recursive: If True, process subdirectories

        Returns:
            Combined SanitizationResult for all files
        """
        total_result = SanitizationResult()

        # Find all YAML files
        if recursive:
            yaml_files = list(directory.rglob("*.yml")) + list(directory.rglob("*.yaml"))
        else:
            yaml_files = list(directory.glob("*.yml")) + list(directory.glob("*.yaml"))

        logger.info(f"Found {len(yaml_files)} YAML files to sanitize")

        for yaml_file in yaml_files:
            file_result = self.sanitize_file(yaml_file, dry_run=dry_run)
            total_result.merge(file_result)

        return total_result

    def print_summary(self, result: SanitizationResult, dry_run: bool = False):
        """
        Print comprehensive sanitization summary.

        Args:
            result: SanitizationResult to summarize
            dry_run: Whether this was a dry run
        """
        mode = "DRY RUN - Preview" if dry_run else "COMPLETED"

        print(f"\n{'='*70}")
        print(f"SANITIZATION {mode}")
        print(f"{'='*70}")

        if not result.has_changes:
            print("No changes needed - all metadata is clean!")
            print(f"{'='*70}\n")
            return

        # Show changes by type
        changes_by_type = result.get_changes_by_type()

        print(f"Files affected: {result.files_modified if not dry_run else 'N/A (dry run)'}")
        print(f"Total changes: {result.change_count}")
        print()

        print("Changes by type:")
        for field_type, count in sorted(changes_by_type.items()):
            print(f"  {field_type}: {count}")

        print()

        # Show sample changes (first 10)
        if result.changes:
            print("Sample changes:")
            for i, change in enumerate(result.changes[:10], 1):
                print(f"  {i}. {change}")

            if len(result.changes) > 10:
                print(f"  ... and {len(result.changes) - 10} more changes")

        print(f"{'='*70}\n")

        if dry_run:
            print("Run without --dry-run to apply these changes")
