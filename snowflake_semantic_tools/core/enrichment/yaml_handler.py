"""
YAML Handler with Preservation Rules

Handles reading and writing YAML files while preserving existing metadata
and maintaining perfect formatting standards.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from ruamel.yaml import YAML, YAMLError

from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


class YAMLHandler:
    """Handles YAML file operations with strict preservation rules."""

    def __init__(self):
        """Initialize YAML handler with proper formatting."""
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.map_indent = 2
        self.yaml.sequence_indent = 4
        self.yaml.sequence_dash_offset = 2
        self.yaml.width = 4096  # Prevent line wrapping
        self.yaml.explicit_start = False
        self.yaml.explicit_end = False

    def read_yaml(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Read YAML file and return parsed content.

        Args:
            file_path: Path to YAML file

        Returns:
            Dict containing YAML content, or None if file doesn't exist
        """
        if not os.path.exists(file_path):
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = self.yaml.load(f)
                return content if content else {}
        except (YAMLError, IOError, OSError) as e:
            # Make YAML errors visible to users (Issue #20)
            # Catch specific exceptions: YAMLError for parsing, IOError/OSError for file access
            logger.error(f"Failed to parse YAML file: {file_path}")
            logger.error(f"Error: {e}")
            raise ValueError(f"YAML parsing error in {file_path}: {e}") from e

    def write_yaml(self, content: Dict[str, Any], file_path: str) -> bool:
        """
        Write YAML content to file with perfect formatting.

        Args:
            content: Dictionary to write as YAML
            file_path: Path to write YAML file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Write YAML first
            with open(file_path, "w", encoding="utf-8") as f:
                self.yaml.dump(content, f)

            # Post-process to add blank lines between columns
            self._add_column_spacing(file_path)

            return True
        except (YAMLError, IOError, OSError) as e:
            # Make YAML errors visible to users (Issue #20)
            # Catch specific exceptions: YAMLError for serialization, IOError/OSError for file access
            logger.error(f"Failed to write YAML file: {file_path}")
            logger.error(f"Error: {e}")
            raise IOError(f"YAML write error for {file_path}: {e}") from e

    def _add_column_spacing(self, file_path: str):
        """
        Add proper spacing for YAML files:
        1. Blank line before "columns:" header
        2. Blank lines between column definitions (after is_enum line)
        3. Remove unwanted blank lines within SST metadata blocks

        Args:
            file_path: Path to YAML file to format
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Split into lines for processing
            lines = content.split("\n")
            formatted_lines = []

            for i, line in enumerate(lines):
                # Check if this line is "columns:" header
                is_columns_header = line.strip() == "columns:"

                # Add blank line before "columns:" header if previous line isn't blank
                if is_columns_header and formatted_lines and formatted_lines[-1].strip() != "":
                    formatted_lines.append("")

                # Skip unwanted blank lines within SST metadata blocks
                if line.strip() == "":
                    # Check context around blank line
                    prev_line = lines[i - 1].strip() if i > 0 else ""
                    next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                    # Skip blank line if it's between synonyms and sample_values
                    skip_line = False

                    # Case 1: Blank line after synonyms list items, before sample_values
                    if prev_line.startswith("- ") and next_line.startswith("sample_values:"):
                        # Look back to see if we're in a synonyms section
                        for j in range(max(0, i - 10), i):
                            if lines[j].strip().endswith("synonyms:"):
                                skip_line = True
                                break

                    # Case 2: Blank line right after synonyms header
                    if prev_line.endswith("synonyms:") and next_line.startswith("sample_values:"):
                        skip_line = True

                    if skip_line:
                        continue

                formatted_lines.append(line)

                # Check if this line ends a column definition (is_enum: true/false)
                # Since we now enforce key ordering, is_enum should always be last
                is_enum_line = line.strip().startswith("is_enum:")

                # Add blank line after is_enum if next line is a new column
                if is_enum_line and i + 1 < len(lines):
                    next_line = lines[i + 1]
                    # If next non-empty line is a new column, add blank line
                    if next_line.strip().startswith("- name:"):
                        formatted_lines.append("")

            # Write formatted content back
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(formatted_lines))

        except Exception as e:
            print(f"Warning: Could not add column spacing to {file_path}: {e}")

    def _get_models_list(self, yaml_content: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get models list, handling both 'models' and 'semantic_models' keys.

        For semantic_models-only files (dbt MetricFlow format), uses that list.
        This allows enrichment to work with MetricFlow YAML files that only have
        semantic_models defined.

        Args:
            yaml_content: Parsed YAML content

        Returns:
            List of model dictionaries
        """
        # Standard dbt YAML
        if "models" in yaml_content:
            models = yaml_content.get("models", [])
            return models if isinstance(models, list) else []

        # dbt MetricFlow format - semantic_models without models
        if "semantic_models" in yaml_content:
            logger.debug("Found semantic_models key without models - using semantic_models as models")
            semantic_models = yaml_content.get("semantic_models", [])
            return semantic_models if isinstance(semantic_models, list) else []

        return []

    def _has_models(self, yaml_content: Dict[str, Any]) -> bool:
        """Check if YAML has models (either 'models' or 'semantic_models' key)."""
        return "models" in yaml_content or "semantic_models" in yaml_content

    def get_existing_model_metadata(self, yaml_content: Dict[str, Any], model_name: str) -> Optional[Dict[str, Any]]:
        """Extract existing metadata for a specific model."""
        # Use helper that handles both 'models' and 'semantic_models'
        models = self._get_models_list(yaml_content)
        for model in models:
            if isinstance(model, dict) and model.get("name") == model_name:
                return model
        return None

    def get_existing_column_metadata(
        self, model_metadata: Dict[str, Any], column_name: str
    ) -> Optional[Dict[str, Any]]:
        """Extract existing metadata for a specific column."""
        return self._find_item_by_name(model_metadata, "columns", column_name)

    def _find_item_by_name(self, container: Dict[str, Any], key: str, name: str) -> Optional[Dict[str, Any]]:
        """
        Generic helper to find an item by name in a list.

        Args:
            container: Dict containing the list
            key: Key of the list to search ('models' or 'columns')
            name: Name to search for

        Returns:
            Found item or None
        """
        if not container or key not in container:
            return None

        items = container.get(key, [])
        if not isinstance(items, list):
            return None

        for item in items:
            if isinstance(item, dict) and item.get("name") == name:
                return item

        return None

    def preserve_existing_values(
        self, existing: Dict[str, Any], new: Dict[str, Any], preserve_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Merge new values while preserving specified existing fields.

        Args:
            existing: Existing metadata dictionary
            new: New metadata dictionary
            preserve_fields: List of field names to preserve from existing

        Returns:
            Dict with merged values
        """
        result = new.copy()

        for field in preserve_fields:
            if field in existing and existing[field] is not None:
                # Handle nested dictionaries (like meta.sst)
                if "." in field:
                    parts = field.split(".")
                    existing_nested = existing
                    new_nested = result

                    # Navigate to nested field
                    for part in parts[:-1]:
                        if part in existing_nested:
                            existing_nested = existing_nested[part]
                            if part not in new_nested:
                                new_nested[part] = {}
                            new_nested = new_nested[part]
                        else:
                            existing_nested = None
                            break

                    # Preserve the final field value
                    if existing_nested and parts[-1] in existing_nested:
                        new_nested[parts[-1]] = existing_nested[parts[-1]]
                else:
                    result[field] = existing[field]

        return result

    def ensure_sst_structure(self, model_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure proper meta.sst structure exists.

        Args:
            model_metadata: Model metadata dictionary

        Returns:
            Dict with proper sst structure
        """
        if "meta" not in model_metadata:
            model_metadata["meta"] = {}

        # Create sst section if it doesn't exist
        if "sst" not in model_metadata["meta"]:
            model_metadata["meta"]["sst"] = {}

        return model_metadata

    def ensure_column_sst_structure(self, column_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure proper meta.sst structure exists for column with correct key ordering.

        IMPORTANT: Always writes 'meta.sst', not 'meta.genie'.

        Args:
            column_metadata: Column metadata dictionary

        Returns:
            Dict with proper sst structure and ordered keys
        """
        if "meta" not in column_metadata:
            column_metadata["meta"] = {}

        # Create sst section if it doesn't exist
        if "sst" not in column_metadata["meta"]:
            column_metadata["meta"]["sst"] = {}

        # Remove old genie section if present (migration/cleanup)
        if "genie" in column_metadata["meta"]:
            logger.debug(f"Migrating column from meta.genie to meta.sst")
            # Preserve any data from genie section before removing
            for key, value in column_metadata["meta"]["genie"].items():
                if key not in column_metadata["meta"]["sst"]:
                    column_metadata["meta"]["sst"][key] = value
            del column_metadata["meta"]["genie"]

        # Ensure proper key ordering for sst metadata
        column_metadata = self._order_column_sst_keys(column_metadata)

        return column_metadata

    def _order_column_sst_keys(self, column_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure SST metadata keys are in the correct order.
        Also handles legacy 'genie' section for migration support.

        Args:
            column_metadata: Column metadata dictionary

        Returns:
            Dict with properly ordered sst/genie keys
        """
        if "meta" not in column_metadata:
            return column_metadata

        # Work with sst (preferred) or genie (legacy migration support)
        section_name = "sst" if "sst" in column_metadata["meta"] else "genie"
        if section_name not in column_metadata["meta"]:
            return column_metadata

        sst = column_metadata["meta"][section_name]

        # Desired order for sst metadata keys
        key_order = ["column_type", "data_type", "synonyms", "sample_values", "is_enum", "privacy_category"]

        # Create ordered dictionary
        ordered_sst = {}

        # Add keys in the desired order
        for key in key_order:
            if key in sst:
                ordered_sst[key] = sst[key]

        # Add any remaining keys that weren't in our order
        for key, value in sst.items():
            if key not in ordered_sst:
                ordered_sst[key] = value

        # Replace the section with ordered version
        column_metadata["meta"][section_name] = ordered_sst

        return column_metadata

    def _order_column_genie_keys(self, column_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deprecated: Use _order_column_sst_keys() instead.
        Maintained for backward compatibility.
        """
        return self._order_column_sst_keys(column_metadata)

    def create_base_yaml_structure(self, model_name: str) -> Dict[str, Any]:
        """
        Create base YAML structure for a new model.

        Args:
            model_name: Name of the model

        Returns:
            Dict with base YAML structure
        """
        return {
            "version": 2,
            "models": [
                {
                    "name": model_name,
                    "description": f"Model description for {model_name}.",
                    "meta": {
                        "sst": {
                            "cortex_searchable": False,
                            "database": "analytics",
                            "schema": "",  # Will be filled in by processor
                            "table": model_name,
                            "primary_key": "",  # Will be detected
                            "synonyms": [],
                        }
                    },
                    "config": {"tags": []},
                    "columns": [],
                }
            ],
        }

    def find_yaml_file_for_model(self, model_sql_path: str) -> str:
        """
        Find or construct YAML file path for a given SQL model file.

        Search order:
        1. Same name as SQL file (model.yml or model.yaml)
        2. Any YAML file in same directory containing this model
        3. Return expected path (model.yml) if nothing found
        """
        sql_path = Path(model_sql_path)
        model_name = sql_path.stem
        parent_dir = sql_path.parent

        # 1. Check for exact name match (both extensions)
        for ext in [".yml", ".yaml"]:
            expected_path = parent_dir / f"{model_name}{ext}"
            if expected_path.exists():
                return str(expected_path)

        # 2. Search all YAML files for one containing this model
        for ext in ["*.yml", "*.yaml"]:
            for yaml_file in parent_dir.glob(ext):
                if self._yaml_contains_model(yaml_file, model_name):
                    return str(yaml_file)

        # 3. Return expected path (will be created if needed)
        return str(parent_dir / f"{model_name}.yml")

    def _yaml_contains_model(self, yaml_path: Path, model_name: str) -> bool:
        """
        Check if a YAML file contains a model with the given name.

        Args:
            yaml_path: Path to YAML file
            model_name: Name of model to search for

        Returns:
            bool: True if YAML contains model with this name
        """
        try:
            content = self.read_yaml(str(yaml_path))
            if not content or not self._has_models(content):
                return False

            models = self._get_models_list(content)
            for model in models:
                if isinstance(model, dict) and model.get("name") == model_name:
                    return True

            return False
        except Exception:
            # If we can't read/parse the file, assume it doesn't contain the model
            return False
