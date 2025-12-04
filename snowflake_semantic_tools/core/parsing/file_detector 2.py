"""
File Type Detection

Intelligent YAML file classification for semantic model processing.

Automatically identifies file types based on content patterns, supporting:
- dbt model definitions (physical layer)
- Semantic model components (metrics, relationships, filters)
- Custom instructions for Cortex Analyst
- Semantic view definitions

Handles both valid YAML and files with Jinja templates through fallback pattern matching.
"""

from pathlib import Path
from typing import Optional

import yaml

from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("file_detector")


class FileTypeDetector:
    """
    Classifies YAML files by analyzing their structure and content patterns.

    Uses a two-tier detection strategy:

    1. **YAML Parsing**: Attempts to parse as valid YAML and check root keys
       - Fast and accurate for standard YAML files
       - Identifies specific semantic model types (metrics, relationships, etc.)

    2. **Pattern Matching**: Falls back to string pattern detection
       - Works with files containing Jinja templates (invalid YAML)
       - Ensures robust detection even with complex template syntax

    Root Key Mapping:
    - `snowflake_metrics:` → Metric definitions (aggregated KPIs)
    - `snowflake_relationships:` → Table join definitions
    - `snowflake_filters:` → Reusable WHERE conditions
    - `snowflake_custom_instructions:` → Cortex Analyst guidance
    - `snowflake_verified_queries:` → Validated query examples
    - `semantic_views:` → Domain-specific view definitions
    - `models:` → dbt model definitions (physical layer)
    """

    # Mapping of root keys to semantic types
    SEMANTIC_TYPE_KEYS = {
        "snowflake_metrics": "metrics",
        "snowflake_relationships": "relationships",
        "snowflake_filters": "filters",
        "snowflake_custom_instructions": "custom_instructions",
        "snowflake_verified_queries": "verified_queries",
        "semantic_views": "semantic_views",
        "models": "dbt",
    }

    # String patterns for fallback detection (when YAML parsing fails)
    SEMANTIC_TYPE_PATTERNS = {
        "snowflake_metrics:": "metrics",
        "snowflake_relationships:": "relationships",
        "snowflake_filters:": "filters",
        "snowflake_custom_instructions:": "custom_instructions",
        "snowflake_verified_queries:": "verified_queries",
        "semantic_views:": "semantic_views",
        "models:": "dbt",
    }

    @classmethod
    def detect_file_type(cls, file_path: Path) -> str:
        """
        Detect the type of YAML file based on its content.

        This method provides backward compatibility for code expecting
        the simpler file type categories.

        Args:
            file_path: Path to the YAML file

        Returns:
            One of: 'dbt', 'semantic', 'unknown'
        """
        semantic_type = cls.detect_semantic_type(file_path)

        if semantic_type == "dbt":
            return "dbt"
        elif semantic_type in [
            "metrics",
            "relationships",
            "filters",
            "custom_instructions",
            "verified_queries",
            "semantic_views",
        ]:
            return "semantic"
        else:
            return "unknown"

    @classmethod
    def detect_semantic_type(cls, file_path: Path) -> Optional[str]:
        """
        Detect the specific semantic model type based on root key.

        This method first attempts to parse the YAML file to check for
        specific root keys. If parsing fails (e.g., due to Jinja templates),
        it falls back to string pattern matching.

        Args:
            file_path: Path to the YAML file

        Returns:
            Semantic type (e.g., 'metrics', 'relationships', 'dbt') or None
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # First attempt: Try to parse as valid YAML
            detected_type = cls._detect_from_parsed_yaml(content)
            if detected_type:
                return detected_type

            # Second attempt: String-based detection for files with templates
            detected_type = cls._detect_from_content_patterns(content)
            if detected_type:
                return detected_type

            return None

        except Exception as e:
            logger.debug(f"Error detecting semantic type for {file_path}: {e}")
            return None

    @classmethod
    def _detect_from_parsed_yaml(cls, content: str) -> Optional[str]:
        """
        Attempt to detect file type by parsing YAML.

        This works for files without Jinja templates.
        """
        try:
            data = yaml.safe_load(content)
            if data:
                # Check each known root key
                for key, semantic_type in cls.SEMANTIC_TYPE_KEYS.items():
                    if key in data:
                        return semantic_type
        except yaml.YAMLError:
            # Expected for files with Jinja templates
            pass

        return None

    @classmethod
    def _detect_from_content_patterns(cls, content: str) -> Optional[str]:
        """
        Detect file type using string patterns.

        This fallback method works even when files contain Jinja templates
        that make them invalid YAML.
        """
        # Check for each pattern in content
        for pattern, semantic_type in cls.SEMANTIC_TYPE_PATTERNS.items():
            if pattern in content:
                return semantic_type

        return None

    @classmethod
    def is_semantic_model(cls, file_path: Path) -> bool:
        """
        Check if a file is a semantic model file.

        Args:
            file_path: Path to check

        Returns:
            True if the file is a semantic model file
        """
        semantic_type = cls.detect_semantic_type(file_path)
        return semantic_type in [
            "metrics",
            "relationships",
            "filters",
            "custom_instructions",
            "verified_queries",
            "semantic_views",
        ]

    @classmethod
    def is_dbt_model(cls, file_path: Path) -> bool:
        """
        Check if a file is a dbt model file.

        Args:
            file_path: Path to check

        Returns:
            True if the file is a dbt model file
        """
        return cls.detect_semantic_type(file_path) == "dbt"
