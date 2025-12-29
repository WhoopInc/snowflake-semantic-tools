"""
Shared Module

Essential cross-cutting utilities and infrastructure for the semantic tools ecosystem.

## Components

### Utilities (utils/)
- **logger**: Centralized logging infrastructure
- **file_utils**: File discovery for dbt and semantic model YAML files
- **character_sanitizer**: Character sanitization for SQL, YAML, Jinja contexts

### Configuration
Flexible configuration system supporting sst_config.yml and .env files with clear precedence.

### Events
Event system for unified logging and CLI output.

Usage:
    from snowflake_semantic_tools.shared import get_logger, get_config
    from snowflake_semantic_tools.shared.utils import find_dbt_model_files, CharacterSanitizer

    logger = get_logger("my_module")
    config = get_config()
"""

# Version is defined in _version.py (single source of truth)
from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.shared.config import Config, get_config
from snowflake_semantic_tools.shared.config_utils import (
    get_enrichment_limits,
    get_exclusion_patterns,
    get_exclusion_summary,
    get_project_paths,
    get_synonym_config,
    is_strict_mode,
)
from snowflake_semantic_tools.shared.progress import (
    CLIProgressCallback,
    NoOpProgressCallback,
    ProgressCallback,
    default_progress_callback,
)
from snowflake_semantic_tools.shared.utils import get_logger

__author__ = "Matt Luizzi"

__all__ = [
    "get_logger",
    "get_config",
    "Config",
    "ProgressCallback",
    "NoOpProgressCallback",
    "CLIProgressCallback",
    "default_progress_callback",
    "get_exclusion_patterns",
    "get_enrichment_limits",
    "get_synonym_config",
    "get_project_paths",
    "is_strict_mode",
    "get_exclusion_summary",
]
