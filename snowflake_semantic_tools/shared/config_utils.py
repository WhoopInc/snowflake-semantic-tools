"""
Configuration Utilities

Reusable helpers for accessing and processing SST configuration.
Provides clean, testable functions that encapsulate common config patterns.

Design Principles:
- Pure functions (no side effects)
- Type-safe with clear return types
- Well-tested in isolation
- Enterprise-grade code reuse
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.shared.config import get_config


def get_exclusion_patterns(cli_exclude: Optional[str] = None) -> Optional[List[str]]:
    """
    Get exclusion patterns from config with optional CLI overrides.

    Combines:
    1. Patterns from sst_config.yaml (validation.exclude_dirs)
    2. Optional CLI overrides (comma-separated string)

    Deduplicates while preserving order (config patterns first, then CLI).

    Args:
        cli_exclude: Optional comma-separated CLI exclusions (e.g., "temp,backup")

    Returns:
        Combined list of exclusion patterns, or None if empty

    Examples:
        # Config only
        >>> get_exclusion_patterns()
        ['models/amplitude/*', 'models/analytics_mart/*']

        # Config + CLI
        >>> get_exclusion_patterns("temp,experimental")
        ['models/amplitude/*', 'models/analytics_mart/*', 'temp', 'experimental']

        # CLI only (no config)
        >>> get_exclusion_patterns("backup")
        ['backup']
    """
    config = get_config()

    # Start with config file exclusions
    config_excludes = config.get_exclude_dirs() or []
    exclude_dirs = config_excludes.copy()

    # Add CLI exclusions if provided
    if cli_exclude:
        cli_excludes = [d.strip() for d in cli_exclude.split(",") if d.strip()]
        exclude_dirs.extend(cli_excludes)

    # Remove duplicates while preserving order (dict.fromkeys maintains insertion order)
    if exclude_dirs:
        exclude_dirs = list(dict.fromkeys(exclude_dirs))
        return exclude_dirs

    return None


def get_enrichment_limits() -> Dict[str, int]:
    """
    Get enrichment configuration limits from config.

    Returns dictionary with:
    - distinct_limit: Number of distinct values to fetch
    - sample_values_display_limit: Sample values to show in YAML

    Uses config values with sensible defaults if not configured.

    Returns:
        Dictionary with enrichment limits

    Example:
        >>> limits = get_enrichment_limits()
        >>> limits['distinct_limit']
        25
        >>> limits['sample_values_display_limit']
        10
    """
    config = get_config()
    enrichment_config = config.get("enrichment", {})

    return {
        "distinct_limit": enrichment_config.get("distinct_limit", 25),
        "sample_values_display_limit": enrichment_config.get("sample_values_display_limit", 10),
    }


def get_synonym_config() -> Dict[str, Any]:
    """
    Get synonym generation configuration from config.

    Returns dictionary with:
    - model: LLM model to use (e.g., 'openai-gpt-4.1', 'claude-4-sonnet')
    - max_count: Maximum synonyms per table/column

    Uses config values with sensible defaults.

    Returns:
        Dictionary with synonym configuration

    Example:
        >>> config = get_synonym_config()
        >>> config['model']
        'openai-gpt-4.1'
        >>> config['max_count']
        4
    """
    config = get_config()
    enrichment_config = config.get("enrichment", {})

    return {
        "model": enrichment_config.get("synonym_model", "openai-gpt-4.1"),
        "max_count": enrichment_config.get("synonym_max_count", 4),
    }


def get_project_paths() -> Dict[str, Path]:
    """
    Get project directory paths from config.

    Returns dictionary with:
    - dbt_models_dir: Path to dbt models directory
    - semantic_models_dir: Path to semantic models directory
    - manifest_path: Path to manifest.json (optional)

    All paths are resolved relative to current working directory.

    Returns:
        Dictionary with project paths

    Example:
        >>> paths = get_project_paths()
        >>> paths['dbt_models_dir']
        PosixPath('/Users/.../analytics-dbt/models')
        >>> paths['semantic_models_dir']
        PosixPath('/Users/.../analytics-dbt/snowflake_semantic_models')
    """
    config = get_config()
    project_config = config.get("project", {})

    cwd = Path.cwd()

    paths = {
        "dbt_models_dir": cwd / project_config.get("dbt_models_dir", "models"),
        "semantic_models_dir": cwd / project_config.get("semantic_models_dir", "snowflake_semantic_models"),
    }

    # Optional manifest path
    if "manifest_path" in project_config:
        paths["manifest_path"] = cwd / project_config["manifest_path"]
    else:
        paths["manifest_path"] = cwd / "target" / "manifest.json"

    return paths


def is_strict_mode() -> bool:
    """
    Check if strict validation mode is enabled.

    In strict mode, warnings block deployment (treated as errors).
    Useful for CI/CD pipelines that require zero issues.

    Returns:
        True if strict mode enabled, False otherwise

    Example:
        >>> is_strict_mode()
        False  # Default unless configured otherwise
    """
    config = get_config()
    validation_config = config.get("validation", {})
    return validation_config.get("strict", False)


def get_exclusion_summary(cli_exclude: Optional[str] = None) -> Dict[str, Any]:
    """
    Get summary of exclusion patterns for display/debugging.

    Returns detailed breakdown of where patterns come from:
    - config_patterns: From sst_config.yaml
    - cli_patterns: From CLI flags
    - total_patterns: Combined (deduplicated)

    Args:
        cli_exclude: Optional CLI exclusion string

    Returns:
        Dictionary with exclusion summary

    Example:
        >>> summary = get_exclusion_summary("temp,backup")
        >>> summary['config_patterns']
        ['models/amplitude/*', 'models/analytics_mart/*']
        >>> summary['cli_patterns']
        ['temp', 'backup']
        >>> summary['total_patterns']
        ['models/amplitude/*', 'models/analytics_mart/*', 'temp', 'backup']
    """
    config = get_config()
    config_patterns = config.get_exclude_dirs() or []

    cli_patterns = []
    if cli_exclude:
        cli_patterns = [d.strip() for d in cli_exclude.split(",") if d.strip()]

    # Get combined (deduplicated)
    all_patterns = get_exclusion_patterns(cli_exclude) or []

    return {
        "config_patterns": config_patterns,
        "cli_patterns": cli_patterns,
        "total_patterns": all_patterns,
        "has_exclusions": len(all_patterns) > 0,
        "total_count": len(all_patterns),
    }
