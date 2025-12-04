"""
Configuration Validator

Validates sst_config.yml configuration files to ensure all required fields are present
and provides recommendations for optional fields. Integrates with the event system
for user-facing messages and logging system for technical debugging.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.events import fire_event
from snowflake_semantic_tools.shared.events.types import ConfigValidationError, ConfigValidationWarning
from snowflake_semantic_tools.shared.utils.logger import get_logger

# Logger: Technical details for debugging
logger = get_logger(__name__)


def _get_nested_value(config: Dict[str, Any], field_path: str) -> Any:
    """
    Get nested value from config dictionary using dot-notation path.

    Args:
        config: Configuration dictionary
        field_path: Dot-notation path (e.g., "project.semantic_models_dir")

    Returns:
        Value if found, None otherwise
    """
    keys = field_path.split(".")
    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value


def validate_config(
    config: Dict[str, Any], config_path: Optional[Path] = None
) -> Tuple[bool, List[str], List[Tuple[str, str, Any]]]:
    """
    Validate configuration has all required fields and identify missing optional fields.

    Args:
        config: Configuration dictionary to validate
        config_path: Optional path to config file (for logging context)

    Returns:
        Tuple of (is_valid, missing_required_fields, missing_optional_fields).
        Missing optional fields are tuples of (field_path, message, default_value).
    """
    missing_required = []
    missing_optional = []

    # Required fields (error if missing)
    required_checks = [
        (
            "project.semantic_models_dir",
            "Required: Directory containing semantic model YAML files (metrics, relationships)",
        ),
        ("project.dbt_models_dir", "Required: Directory containing dbt model YAML files"),
    ]

    for field_path, message in required_checks:
        if _get_nested_value(config, field_path) is None:
            missing_required.append(field_path)

    # Optional fields with defaults (warn if missing)
    optional_checks = [
        ("validation.strict", "Recommended: Set to true in CI/CD to block deployments on warnings", False),
        ("validation.exclude_dirs", "Recommended: List of directories to exclude from validation", []),
        ("enrichment.distinct_limit", "Optional: Number of distinct values to fetch during enrichment", 25),
        ("enrichment.sample_values_display_limit", "Optional: Number of sample values to show in YAML files", 10),
        ("enrichment.synonym_model", "Optional: LLM model for synonym generation", "openai-gpt-5"),
        ("enrichment.synonym_max_count", "Optional: Maximum synonyms per table/column", 4),
    ]

    for field_path, message, default_value in optional_checks:
        if _get_nested_value(config, field_path) is None:
            missing_optional.append((field_path, message, default_value))

    # Log technical details (not shown to users)
    if missing_required or missing_optional:
        logger.debug(
            f"Config validation results: {len(missing_required)} required missing, {len(missing_optional)} optional missing"
        )
        if config_path:
            logger.debug(f"Config file: {config_path}")

    return (len(missing_required) == 0, missing_required, missing_optional)


def _sanitize_config_for_logging(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove sensitive information from config before logging.

    Args:
        config: Configuration dictionary to sanitize

    Returns:
        New dictionary with sensitive fields removed (passwords, keys, etc.)
    """
    sanitized = config.copy()

    # Remove any potential secret fields (none currently, but future-proof)
    # If we add secrets later, they would go in snowflake.credentials section
    if "snowflake" in sanitized and isinstance(sanitized["snowflake"], dict):
        snowflake_copy = sanitized["snowflake"].copy()
        # Don't log credentials
        snowflake_copy.pop("password", None)
        snowflake_copy.pop("private_key", None)
        snowflake_copy.pop("private_key_path", None)
        sanitized["snowflake"] = snowflake_copy

    return sanitized


def validate_and_report_config(
    config: Dict[str, Any], config_path: Optional[Path] = None, fail_on_errors: bool = True
) -> bool:
    """
    Validate configuration and report issues via events (CLI) and logger (technical).

    Validates configuration and reports through dual channels: events for CLI messages
    and logger for debugging. Sanitized config values are logged as JSON to logs/sst.log.
    Should be called at the start of CLI commands.

    Args:
        config: Configuration dictionary to validate
        config_path: Optional path to config file (for error context)
        fail_on_errors: If True, raise SystemExit when required fields are missing

    Returns:
        True if config is valid, False otherwise

    Raises:
        SystemExit: If fail_on_errors is True and required fields are missing
    """
    # Log config values to structured log (sanitized - remove secrets if any)
    config_for_logging = _sanitize_config_for_logging(config.copy())
    logger.debug(f"Config validation starting. Config values: {json.dumps(config_for_logging, indent=2)}")
    if config_path:
        logger.debug(f"Config file path: {config_path}")

    is_valid, missing_required, missing_optional = validate_config(config, config_path)

    # Report missing required fields (errors)
    for field in missing_required:
        message = _get_field_message(field)
        # Event: User-facing error message
        fire_event(ConfigValidationError(field=field, message_text=message))

    # Report missing optional fields (warnings)
    for field_path, message, default_value in missing_optional:
        # Event: User-facing warning message
        fire_event(ConfigValidationWarning(field=field_path, message_text=message, default_value=default_value))

    # If required fields missing, exit with error
    if not is_valid and fail_on_errors:
        error_msg = _build_helpful_error_message(missing_required, config_path)
        # Log technical details
        logger.error(f"Config validation failed: {', '.join(missing_required)}")
        raise SystemExit(error_msg)

    return is_valid


def validate_cli_config(fail_on_errors: bool = True) -> bool:
    """
    Validate configuration for CLI commands with automatic config loading.

    Convenience function that automatically loads config and finds the config file.
    Recommended for use in CLI commands. If config file is missing, fails immediately
    with a clear error message.

    Args:
        fail_on_errors: If True, raise SystemExit when required fields are missing

    Returns:
        True if config is valid, False otherwise

    Raises:
        SystemExit: If fail_on_errors is True and config file is missing or required fields are missing
    """
    config = get_config()
    config_path = config._find_config_file() if hasattr(config, "_find_config_file") else None

    # If config file is missing entirely, provide helpful error
    if config_path is None:
        error_msg = (
            "\nConfiguration file not found: sst_config.yml\n\n"
            "SST requires sst_config.yml in your dbt project root.\n"
            "See sst_config.yml.example for the required configuration format.\n"
        )
        logger.error("Config file (sst_config.yml) not found. Validation cannot proceed.")
        if fail_on_errors:
            raise SystemExit(error_msg)
        return False

    return validate_and_report_config(
        config._config if hasattr(config, "_config") else {}, config_path=config_path, fail_on_errors=fail_on_errors
    )


def _get_field_message(field_path: str) -> str:
    """
    Get user-friendly description message for a config field.

    Args:
        field_path: Dot-notation path to the field (e.g., "project.semantic_models_dir")

    Returns:
        User-friendly description of the field, or fallback message if not found
    """
    messages = {
        "project.semantic_models_dir": "Directory containing semantic model YAML files (metrics, relationships)",
        "project.dbt_models_dir": "Directory containing dbt model YAML files",
        "validation.strict": "Set to true in CI/CD to block deployments on warnings",
        "validation.exclude_dirs": "List of directories to exclude from validation",
        "enrichment.distinct_limit": "Number of distinct values to fetch during enrichment",
        "enrichment.sample_values_display_limit": "Number of sample values to show in YAML files",
        "enrichment.synonym_model": "LLM model for synonym generation",
        "enrichment.synonym_max_count": "Maximum synonyms per table/column",
    }
    return messages.get(field_path, "Configuration field")


def _build_helpful_error_message(missing_fields: List[str], config_path: Optional[Path] = None) -> str:
    """
    Generate formatted error message with actionable guidance for missing config fields.

    Args:
        missing_fields: List of dot-notation paths for missing required fields
        config_path: Optional path to config file (for display in error)

    Returns:
        Formatted multi-line error message with list of missing fields and code snippet
    """
    path_str = str(config_path) if config_path else "sst_config.yml"
    message = f"\nConfiguration incomplete: {path_str}\n\n"
    message += "Missing required fields:\n"

    for field in missing_fields:
        message += f"  - {field}\n"

    message += "\nAdd these to your sst_config.yml:\n\n"

    # Group by section
    project_fields = [f for f in missing_fields if f.startswith("project.")]
    if project_fields:
        message += "project:\n"
        if "project.semantic_models_dir" in project_fields:
            message += '  semantic_models_dir: "snowflake_semantic_models"  # Required\n'
        if "project.dbt_models_dir" in project_fields:
            message += '  dbt_models_dir: "models"  # Required\n'
        message += "\n"

    return message
