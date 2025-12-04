#!/usr/bin/env python3
"""
dbt YAML Parser

Extracts physical table metadata from dbt model definitions.

Processes dbt YAML files to build the physical layer foundation that semantic
models reference. Extracts:
- Table locations (database, schema, table name)
- Column definitions with data types and descriptions
- Primary key specifications for relationship validation
- SST metadata for semantic layer integration

The dbt catalog serves as the source of truth for physical schema validation,
ensuring semantic models reference valid tables and columns.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from snowflake_semantic_tools.core.parsing.parsers.data_extractors import (
    apply_uppercase_formatting,
    extract_column_info,
    extract_table_info,
    get_column_type,
)
from snowflake_semantic_tools.core.parsing.parsers.error_handler import ErrorTracker, format_yaml_error
from snowflake_semantic_tools.shared import get_logger

logger = get_logger("yaml_parser.dbt_parser")


def get_empty_result() -> Dict[str, List[Dict[str, Any]]]:
    """Return an empty result structure for all table types."""
    return {
        "sm_tables": [],
        "sm_dimensions": [],
        "sm_time_dimensions": [],
        "sm_facts": [],
        "sm_metrics": [],
        "sm_filters": [],
        "sm_relationships": [],
        "sm_relationship_columns": [],
        "sm_verified_queries": [],
        "sm_custom_instructions": [],
        "sm_table_summaries": [],
        "sm_semantic_views": [],
    }


def parse_dbt_yaml_file(
    file_path: Path, error_tracker: ErrorTracker, target_database: Optional[str] = None, manifest_parser=None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse a single dbt YAML file and extract model information.

    Args:
        file_path: Path to the YAML file
        error_tracker: Error tracker to record any parsing errors
        target_database: Optional target database to use for table references
        manifest_parser: Optional ManifestParser for auto-detecting database/schema

    Returns:
        Dictionary with extracted data for each table type
    """
    logger.debug(f"Parsing dbt YAML file: {file_path}")

    try:
        # Load YAML content
        with open(file_path, "r", encoding="utf-8") as f:
            yaml_content = yaml.safe_load(f)

        if not yaml_content:
            logger.debug(f"Empty YAML file: {file_path}")
            return get_empty_result()

        # Extract models
        models = yaml_content.get("models", [])
        if not models:
            logger.debug(f"No models found in {file_path}")
            return get_empty_result()

        # Process each model
        result = get_empty_result()

        for model in models:
            model_data = parse_single_model(model, file_path, target_database, manifest_parser)

            # Merge results
            for key in result:
                result[key].extend(model_data[key])

        logger.debug(f"Successfully parsed {len(models)} models from {file_path}")
        return result

    except yaml.YAMLError as e:
        # Clean up YAML error message
        error_msg = format_yaml_error(e, file_path)
        logger.debug(error_msg)
        error_tracker.add_error(error_msg)
        return get_empty_result()

    except Exception as e:
        error_msg = f"Unexpected error parsing {file_path}: {e}"
        logger.error(error_msg)
        error_tracker.add_error(error_msg)
        return get_empty_result()


def parse_single_model(
    model: Dict[str, Any], file_path: Path, target_database: Optional[str] = None, manifest_parser=None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse a single model from the YAML and extract all relevant data.

    Args:
        model: Model dictionary from YAML
        file_path: Source file path for reference
        target_database: Optional target database to use for table references
        manifest_parser: Optional ManifestParser for auto-detecting database/schema

    Returns:
        Extracted data for all table types
    """
    result = get_empty_result()

    # Extract cortex_searchable flag
    meta = model.get("meta", {})
    sst_meta = meta.get("sst", {})
    cortex_searchable = sst_meta.get("cortex_searchable", False)

    model_name = model.get("name", "unknown")
    logger.debug(f"Processing model '{model_name}' - cortex_searchable={cortex_searchable}")

    # Always extract table info for validation purposes
    # Extract table-level information
    table_info = extract_table_info(model, file_path, target_database, manifest_parser)
    if table_info:
        # Add cortex_searchable flag to table info
        table_info["cortex_searchable"] = cortex_searchable
        result["sm_tables"].append(table_info)

    # Log when processing models with cortex_searchable=False
    if not cortex_searchable:
        logger.debug(
            f"Extracting all metadata for model '{model_name}' despite cortex_searchable=False (for validation)"
        )

    # ALWAYS extract column information for validation purposes
    # The cortex_searchable flag will be used later during extraction/loading
    # Extract column information
    columns = model.get("columns", [])
    table_name = model.get("name", "unknown")

    for column in columns:
        column_data = extract_column_info(column, table_name, file_path)

        # Get column_type for classification (preserve original in column_data for validation)
        column_type = column_data.get("column_type")

        # Normalize for internal processing (but keep original in column_data)
        column_type_normalized = column_type
        if column_type_normalized == "time_dimension":
            column_type_normalized = "time"  # Only for categorization, not validation

        # Apply uppercase formatting and categorize
        if column_type_normalized == "dimension":
            dimension_data = apply_uppercase_formatting(column_data, ["table_name", "name", "expr", "data_type"])
            result["sm_dimensions"].append(dimension_data)

        elif column_type_normalized == "time":
            time_dimension_data = apply_uppercase_formatting(column_data, ["table_name", "name", "expr", "data_type"])
            # Remove 'is_enum' field from time dimension data as it's not supported
            time_dimension_data.pop("is_enum", None)
            result["sm_time_dimensions"].append(time_dimension_data)

        elif column_type_normalized == "fact":
            fact_data = apply_uppercase_formatting(column_data, ["table_name", "name", "expr", "data_type"])
            # Remove fields that don't apply to facts
            fact_data.pop("is_enum", None)
            result["sm_facts"].append(fact_data)

        else:
            # Skip columns with missing or invalid column_type - validation will catch this
            logger.debug(
                f"Skipping column '{column.get('name', 'unknown')}' with missing/invalid column_type during extraction"
            )

    return result


def parse_multiple_dbt_files(
    file_paths: List[Path], error_tracker: ErrorTracker, target_database: Optional[str] = None, manifest_parser=None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse multiple dbt YAML files and aggregate results.

    Args:
        file_paths: List of file paths to parse
        error_tracker: Error tracker to record any parsing errors
        target_database: Optional target database to use for table references
        manifest_parser: Optional ManifestParser for auto-detecting database/schema

    Returns:
        Aggregated results from all files
    """
    logger.info(f"Parsing {len(file_paths)} dbt YAML files")

    # Initialize aggregated results
    aggregated_results = get_empty_result()

    # Parse each file
    for file_path in file_paths:
        try:
            file_results = parse_dbt_yaml_file(file_path, error_tracker, target_database, manifest_parser)

            # Aggregate results
            for table_type, records in file_results.items():
                aggregated_results[table_type].extend(records)

        except Exception as e:
            error_msg = f"Failed to parse dbt file {file_path}: {e}"
            logger.error(error_msg)
            error_tracker.add_error(error_msg)

    return aggregated_results
