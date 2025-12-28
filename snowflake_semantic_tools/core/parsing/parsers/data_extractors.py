#!/usr/bin/env python3
"""
Data Extraction Utilities

Pure utility functions for extracting structured data from YAML content.
These functions handle the transformation from raw YAML dictionaries to our target data structures.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.shared import get_logger

logger = get_logger("yaml_parser.data_extractors")


def extract_table_info(
    model: Dict[str, Any], file_path: Path, target_database: Optional[str] = None, manifest_parser=None
) -> Optional[Dict[str, Any]]:
    """
    Extract table-level information from a dbt model dictionary.

    Args:
        model: Model dictionary from YAML
        file_path: Source file path for reference
        target_database: Optional target database to use instead of SST metadata database
        manifest_parser: Optional ManifestParser for auto-detecting database/schema

    Returns:
        Table record dictionary or None if extraction fails
    """
    try:
        # Basic model info
        name = model.get("name")
        if not name:
            logger.debug(f"Model without name in {file_path}")
            return None

        description = model.get("description", "")

        # Extract SST metadata if present
        meta = model.get("meta", {})
        sst_meta = meta.get("sst", {})

        # Extract primary keys and apply upper case formatting
        primary_keys = extract_primary_key(sst_meta)
        primary_keys_upper = [pk.upper() if isinstance(pk, str) else pk for pk in primary_keys]

        # Extract unique keys and apply upper case formatting
        unique_keys = extract_unique_keys(sst_meta)
        unique_keys_upper = [uk.upper() if isinstance(uk, str) else uk for uk in unique_keys]

        # Database and Schema Resolution:
        # ONLY source: manifest.json (dbt's compiled output)
        # The --database flag (target_database) is ONLY for defer mechanism (environment override)
        # YAML values are COMPLETELY IGNORED

        # Warn if database/schema are in YAML (they will be ignored)
        if sst_meta.get("database"):
            logger.warning(
                f"Model '{name}' has database in meta.sst - this is IGNORED. Remove it. Database comes from manifest.json only."
            )
        if sst_meta.get("schema"):
            logger.warning(
                f"Model '{name}' has schema in meta.sst - this is IGNORED. Remove it. Schema comes from manifest.json only."
            )

        if target_database:
            # Defer mechanism: override database for environment deployment
            database = target_database.upper()
            logger.debug(f"Using target_database '{target_database}' (defer mechanism) for table '{name}'")
        elif manifest_parser and manifest_parser.manifest:
            # Normal operation: read from manifest
            location = manifest_parser.get_location(name)
            if location:
                database = location["database"]
                logger.debug(f"Database from manifest for '{name}': {database}")
            else:
                database = ""
        else:
            database = ""

        # Schema ALWAYS comes from manifest (no override mechanism)
        schema = ""
        if manifest_parser and manifest_parser.manifest:
            location = manifest_parser.get_location(name)
            if location:
                schema = location["schema"].upper()
                logger.debug(f"Schema from manifest for '{name}': {schema}")

        # Build table record with uppercase formatting for specified fields
        # Write BOTH new and old field names for database backward compatibility
        table_record = {
            "table_name": sst_meta.get("table", name).upper() if sst_meta.get("table", name) else name.upper(),
            "database": database,
            "schema": schema.upper() if schema else "",
            "description": description,
            "primary_key": primary_keys_upper,
            "unique_keys": unique_keys_upper,
            "synonyms": sst_meta.get("synonyms", []),
            "model_name": name,  # Store the original model name
            "file_path": str(file_path),  # Store the file path for validation
            "cortex_searchable": sst_meta.get("cortex_searchable", False),
        }

        return table_record

    except Exception as e:
        logger.error(f"Error extracting table info from {file_path}: {e}")
        return None


def extract_primary_key(sst_meta: Dict[str, Any]) -> List[str]:
    """
    Extract primary key information from SST metadata.

    Handles both list format and comma-separated string format:
    - primary_key: [calendar_date, user_id]
    - primary_key: "calendar_date, user_id"

    Args:
        sst_meta: SST metadata dictionary (meta.sst)

    Returns:
        List of primary key column names
    """
    pk_from_sst = sst_meta.get("primary_key")
    if pk_from_sst:
        if isinstance(pk_from_sst, str):
            # Check if it's a comma-separated string
            if "," in pk_from_sst:
                # Split on comma and strip whitespace from each key
                return [key.strip() for key in pk_from_sst.split(",")]
            else:
                return [pk_from_sst.strip()]
        elif isinstance(pk_from_sst, list):
            # Ensure each item is stripped of whitespace
            return [str(key).strip() for key in pk_from_sst]
    return []


def extract_unique_keys(sst_meta: Dict[str, Any]) -> List[str]:
    """
    Extract unique key information from SST metadata.

    Handles both list format and comma-separated string format:
    - unique_keys: [customer_id, ordered_at]
    - unique_keys: "customer_id, ordered_at"

    Args:
        sst_meta: SST metadata dictionary (meta.sst)

    Returns:
        List of unique key column names
    """
    uk_from_sst = sst_meta.get("unique_keys")
    if uk_from_sst:
        if isinstance(uk_from_sst, str):
            # Check if it's a comma-separated string
            if "," in uk_from_sst:
                # Split on comma and strip whitespace from each key
                return [key.strip() for key in uk_from_sst.split(",")]
            else:
                return [uk_from_sst.strip()]
        elif isinstance(uk_from_sst, list):
            # Ensure each item is stripped of whitespace
            return [str(key).strip() for key in uk_from_sst]
    return []


def extract_column_info(column: Dict[str, Any], table_name: str, file_path: Path) -> Dict[str, Any]:
    """
    Extract column-level information from a column dictionary.

    Args:
        column: Column dictionary from YAML
        table_name: Name of the parent table
        file_path: Source file path for reference

    Returns:
        Column record dictionary
    """
    try:
        name = column.get("name", "")
        description = column.get("description", "")

        # Extract SST metadata
        meta = column.get("meta", {})
        sst_meta = meta.get("sst", {})

        # Build column record
        # Note: Column names uppercased to match Snowflake identifier behavior
        column_record = {
            "table_name": table_name,
            "name": name.upper() if name else name,
            "expr": name.upper() if name else name,  # expr is just the column name
            "column_type": sst_meta.get("column_type"),  # Extract column_type from meta.sst
            "data_type": sst_meta.get("data_type", "text"),
            "description": description,
            "synonyms": sst_meta.get("synonyms", []),
            "sample_values": sst_meta.get("sample_values", []),
            "is_enum": sst_meta.get("is_enum", False),
        }

        return column_record

    except Exception as e:
        logger.error(f"Error extracting column info from {file_path}: {e}")
        return {}


def get_column_type(column: Dict[str, Any]) -> str:
    """
    Get the column type (dimension, time, fact) from metadata.

    NOTE: column_type is now REQUIRED - no defaults applied.

    Args:
        column: Column dictionary from YAML

    Returns:
        Column type string ('dimension', 'time', or 'fact'), or empty string if missing
    """
    # Extract SST metadata
    meta = column.get("meta", {})
    sst_meta = meta.get("sst", {})
    column_type = sst_meta.get("column_type", "")  # No default - must be explicit

    # Normalize the column type if provided
    if column_type in ["dimension"]:
        return "dimension"
    elif column_type in ["time_dimension", "time", "date", "timestamp"]:
        return "time"
    elif column_type in ["fact", "measure", "metric"]:
        return "fact"
    else:
        # Return empty string if missing or invalid - validation will catch this
        return ""


def apply_uppercase_formatting(data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    """
    Apply uppercase formatting to specified fields in a data dictionary.

    Args:
        data: Data dictionary to format
        fields: List of field names to uppercase

    Returns:
        New dictionary with specified fields uppercased
    """
    formatted_data = data.copy()

    for field in fields:
        if field in formatted_data and isinstance(formatted_data[field], str):
            formatted_data[field] = formatted_data[field].upper()

    return formatted_data
