"""
Snowflake to SST Data Type Mappings

Maps Snowflake data types (from DESCRIBE TABLE) to semantic view data_type values.
Returns UPPERCASE Snowflake type names as required by semantic view YAML specification.

Note: Semantic views expect Snowflake's native type names (TEXT, NUMBER, DATE, etc.)
in uppercase, not custom type aliases.

References:
- Snowflake Data Types: https://docs.snowflake.com/en/sql-reference/data-types
- Snowflake Type Aliases: https://docs.snowflake.com/en/sql-reference/data-types-numeric#label-data-types-for-fixed-point-numbers
- Semantic Views Spec: https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view
"""

# String Types → Normalized to TEXT
SNOWFLAKE_STRING_TYPES = {
    "VARCHAR": "TEXT",
    "CHAR": "TEXT",
    "CHARACTER": "TEXT",
    "STRING": "TEXT",
    "TEXT": "TEXT",
}

# Numeric Types → Keep Snowflake type names
SNOWFLAKE_NUMERIC_TYPES = {
    "NUMBER": "NUMBER",
    "DECIMAL": "NUMBER",  # Snowflake DECIMAL is alias for NUMBER
    "NUMERIC": "NUMBER",  # Snowflake NUMERIC is alias for NUMBER
    "INT": "NUMBER",  # Snowflake INT is alias for NUMBER(38,0)
    "INTEGER": "NUMBER",  # Snowflake INTEGER is alias for NUMBER(38,0)
    "BIGINT": "NUMBER",  # Snowflake BIGINT is alias for NUMBER(38,0)
    "SMALLINT": "NUMBER",  # Snowflake SMALLINT is alias for NUMBER(38,0)
    "TINYINT": "NUMBER",  # Snowflake TINYINT is alias for NUMBER(38,0)
    "BYTEINT": "NUMBER",  # Snowflake BYTEINT is alias for NUMBER(38,0)
    "FLOAT": "FLOAT",
    "FLOAT4": "FLOAT",  # Alias for FLOAT
    "FLOAT8": "FLOAT",  # Alias for FLOAT
    "DOUBLE": "FLOAT",  # Snowflake DOUBLE is alias for FLOAT
    "DOUBLE PRECISION": "FLOAT",  # Alias for FLOAT
    "REAL": "FLOAT",  # Alias for FLOAT
}

# Date/Time Types → Keep Snowflake type names
SNOWFLAKE_DATETIME_TYPES = {
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP_NTZ",  # DATETIME is alias for TIMESTAMP_NTZ
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP_NTZ",  # Default TIMESTAMP is TIMESTAMP_NTZ
    "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    "TIMESTAMP_TZ": "TIMESTAMP_TZ",
}

# Other Types → Snowflake type names
SNOWFLAKE_OTHER_TYPES = {
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "VARIANT": "VARIANT",
    "OBJECT": "OBJECT",
    "ARRAY": "ARRAY",
    "BINARY": "TEXT",  # Treat as text in semantic views
    "VARBINARY": "TEXT",  # Treat as text in semantic views
    "GEOGRAPHY": "GEOGRAPHY",
    "GEOMETRY": "GEOMETRY",
    "VECTOR": "ARRAY",  # AI/ML vector embeddings
}


def map_snowflake_to_sst_datatype(snowflake_type: str) -> str:
    """
    Maps Snowflake data type from DESCRIBE TABLE to semantic view data_type.

    Returns normalized UPPERCASE Snowflake type names for semantic view compatibility.
    Handles precision/scale specifications (e.g., NUMBER(10,2) → NUMBER).

    Args:
        snowflake_type: Raw Snowflake data type (e.g., "NUMBER(10,2)", "VARCHAR(255)")

    Returns:
        Uppercase Snowflake type name (e.g., "NUMBER", "TEXT", "TIMESTAMP_NTZ")

    Examples:
        >>> map_snowflake_to_sst_datatype("VARCHAR(255)")
        "TEXT"
        >>> map_snowflake_to_sst_datatype("NUMBER(10,2)")
        "NUMBER"
        >>> map_snowflake_to_sst_datatype("TIMESTAMP_NTZ(9)")
        "TIMESTAMP_NTZ"
    """
    # Normalize to uppercase and handle precision patterns
    normalized_type = snowflake_type.upper().strip()

    # Remove precision/scale for mapping (e.g., "NUMBER(10,2)" -> "NUMBER")
    if "(" in normalized_type:
        base_type = normalized_type.split("(")[0]
    else:
        base_type = normalized_type

    # Direct mapping for exact matches
    all_mappings = {
        **SNOWFLAKE_STRING_TYPES,
        **SNOWFLAKE_NUMERIC_TYPES,
        **SNOWFLAKE_DATETIME_TYPES,
        **SNOWFLAKE_OTHER_TYPES,
    }

    if base_type in all_mappings:
        return all_mappings[base_type]

    # Fallback to TEXT for unknown types (safe for semantic views)
    print(f"Warning: Unknown Snowflake type '{snowflake_type}', defaulting to 'TEXT'")
    return "TEXT"


def determine_column_type(column_name: str, snowflake_data_type: str, existing_column_type: str = None) -> str:
    """
    Determines SST column_type based on data type and naming patterns.

    Classifies columns as dimension, fact, or time_dimension based on their
    Snowflake data type. Never overwrites existing column_type values.

    Args:
        column_name: Name of the column
        snowflake_data_type: Snowflake data type
        existing_column_type: Existing column_type value (preserves if set)

    Returns:
        str: SST column_type ('dimension', 'fact', or 'time_dimension')
    """
    # Never overwrite existing column_type
    if existing_column_type:
        return existing_column_type

    # Get normalized data type for classification
    normalized_type = map_snowflake_to_sst_datatype(snowflake_data_type)
    column_name_lower = column_name.lower()

    # Time dimension: temporal data types
    temporal_types = ["DATE", "TIME", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"]
    if normalized_type in temporal_types:
        return "time_dimension"

    # Fact: numeric types but NOT boolean-named columns (is_*, has_*)
    numeric_types = ["NUMBER", "FLOAT"]
    if normalized_type in numeric_types and not (
        column_name_lower.startswith("is_") or column_name_lower.startswith("has_")
    ):
        return "fact"

    # Dimension: everything else (TEXT, BOOLEAN, VARIANT, ARRAY, etc.)
    return "dimension"
