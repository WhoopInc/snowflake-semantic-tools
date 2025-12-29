"""
Validation Constants

Constants used for validating Snowflake semantic models including:
- Identifier limits and patterns
- Reserved keywords
- Valid data types
- Column types
"""

import re
from typing import Set

# =============================================================================
# IDENTIFIER VALIDATION CONSTANTS
# =============================================================================

# Per Snowflake docs: Maximum identifier length is 255 characters
# Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax
SNOWFLAKE_MAX_IDENTIFIER_LENGTH: int = 255

# Warn users about very long identifiers for readability
IDENTIFIER_WARNING_LENGTH: int = 200

# Identifier pattern per Snowflake docs
# Must start with letter or underscore, contain only alphanumeric or underscore
# Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# =============================================================================
# SQL RESERVED KEYWORDS
# =============================================================================

# SQL Reserved Keywords from Snowflake docs
# Ref: https://docs.snowflake.com/en/sql-reference/reserved-keywords
SQL_RESERVED_KEYWORDS: Set[str] = {
    # Core ANSI keywords
    "SELECT",
    "FROM",
    "WHERE",
    "JOIN",
    "ON",
    "GROUP",
    "ORDER",
    "BY",
    "HAVING",
    "UNION",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "GRANT",
    "REVOKE",
    "TABLE",
    "VIEW",
    "INDEX",
    "DATABASE",
    "SCHEMA",
    "AS",
    "IN",
    "EXISTS",
    "BETWEEN",
    "LIKE",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "ALL",
    "ANY",
    "DISTINCT",
    "FOR",
    "IS",
    "SET",
    "TO",
    "WITH",
    # Snowflake-specific
    "ILIKE",
    "MINUS",
    "QUALIFY",
    "REGEXP",
    "RLIKE",
    "SAMPLE",
    # Date/Time functions (commonly problematic)
    "DATE",
    "TIME",
    "TIMESTAMP",
    "DATETIME",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "INTERVAL",
    # Common problematic words
    "USER",
    "ROLE",
    "WAREHOUSE",
    "STAGE",
    "STREAM",
    "TASK",
    "PIPE",
    "PROCEDURE",
    "FUNCTION",
    "SEQUENCE",
    "CLUSTER",
    "COPY",
    "MERGE",
    # Limited keywords (cannot use in certain contexts)
    "CROSS",
    "FULL",
    "INNER",
    "LATERAL",
    "LEFT",
    "NATURAL",
    "RIGHT",
    "USING",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "CONSTRAINT",
    "TRIGGER",
}

# Lowercase version for case-insensitive matching
SQL_RESERVED_KEYWORDS_LOWER: Set[str] = {kw.lower() for kw in SQL_RESERVED_KEYWORDS}

# =============================================================================
# DATA TYPE CONSTANTS
# =============================================================================

# Valid Snowflake data types per official documentation
# Ref: https://docs.snowflake.com/en/sql-reference-data-types
VALID_SNOWFLAKE_DATA_TYPES: Set[str] = {
    # Numeric types
    "NUMBER",
    "DECIMAL",
    "NUMERIC",
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "BYTEINT",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "DOUBLE",
    "DOUBLE PRECISION",
    "REAL",
    "DECFLOAT",
    # String types
    "VARCHAR",
    "CHAR",
    "CHARACTER",
    "STRING",
    "TEXT",
    "NCHAR",
    "NVARCHAR",
    "NVARCHAR2",
    "CHAR VARYING",
    "NCHAR VARYING",
    "BINARY",
    "VARBINARY",
    # Date/Time types
    "DATE",
    "DATETIME",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    # Semi-structured types
    "VARIANT",
    "OBJECT",
    "ARRAY",
    # Other types
    "BOOLEAN",
    "GEOGRAPHY",
    "GEOMETRY",
}

# Lowercase version for case-insensitive matching
VALID_SNOWFLAKE_DATA_TYPES_LOWER: Set[str] = {dt.lower() for dt in VALID_SNOWFLAKE_DATA_TYPES}

# =============================================================================
# COLUMN TYPE CONSTANTS
# =============================================================================

# Valid column types in SST semantic models
VALID_COLUMN_TYPES: Set[str] = {"dimension", "fact", "time_dimension"}

# Valid ASOF column types (required for ASOF relationships)
# Ref: https://docs.snowflake.com/en/user-guide/views-semantic/sql
VALID_ASOF_COLUMN_TYPES: Set[str] = {
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "NUMBER",
}

# Lowercase version for case-insensitive matching
VALID_ASOF_COLUMN_TYPES_LOWER: Set[str] = {ct.lower() for ct in VALID_ASOF_COLUMN_TYPES}
