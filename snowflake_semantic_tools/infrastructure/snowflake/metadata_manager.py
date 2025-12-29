#!/usr/bin/env python3
"""
Metadata Manager

Handles metadata enrichment operations including schema inspection,
sample value collection, and primary key validation.

This manager provides the intelligence layer for automatic metadata enrichment,
querying Snowflake to gather structural information and statistics about tables.
"""

import re
from typing import Any, Dict, List, Set

import pandas as pd

from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer

logger = get_logger("infrastructure.snowflake.metadata_manager")

# Snowflake unquoted identifier rules: starts with letter/underscore, contains only
# alphanumeric, underscore, or dollar sign ($ is allowed but rare)
UNQUOTED_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class MetadataManager:
    """
    Manages metadata retrieval and enrichment operations.

    Provides methods to:
    - Inspect table and view schemas
    - Collect diverse sample values
    - Validate primary key uniqueness
    - Get row counts and statistics
    """

    def __init__(self, connection_manager, config):
        """
        Initialize metadata manager.

        Args:
            connection_manager: ConnectionManager instance
            config: SnowflakeConfig instance
        """
        self.connection_manager = connection_manager
        self.config = config
        self._warned_tables: Set[str] = set()  # Track tables we've warned about

    def _requires_quoting(self, identifier: str) -> bool:
        """
        Check if an identifier requires quoting for Snowflake SQL.

        Snowflake unquoted identifiers must:
        - Start with a letter (A-Z, a-z) or underscore (_)
        - Contain only letters, digits (0-9), underscores (_), or dollar signs ($)

        Args:
            identifier: The identifier to check

        Returns:
            True if the identifier needs to be double-quoted
        """
        return not bool(UNQUOTED_IDENTIFIER_PATTERN.match(identifier))

    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote an identifier if it requires quoting, preserving case.

        For identifiers with special characters (spaces, dashes, dots, etc.),
        wraps in double quotes so Snowflake treats them literally.

        Args:
            identifier: The identifier to potentially quote

        Returns:
            Quoted identifier if needed, otherwise uppercased identifier
        """
        if self._requires_quoting(identifier):
            # Escape any existing double quotes and wrap
            escaped = identifier.replace('"', '""')
            return f'"{escaped}"'
        else:
            # Standard Snowflake behavior: unquoted identifiers are uppercased
            return identifier.upper()

    def _log_quoted_identifier_warning(self, table_name: str, quoted_columns: List[str]) -> None:
        """
        Log a warning about columns that require quoted identifiers.

        Only logs once per table to avoid spam. Suggests using underscores
        for simpler SQL.

        Args:
            table_name: The table containing the columns
            quoted_columns: List of column names that required quoting
        """
        if not quoted_columns:
            return

        # Only warn once per table
        if table_name in self._warned_tables:
            return
        self._warned_tables.add(table_name)

        cols_display = ", ".join([f"'{c}'" for c in quoted_columns[:5]])
        if len(quoted_columns) > 5:
            cols_display += f", ... ({len(quoted_columns) - 5} more)"

        logger.warning(
            f"Table '{table_name}' has columns with special characters: {cols_display}. "
            f"These require quoted identifiers in Snowflake SQL. "
            f"Consider using underscores instead of dashes/dots/spaces for simpler SQL."
        )

    def get_table_schema(self, table_name: str, schema_name: str, database_name: str) -> List[Dict[str, Any]]:
        """
        Get table or view schema using DESCRIBE TABLE or DESCRIBE VIEW.

        Tries DESCRIBE TABLE first, then falls back to DESCRIBE VIEW if the object
        is a view (common for analytics_mart models).

        Args:
            table_name: Name of the table/view (case-insensitive, will be uppercased)
            schema_name: Schema containing the table/view (case-insensitive, will be uppercased)
            database_name: Database containing the table/view (case-insensitive, will be uppercased)

        Returns:
            List of column information dictionaries

        Example:
            >>> metadata_mgr.get_table_schema('users', 'public', 'analytics')
            [
                {
                    'name': 'user_id',
                    'type': 'NUMBER(38,0)',
                    'kind': 'COLUMN',
                    'null?': 'N',
                    ...
                }
            ]
        """
        # Uppercase all identifiers for Snowflake case-insensitive matching
        # Snowflake stores all unquoted identifiers as uppercase
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()

        # Try DESCRIBE TABLE first
        try:
            query = f"DESCRIBE TABLE {database_upper}.{schema_upper}.{table_upper}"
            df = self._execute_query(query)
            return df.to_dict("records") if not df.empty else []
        except Exception as e:
            error_msg = str(e).lower()
            # If it's not a table, try DESCRIBE VIEW
            if "does not exist" in error_msg or "not authorized" in error_msg:
                try:
                    query = f"DESCRIBE VIEW {database_upper}.{schema_upper}.{table_upper}"
                    df = self._execute_query(query)
                    return df.to_dict("records") if not df.empty else []
                except Exception as view_error:
                    # Provide helpful error message for permission issues
                    if "not authorized" in error_msg or "not authorized" in str(view_error).lower():
                        current_role = self.config.role or "your current role"
                        logger.error(f"Permission denied accessing {database_upper}.{schema_upper}.{table_upper}")
                        logger.error(f"Current role: {current_role}")
                        logger.error(f"")
                        logger.error(f"This usually means:")
                        logger.error(f"  • Role '{current_role}' doesn't have access to database '{database_upper}'")
                        logger.error(f"  • The database exists but requires different permissions")
                        logger.error(f"")
                        logger.error(f"Solutions:")
                        logger.error(f"  1. Set SNOWFLAKE_ROLE env var to a role with access (e.g., ACCOUNTADMIN)")
                        logger.error(
                            f"  2. Grant your role access: GRANT USAGE ON DATABASE {database_upper} TO ROLE {current_role};"
                        )
                        logger.error(f"  3. Contact your Snowflake admin for permissions")
                        logger.error(f"")
                    # Re-raise the original error for better debugging
                    raise e
            else:
                # Re-raise if it's a different error
                raise

    def get_sample_values_batch(
        self, table_name: str, schema_name: str, column_names: List[str], database_name: str, limit: int = 25
    ) -> Dict[str, List[Any]]:
        """
        Get distinct sample values for multiple columns in a SINGLE query.

        PERFORMANCE OPTIMIZATION: Uses UNION ALL to fetch samples for all columns
        in one query instead of N sequential queries. This is dramatically faster
        for tables with many columns (50+ columns: 50 queries → 1 query).

        Args:
            table_name: Name of the table (case-insensitive, will be uppercased)
            schema_name: Schema containing the table (case-insensitive, will be uppercased)
            column_names: List of column names to sample (case-insensitive, will be uppercased)
            database_name: Database containing the table (case-insensitive, will be uppercased)
            limit: Maximum number of distinct non-null values per column

        Returns:
            Dictionary mapping column names to their sample values

        Example:
            >>> metadata_mgr.get_sample_values_batch(
            ...     'users', 'public', ['status', 'country'], 'analytics'
            ... )
            {'status': ['active', 'inactive'], 'country': ['US', 'CA', 'UK']}
        """
        if not column_names:
            return {}

        # Uppercase identifiers for database/schema/table
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()

        # Track columns that need quoting for warning
        quoted_columns = [c for c in column_names if self._requires_quoting(c)]
        if quoted_columns:
            self._log_quoted_identifier_warning(table_name, quoted_columns)

        # Build UNION ALL query for all columns
        union_queries = []
        for col_name in column_names:
            # Use intelligent quoting - quotes if special chars, otherwise uppercases
            col_sql = self._quote_identifier(col_name)
            col_key = col_name.upper() if not self._requires_quoting(col_name) else col_name
            # Each subquery selects column name and values
            union_queries.append(
                f"""
                SELECT '{col_key}' as COLUMN_NAME, {col_sql} as VALUE
                FROM {database_upper}.{schema_upper}.{table_upper}
                WHERE {col_sql} IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {col_sql} ORDER BY NULL) = 1
                LIMIT {limit + 1}
            """
            )

        # Combine all queries with UNION ALL
        full_query = "\nUNION ALL\n".join(union_queries)

        try:
            df = self._execute_query(full_query)

            if df.empty:
                # Return uppercase keys for standard cols, original case for quoted cols
                return {(col.upper() if not self._requires_quoting(col) else col): [] for col in column_names}

            # Group results by column name
            results = {}
            for col_name in column_names:
                # Key matches what we used in the SELECT clause
                col_key = col_name.upper() if not self._requires_quoting(col_name) else col_name
                col_df = df[df["COLUMN_NAME"] == col_key]

                if col_df.empty:
                    results[col_key] = []
                    continue

                # Extract values and sanitize
                values = col_df["VALUE"].tolist()
                non_null_values = [val for val in values if val is not None and str(val).strip()]

                # Apply sanitization
                sanitized_values = []
                MAX_SAMPLE_VALUE_LENGTH = 1000
                for val in non_null_values[:limit]:
                    val_str = CharacterSanitizer.sanitize_for_yaml_value(str(val), MAX_SAMPLE_VALUE_LENGTH)
                    sanitized_values.append(val_str)

                results[col_key] = sanitized_values

            return results

        except Exception as e:
            logger.warning(f"Batch sample values query failed, falling back to sequential: {e}")
            # Fallback to sequential queries
            results = {}
            for col_name in column_names:
                results[col_name.upper()] = self.get_sample_values(
                    table_name, schema_name, col_name, database_name, limit
                )
            return results

    def validate_primary_key(
        self, table_name: str, schema_name: str, primary_key_columns: List[str], database_name: str
    ) -> bool:
        """
        Validate if columns form a unique primary key.

        Args:
            table_name: Name of the table (handles special characters with quoting)
            schema_name: Schema containing the table
            primary_key_columns: List of column names to test (handles special chars)
            database_name: Database containing the table

        Returns:
            True if columns form a unique key, False otherwise

        Example:
            >>> metadata_mgr.validate_primary_key(
            ...     'users', 'public', ['user_id'], 'analytics'
            ... )
            True
        """
        # Uppercase database/schema/table identifiers
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()

        # Check for columns needing quoting and warn
        quoted_columns = [c for c in primary_key_columns if self._requires_quoting(c)]
        if quoted_columns:
            self._log_quoted_identifier_warning(table_name, quoted_columns)

        # Use intelligent quoting for column names
        columns_sql = [self._quote_identifier(col) for col in primary_key_columns]

        column_list = ", ".join(columns_sql)
        where_clause = " AND ".join([f"{col} IS NOT NULL" for col in columns_sql])

        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT {column_list}) as distinct_combinations
        FROM {database_upper}.{schema_upper}.{table_upper}
        WHERE {where_clause}
        """

        df = self._execute_query(query)

        if df.empty:
            return False

        row = df.iloc[0]
        total_rows = row["TOTAL_ROWS"]
        distinct_combinations = row["DISTINCT_COMBINATIONS"]

        # Valid if total equals distinct and > 0
        return total_rows == distinct_combinations and total_rows > 0

    def get_row_count(self, table_name: str, schema_name: str, database_name: str) -> int:
        """
        Get total row count for a table.

        Args:
            table_name: Name of the table (case-insensitive, will be uppercased)
            schema_name: Schema containing the table (case-insensitive, will be uppercased)
            database_name: Database containing the table (case-insensitive, will be uppercased)

        Returns:
            Total number of rows
        """
        # Uppercase all identifiers for Snowflake case-insensitive matching
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()

        query = f"SELECT COUNT(*) as row_count FROM {database_upper}.{schema_upper}.{table_upper}"
        df = self._execute_query(query)

        if df.empty:
            return 0

        return int(df.iloc[0]["ROW_COUNT"])

    def _execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Args:
            query: SQL query to execute

        Returns:
            DataFrame with query results

        Raises:
            Exception: If query execution fails
        """
        with self.connection_manager.get_connection() as conn:
            return pd.read_sql(query, conn)

    def get_sample_values(
        self, table_name: str, schema_name: str, column_name: str, database_name: str, limit: int = 25
    ) -> List[Any]:
        """
        Get distinct sample values for a single column.

        For better performance when sampling multiple columns, use get_sample_values_batch().

        Args:
            table_name: Name of the table (handles special characters with quoting)
            schema_name: Schema containing the table
            column_name: Name of the column (handles special characters with quoting)
            database_name: Database containing the table
            limit: Maximum number of distinct non-null values to return

        Returns:
            List of sample values as strings (sanitized for YAML/Jinja compatibility)
        """
        # Uppercase database/schema/table identifiers
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()

        # Use intelligent quoting for column name
        if self._requires_quoting(column_name):
            self._log_quoted_identifier_warning(table_name, [column_name])
        col_sql = self._quote_identifier(column_name)

        # Fetch limit+1 to account for potential null value
        query = f"""
        SELECT DISTINCT {col_sql}
        FROM {database_upper}.{schema_upper}.{table_upper}
        LIMIT {limit + 1}
        """
        df = self._execute_query(query)

        if df.empty:
            return []

        # Convert to list and filter out nulls/None values in Python
        values = df.iloc[:, 0].tolist()
        non_null_values = [val for val in values if val is not None and str(val).strip()]

        # Sanitize values using CharacterSanitizer
        result = []
        for val in non_null_values[:limit]:
            val_str = CharacterSanitizer.sanitize_for_yaml_value(str(val), max_length=500)
            result.append(val_str)
        return result
