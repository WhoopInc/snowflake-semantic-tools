#!/usr/bin/env python3
"""
Metadata Manager

Handles metadata enrichment operations including schema inspection,
sample value collection, and primary key validation.

This manager provides the intelligence layer for automatic metadata enrichment,
querying Snowflake to gather structural information and statistics about tables.
"""

from typing import Any, Dict, List

import pandas as pd

from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer

logger = get_logger("infrastructure.snowflake.metadata_manager")


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

        # Uppercase all identifiers
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()

        # Build UNION ALL query for all columns
        union_queries = []
        for col_name in column_names:
            col_upper = col_name.upper()
            # Each subquery selects column name and values
            union_queries.append(
                f"""
                SELECT '{col_upper}' as COLUMN_NAME, {col_upper} as VALUE
                FROM {database_upper}.{schema_upper}.{table_upper}
                WHERE {col_upper} IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {col_upper} ORDER BY NULL) = 1
                LIMIT {limit + 1}
            """
            )

        # Combine all queries with UNION ALL
        full_query = "\nUNION ALL\n".join(union_queries)

        try:
            df = self._execute_query(full_query)

            if df.empty:
                return {col.upper(): [] for col in column_names}

            # Group results by column name
            results = {}
            for col_name in column_names:
                col_upper = col_name.upper()
                col_df = df[df["COLUMN_NAME"] == col_upper]

                if col_df.empty:
                    results[col_upper] = []
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

                results[col_upper] = sanitized_values

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

    def get_sample_values(
        self, table_name: str, schema_name: str, column_name: str, database_name: str, limit: int = 25
    ) -> List[Any]:
        """
        Get distinct sample values for a column with better diversity.

        Performance optimizations:
        - No ORDER BY for faster query execution and more diverse samples
        - No WHERE IS NOT NULL to avoid full table scan
        - Filters nulls in Python after retrieval
        - Fetches limit+1 to account for potential null value
        - Truncates individual values to 1000 characters to prevent Snowflake load errors
        - Sanitizes Jinja template characters to prevent dbt compilation errors
        - Sanitizes YAML-breaking characters to prevent YAML parsing errors

        Args:
            table_name: Name of the table (case-insensitive, will be uppercased)
            schema_name: Schema containing the table (case-insensitive, will be uppercased)
            column_name: Name of the column (case-insensitive, will be uppercased)
            database_name: Database containing the table (case-insensitive, will be uppercased)
            limit: Maximum number of distinct non-null values to return

        Returns:
            List of sample values as strings (nulls filtered out, max 1000 chars each,
            Jinja and YAML-breaking characters sanitized)

        Example:
            >>> metadata_mgr.get_sample_values('users', 'public', 'status', 'analytics')
            ['active', 'inactive', 'pending']
        """
        # Uppercase all identifiers for Snowflake case-insensitive matching
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()
        column_upper = column_name.upper()

        # Fetch limit+1 to account for potential null value
        query = f"""
        SELECT DISTINCT {column_upper}
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

    def validate_primary_key(
        self, table_name: str, schema_name: str, primary_key_columns: List[str], database_name: str
    ) -> bool:
        """
        Validate if columns form a unique primary key.

        Args:
            table_name: Name of the table (case-insensitive, will be uppercased)
            schema_name: Schema containing the table (case-insensitive, will be uppercased)
            primary_key_columns: List of column names to test (case-insensitive, will be uppercased)
            database_name: Database containing the table (case-insensitive, will be uppercased)

        Returns:
            True if columns form a unique key, False otherwise

        Example:
            >>> metadata_mgr.validate_primary_key(
            ...     'users', 'public', ['user_id'], 'analytics'
            ... )
            True
        """
        # Uppercase all identifiers for Snowflake case-insensitive matching
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()
        columns_upper = [col.upper() for col in primary_key_columns]

        column_list = ", ".join(columns_upper)
        where_clause = " AND ".join([f"{col} IS NOT NULL" for col in columns_upper])

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
            table_name: Name of the table (case-insensitive, will be uppercased)
            schema_name: Schema containing the table (case-insensitive, will be uppercased)
            column_name: Name of the column (case-insensitive, will be uppercased)
            database_name: Database containing the table (case-insensitive, will be uppercased)
            limit: Maximum number of distinct non-null values to return

        Returns:
            List of sample values as strings (sanitized for YAML/Jinja compatibility)
        """
        # Uppercase all identifiers for Snowflake case-insensitive matching
        database_upper = database_name.upper()
        schema_upper = schema_name.upper()
        table_upper = table_name.upper()
        column_upper = column_name.upper()

        # Fetch limit+1 to account for potential null value
        query = f"""
        SELECT DISTINCT {column_upper}
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
