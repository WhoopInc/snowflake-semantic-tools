#!/usr/bin/env python3
"""
Snowflake Data Loader

High-performance bulk data loading with atomic table swaps for zero-downtime updates.

Implements a staging table pattern for reliable data loading:
1. Creates temporary staging tables with identical schema
2. Bulk loads data using optimized pandas integration
3. Validates loaded data integrity
4. Performs atomic table swap for seamless updates

This approach ensures:
- Zero downtime during updates
- Transactional consistency
- Automatic rollback on failures
- Preservation of table permissions and dependencies

The loader handles all Snowflake-specific data type conversions and
optimizations for maximum throughput.
"""

import json
import time
from datetime import date, datetime
from typing import Any, List, Optional

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("snowflake.data_loader")


def format_permission_error(error: Exception, database: str, schema: str, role: Optional[str]) -> str:
    """
    Format Snowflake permission errors with helpful context and remediation.

    Args:
        error: Original Snowflake exception
        database: Target database name
        schema: Target schema name
        role: Current Snowflake role (if known)

    Returns:
        User-friendly error message with remediation steps
    """
    error_str = str(error).lower()

    # Detect permission-related errors
    is_permission_error = any(
        [
            "not authorized" in error_str,
            "insufficient privileges" in error_str,
            "access denied" in error_str,
            "does not exist or not authorized" in error_str,
        ]
    )

    if not is_permission_error:
        # Not a permission error - return original
        return str(error)

    # Build friendly error message
    role_info = f"Current role: {role}" if role else "Current role: Unknown"
    role_name = role.upper() if role else "YOUR_ROLE"

    message = f"""
ERROR: Cannot access database '{database}'

{role_info}
Database: {database}
Schema: {schema}

This usually means:
  • Role '{role or 'your current role'}' doesn't have access to database '{database}'
  • The database exists but requires different permissions
  • You're using the wrong role for this operation

Solutions:
  1. Set SNOWFLAKE_ROLE env var to a role with access:
     export SNOWFLAKE_ROLE=ACCOUNTADMIN
     
  2. Grant your role access (requires ACCOUNTADMIN or SECURITYADMIN):
     GRANT USAGE ON DATABASE {database.upper()} TO ROLE {role_name};
     GRANT USAGE ON SCHEMA {database.upper()}.{schema.upper()} TO ROLE {role_name};
     
  3. Contact your Snowflake admin for permissions

For more help, see: https://docs.snowflake.com/en/user-guide/security-access-control

Original error: {error}
"""
    return message.strip()


def build_column_definitions(columns: List) -> str:
    """
    Build SQL column definitions from schema columns.

    Static utility function to avoid circular dependencies.

    Args:
        columns: List of Column objects from SemanticTableSchemas

    Returns:
        SQL string with column definitions
    """
    from snowflake_semantic_tools.core.models.schemas import ColumnType

    column_defs = []
    for col in columns:
        col_type_str = col.type.value

        # Add description as comment if available
        if col.description:
            # Escape single quotes in description
            escaped_desc = col.description.replace("'", "''")
            column_defs.append(f"{col.name} {col_type_str} COMMENT '{escaped_desc}'")
        else:
            column_defs.append(f"{col.name} {col_type_str}")

    return ", ".join(column_defs)


class DataLoader:
    """
    Orchestrates high-performance data loading with atomic updates.

    Manages the complete lifecycle of data loading operations:
    - **Preparation**: Converts pandas DataFrames to Snowflake-compatible formats
    - **Staging**: Creates temporary tables for safe data loading
    - **Loading**: Uses bulk operations for optimal performance
    - **Validation**: Ensures data integrity before committing
    - **Swap**: Atomic table replacement for zero-downtime updates

    The loader automatically handles complex data types (arrays, JSON),
    timezone conversions, and special character escaping to ensure
    reliable data transfer to Snowflake.
    """

    def __init__(self, connection_manager: ConnectionManager, config: SnowflakeConfig):
        """
        Initialize the data loader with required dependencies.

        Args:
            connection_manager: Connection manager instance
            config: Configuration instance
        """
        self.connection_manager = connection_manager
        self.config = config

    @property
    def table_names(self) -> dict:
        """Get default table names mapping."""
        # Default table names for semantic models
        return {
            "sm_metrics": "sm_metrics",
            "sm_dimensions": "sm_dimensions",
            "sm_facts": "sm_facts",
            "sm_time_dimensions": "sm_time_dimensions",
            "sm_relationships": "sm_relationships",
            "sm_relationship_columns": "sm_relationship_columns",
            "sm_custom_instructions": "sm_custom_instructions",
            "sm_filters": "sm_filters",
            "sm_verified_queries": "sm_verified_queries",
            "sm_semantic_views": "sm_semantic_views",
            "sm_table_summaries": "sm_table_summaries",
        }

    @property
    def staging_suffix(self) -> str:
        """Get default staging suffix."""
        return "_staging"

    def create_staging_tables(self) -> bool:
        """
        Create staging tables with the same schema as production tables.

        Returns:
            True if successful, False otherwise
        """
        failed_staging_tables = []
        created_staging_tables = []

        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Ensure we're in the right context and commit each table creation
                cursor.execute(f"USE DATABASE {self.config.database.upper()}")
                cursor.execute(f"USE SCHEMA {self.config.schema.upper()}")

                from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

                schemas = SemanticTableSchemas.get_all_schemas()

                for table_key, schema_def in schemas.items():
                    production_table = self.table_names.get(table_key, table_key)
                    staging_table = f"{production_table}{self.staging_suffix}"

                    try:
                        # Drop staging table if exists
                        drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
                        cursor.execute(drop_sql)
                        logger.debug(f"Dropped staging table: {staging_table}")

                        # Create staging table with same structure as production
                        columns_sql = build_column_definitions(schema_def.columns)
                        create_sql = f"CREATE TABLE {staging_table} ({columns_sql})"

                        cursor.execute(create_sql)

                        # Verify table exists (no need to commit yet - batch at end)
                        verify_sql = f"SELECT COUNT(*) FROM {staging_table} WHERE 1=0"
                        cursor.execute(verify_sql)

                        logger.info(f"Created staging table: {staging_table}")
                        created_staging_tables.append(staging_table)

                    except Exception as e:
                        failed_staging_tables.append(staging_table)
                        logger.error(f"Failed to create staging table {staging_table}: {e}")
                        # Continue with other tables

                # Single commit for all staging table creations (better performance)
                conn.commit()

                # Log summary
                logger.info(
                    f"Staging table creation summary: {len(created_staging_tables)} created, {len(failed_staging_tables)} failed"
                )
                if failed_staging_tables:
                    logger.warning(f"Failed staging tables: {', '.join(failed_staging_tables)}")

                # Return True if we created at least some staging tables
                return len(created_staging_tables) > 0

        except Exception as e:
            # Check if it's a permission error and format nicely (Issue #30)
            friendly_error = format_permission_error(e, self.config.database, self.config.schema, self.config.role)
            logger.error(friendly_error)
            # Preserve original exception type for programmatic handling
            raise type(e)(friendly_error) from e

    def load_dataframe_to_staging(
        self, df: pd.DataFrame, table_key: str, conn=None  # Allow passing existing connection
    ) -> bool:
        """
        Load a DataFrame to the corresponding staging table in a single batch.

        Args:
            df: DataFrame to load
            table_key: Key from SemanticTableSchemas to identify target table
            conn: Optional existing connection to reuse

        Returns:
            True if successful, False otherwise
        """
        from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

        schemas = SemanticTableSchemas.get_all_schemas()
        if table_key not in schemas:
            logger.error(f"Unknown table key: {table_key}")
            return False

        if df.empty:
            logger.debug(f"Empty DataFrame for table: {table_key}")
            return True

        production_table = self.table_names.get(table_key, table_key)
        staging_table = f"{production_table}{self.staging_suffix}"

        # Start timing for performance monitoring
        start_time = time.time()

        try:
            # Prepare DataFrame for Snowflake (integrated preparation)
            df_prepared = self.prepare_dataframe_for_snowflake(df, table_key)

            # Always load entire DataFrame in single batch for maximum performance
            chunk_size = len(df_prepared)

            # Use provided connection or create new one
            if conn:
                use_conn = conn
            else:
                use_conn = self.connection_manager.get_connection().__enter__()

            # Optimized single-batch upload
            success, nchunks, nrows, _ = write_pandas(
                conn=use_conn,
                df=df_prepared,
                table_name=staging_table,
                database=self.config.database,
                schema=self.config.schema,
                chunk_size=chunk_size,  # Entire DataFrame in one shot
                compression="gzip",  # Use gzip compression (Snowflake requirement)
                on_error="continue",
                parallel=3,  # Use 3 threads for better performance (balanced)
                quote_identifiers=False,
                auto_create_table=False,
            )

            elapsed_time = time.time() - start_time

            if success:
                logger.info(f"Loaded {nrows} rows to {staging_table} in {elapsed_time:.1f}s")
                return True
            else:
                logger.error(f"Failed to load data to {staging_table}")
                return False

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Error loading DataFrame to {staging_table} after {elapsed_time:.1f}s: {e}")
            return False
        finally:
            # Clean up connection if we created it
            if not conn:
                try:
                    use_conn.__exit__(None, None, None)
                except:
                    pass

    def cleanup_staging_tables(self) -> bool:
        """Clean up any remaining staging tables."""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

                schemas = SemanticTableSchemas.get_all_schemas()

                for table_key in schemas.keys():
                    production_table = self.table_names.get(table_key, table_key)
                    staging_table = f"{production_table}{self.staging_suffix}"

                    cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                    logger.debug(f"Cleaned up staging table: {staging_table}")

                return True

        except Exception as e:
            logger.error(f"Failed to clean up staging tables: {e}")
            return False

    # ==========================================
    # Data Preparation Methods (merged from data_preparator.py)
    # ==========================================

    def prepare_dataframe_for_snowflake(self, df: pd.DataFrame, table_key: str) -> pd.DataFrame:
        """
        Prepare DataFrame for Snowflake by handling special data types.
        Optimized for performance.

        Args:
            df: Input DataFrame to be prepared for Snowflake ingestion
            table_key: Table schema key from SemanticTableSchemas to determine column types

        Returns:
            Prepared DataFrame with properly formatted columns for Snowflake:
            - Arrays converted to JSON strings
            - Boolean values normalized
            - Date columns properly formatted
            - Missing values handled appropriately

        Note:
            This method performs in-place transformations when possible to optimize memory usage.
        """
        # Work with original DataFrame to avoid expensive copy for simple cases
        from snowflake_semantic_tools.core.models.schemas import ColumnType, SemanticTableSchemas

        schemas = SemanticTableSchemas.get_all_schemas()
        schema_def = schemas[table_key]

        # Check if we need any transformations
        needs_transformation = False
        array_columns = []
        boolean_columns = []
        date_columns = []

        for col in schema_def.columns:
            col_name = col.name
            col_type = col.type

            if col_name in df.columns:
                if col_type == ColumnType.ARRAY:
                    array_columns.append(col_name)
                    needs_transformation = True
                elif col_type == ColumnType.BOOLEAN:
                    # Only transform if we have non-boolean data
                    if not pd.api.types.is_bool_dtype(df[col_name]):
                        boolean_columns.append(col_name)
                        needs_transformation = True
                else:
                    # Check for datetime columns that need conversion
                    if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                        date_columns.append(col_name)
                        needs_transformation = True

        # Only copy and transform if necessary
        if not needs_transformation:
            return df.reset_index(drop=True)

        df_copy = df.copy()
        df_copy = df_copy.reset_index(drop=True)

        # Optimize array column processing with proper date handling
        if array_columns:
            for col_name in array_columns:
                df_copy[col_name] = df_copy[col_name].apply(self._safe_json_serialize)

        # Optimize boolean column processing
        if boolean_columns:
            for col_name in boolean_columns:
                # Simple, fast boolean conversion
                df_copy[col_name] = df_copy[col_name].fillna(False).astype(bool)

        # Handle verified_at column for verified queries (convert YYYY-MM-DD to Unix timestamp)
        if table_key == "sm_verified_queries" and "verified_at" in df_copy.columns:
            df_copy["verified_at"] = df_copy["verified_at"].apply(self._convert_date_to_unix_timestamp)

        # Optimize date column processing
        if date_columns:
            for col_name in date_columns:
                # Fast datetime to string conversion
                if hasattr(df_copy[col_name].dtype, "tz"):
                    # Handle timezone-aware datetimes
                    df_copy[col_name] = df_copy[col_name].dt.strftime("%Y-%m-%d %H:%M:%S %Z")
                else:
                    df_copy[col_name] = df_copy[col_name].dt.strftime("%Y-%m-%d %H:%M:%S")

        return df_copy

    def _convert_date_to_unix_timestamp(self, date_str: Any) -> Optional[int]:
        """
        Convert YYYY-MM-DD date string to Unix timestamp (seconds since epoch).

        Snowflake's Cortex Analyst requires verified_at as Unix timestamp for proper
        date handling. This method converts human-readable YYYY-MM-DD format to
        Unix timestamp while preserving None/empty values.

        Args:
            date_str: Date string in YYYY-MM-DD format, or None/empty

        Returns:
            Unix timestamp (seconds since epoch) or None if input is None/empty

        Example:
            "2024-01-15" → 1705276800
            None → None
            "" → None
        """
        if not date_str or pd.isna(date_str):
            return None

        try:
            # Parse YYYY-MM-DD format
            from datetime import datetime

            dt = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
            # Convert to Unix timestamp (seconds since epoch)
            return int(dt.timestamp())
        except (ValueError, AttributeError) as e:
            logger.warning(f"Could not convert '{date_str}' to Unix timestamp: {e}")
            return None

    def _safe_json_serialize(self, obj: Any) -> str:
        """
        Safely serialize objects to JSON, handling dates properly.

        Args:
            obj: Object to serialize

        Returns:
            JSON string representation of the object
        """
        if obj is None:
            return None
        elif isinstance(obj, (list, tuple)):
            # Handle arrays with proper date conversion
            converted_list = []
            for item in obj:
                if isinstance(item, (date, datetime)):
                    converted_list.append(item.isoformat())
                elif hasattr(item, "isoformat"):  # pandas Timestamp
                    converted_list.append(item.isoformat())
                else:
                    converted_list.append(str(item) if item is not None else None)
            return json.dumps(converted_list)
        else:
            # Single value - convert to array
            if isinstance(obj, (date, datetime)):
                return json.dumps([obj.isoformat()])
            elif hasattr(obj, "isoformat"):  # pandas Timestamp
                return json.dumps([obj.isoformat()])
            else:
                return json.dumps([str(obj)])

    def validate_dataframe_compatibility(self, df: pd.DataFrame, table_key: str) -> bool:
        """
        Validate that a DataFrame is compatible with the target table schema.

        Args:
            df: DataFrame to validate
            table_key: Table schema key from SemanticTableSchemas

        Returns:
            True if compatible, False otherwise
        """
        try:
            from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

            schemas = SemanticTableSchemas.get_all_schemas()
            if table_key not in schemas:
                logger.error(f"Unknown table key: {table_key}")
                return False

            schema_def = schemas[table_key]
            expected_columns = {col.name for col in schema_def.columns}
            actual_columns = set(df.columns)

            # Check for missing required columns
            missing_columns = expected_columns - actual_columns
            if missing_columns:
                logger.warning(f"DataFrame missing columns for {table_key}: {missing_columns}")
                # This might be acceptable depending on requirements

            # Check for unexpected columns
            extra_columns = actual_columns - expected_columns
            if extra_columns:
                logger.warning(f"DataFrame has unexpected columns for {table_key}: {extra_columns}")
                # This might be acceptable depending on requirements

            return True

        except Exception as e:
            logger.error(f"Error validating DataFrame compatibility: {e}")
            return False

    def load_semantic_models(self, models_data: dict, database: str, schema: str) -> bool:
        """
        Load semantic model metadata to Snowflake tables.

        This is the core method for extracting dbt metadata into Snowflake.
        Handles data transformation, table creation, and bulk loading.

        Args:
            models_data: Dictionary with table names as keys and data as values
            database: Target database name
            schema: Target schema name

        Returns:
            True if successful, False otherwise

        Raises:
            ValueError: If warehouse not configured or database/schema operations fail
        """
        try:
            import pandas as pd
            from snowflake.connector.pandas_tools import write_pandas

            # Get a connection
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Create database and schema if needed
                # Skip database creation for Snowflake system databases
                if database.upper() not in ["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]:
                    try:
                        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database.upper()}")
                    except Exception as e:
                        logger.debug(f"Could not create database {database.upper()}: {e}")
                        # Continue anyway, database might already exist or user lacks permissions

                cursor.execute(f"USE DATABASE {database.upper()}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema.upper()}")
                cursor.execute(f"USE SCHEMA {schema.upper()}")

                # Check if warehouse is set, if not set it
                cursor.execute("SELECT CURRENT_WAREHOUSE()")
                result = cursor.fetchone()
                current_warehouse = result[0] if result else None

                if not current_warehouse:
                    # Try to get warehouse from connection manager config
                    warehouse = None

                    if hasattr(self.connection_manager, "config"):
                        warehouse = getattr(self.connection_manager.config, "warehouse", None)

                    if not warehouse:
                        raise ValueError(
                            "Snowflake warehouse not configured. Set via:\n"
                            "  1. Environment: export SNOWFLAKE_WAREHOUSE='YOUR_WH'\n"
                            "  2. Config file: Add to sst_config.yaml under snowflake.warehouse\n"
                            "  3. Connection string: Include warehouse in connection params\n"
                            "\n"
                            "Example warehouse names: ANALYTICS_WH, COMPUTE_WH, TRANSFORM_WH"
                        )

                    logger.info(f"No warehouse set in session, setting to configured warehouse: {warehouse}")
                    try:
                        cursor.execute(f"USE WAREHOUSE {warehouse}")
                        current_warehouse = warehouse
                    except Exception as e:
                        raise ValueError(
                            f"Could not set warehouse '{warehouse}': {e}\n"
                            f"Ensure:\n"
                            f"  1. Warehouse '{warehouse}' exists in your Snowflake account\n"
                            f"  2. Your role has USAGE privilege on the warehouse\n"
                            f"  3. Warehouse name is spelled correctly (case-insensitive)"
                        ) from e

                logger.info(f"Using database {database}, schema {schema}, and warehouse {current_warehouse}")

                # Load each model type
                for table_key, data in models_data.items():
                    if data is None or (isinstance(data, list) and len(data) == 0):
                        logger.info(f"Skipping {table_key} - no data to load")
                        continue

                    # Convert to DataFrame if needed
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, pd.DataFrame):
                        df = data
                    else:
                        logger.warning(f"Unexpected data type for {table_key}: {type(data)}")
                        continue

                    # Handle columns with mixed types (like sample_values, synonyms) by converting to JSON strings
                    import json
                    from datetime import date, datetime

                    def safe_json_dumps(obj):
                        """Convert object to JSON string, handling special types."""
                        if obj is None:
                            return None
                        try:
                            # Custom JSON encoder for dates
                            def json_encoder(o):
                                if isinstance(o, (datetime, date)):
                                    return o.isoformat()
                                return str(o)

                            return json.dumps(obj, default=json_encoder)
                        except:
                            # If all else fails, convert to string
                            return str(obj)

                    for col in df.columns:
                        if col in ["sample_values", "synonyms", "primary_key", "unique_keys", "foreign_keys"]:
                            # Convert lists/arrays to JSON strings
                            df[col] = df[col].apply(safe_json_dumps)

                    # Add sm_ prefix if not already present
                    if not table_key.startswith("sm_"):
                        snowflake_table_name = f"sm_{table_key}"
                    else:
                        snowflake_table_name = table_key

                    # Convert to uppercase for Snowflake
                    snowflake_table_name = snowflake_table_name.upper()

                    # Uppercase column names for case-insensitive access in Snowflake
                    df.columns = [col.upper() for col in df.columns]

                    logger.info(f"Loading {len(df)} rows to {snowflake_table_name}")

                    # Use write_pandas directly - this is what worked in our test
                    success, num_chunks, num_rows, output = write_pandas(
                        conn,
                        df,
                        snowflake_table_name,
                        auto_create_table=True,
                        overwrite=True,
                        database=database.upper() if database else None,
                        schema=schema.upper() if schema else None,
                    )

                    if success:
                        logger.info(f"Successfully loaded {num_rows} rows to {snowflake_table_name}")
                    else:
                        logger.error(f"Failed to load data to {snowflake_table_name}")
                        return False

                logger.info(f"Successfully loaded all semantic models to {database}.{schema}")
                return True

        except Exception as e:
            error_msg = str(e)

            # Check if this is a common/expected error (auth, connection)
            is_auth_error = "differs from the user currently logged in" in error_msg
            is_connection_error = "250001" in error_msg or "Failed to connect" in error_msg

            if is_auth_error:
                # Clean message for SSO mismatch (don't show stack trace)
                logger.error("Snowflake SSO authentication mismatch")
                import sys

                import click

                click.echo("\nERROR: Snowflake SSO authentication mismatch", err=True)
                click.echo("The user authenticated in browser doesn't match SNOWFLAKE_USER in .env", err=True)
                click.echo("\nTo fix:", err=True)
                click.echo("  1. Log out of Okta in your browser", err=True)
                click.echo("  2. Run the command again", err=True)
                click.echo("  3. Authenticate as the user specified in .env", err=True)
            elif is_connection_error:
                # Clean message for connection errors (don't show stack trace)
                logger.error(f"Failed to connect to Snowflake: {error_msg}")
                import sys

                import click

                click.echo(f"\nERROR: Failed to connect to Snowflake", err=True)
                click.echo(f"{error_msg}", err=True)
            else:
                # Unexpected error - show stack trace for debugging
                logger.error(f"Failed to load semantic models: {e}")
                import traceback

                logger.error(traceback.format_exc())
                import sys

                import click

                click.echo(f"\nERROR: Failed to load semantic models: {e}", err=True)
                traceback.print_exc(file=sys.stderr)

            return False
