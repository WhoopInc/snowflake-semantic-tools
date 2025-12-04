#!/usr/bin/env python3
"""
Snowflake Table Manager

Handles atomic table swaps, row counting, and other table management operations.
Provides safe, transactional operations for managing production tables.
"""

from typing import Dict, List, Optional

import pandas as pd
from snowflake.connector import DictCursor

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("snowflake.table_manager")


class TableManager:
    """Manages Snowflake table operations including atomic swaps and maintenance."""

    def __init__(self, connection_manager: ConnectionManager, config: SnowflakeConfig):
        """
        Initialize the table manager with required dependencies.

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
            "sm_custom_instructions": "sm_custom_instructions",
            "sm_filters": "sm_filters",
            "sm_verified_queries": "sm_verified_queries",
            "sm_semantic_views": "sm_semantic_views",
        }

    @property
    def staging_suffix(self) -> str:
        """Get default staging suffix."""
        return "_staging"

    def swap_staging_to_production(self, table_keys: Optional[List[str]] = None) -> bool:
        """
        Atomically swap staging tables with production tables.

        Args:
            table_keys: List of table keys to swap. If None, swap all tables.

        Returns:
            True if successful, False otherwise
        """
        # Use all table keys if none specified
        if table_keys is None:
            from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

            schemas = SemanticTableSchemas.get_all_schemas()
            table_keys = list(schemas.keys())

        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Start transaction
                cursor.execute("BEGIN")

                try:
                    for table_key in table_keys:
                        production_table = self.table_names.get(table_key, table_key)
                        staging_table = f"{production_table}{self.staging_suffix}"
                        temp_table = f"{production_table}_temp_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"

                        # Step 1: Rename production table to temp
                        cursor.execute(f"ALTER TABLE {production_table} RENAME TO {temp_table}")
                        logger.debug(f"Renamed {production_table} to {temp_table}")

                        # Step 2: Rename staging table to production
                        cursor.execute(f"ALTER TABLE {staging_table} RENAME TO {production_table}")
                        logger.debug(f"Swapped {staging_table} to {production_table}")

                        # Step 3: Drop old production table (now temp)
                        cursor.execute(f"DROP TABLE {temp_table}")
                        logger.debug(f"Dropped old table: {temp_table}")

                    # Commit transaction
                    cursor.execute("COMMIT")
                    logger.info(f"Successfully swapped {len(table_keys)} tables to production")
                    return True

                except Exception as e:
                    # Rollback on error
                    cursor.execute("ROLLBACK")
                    logger.error(f"Table swap failed, rolled back: {e}")
                    return False

        except Exception as e:
            logger.error(f"Failed to swap staging tables: {e}")
            return False

    def get_table_row_counts(self) -> Dict[str, int]:
        """Get row counts for all production tables."""
        row_counts = {}

        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor(DictCursor)

                from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

                schemas = SemanticTableSchemas.get_all_schemas()

                for table_key in schemas.keys():
                    table_name = self.table_names.get(table_key, table_key)

                    try:
                        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                        result = cursor.fetchone()
                        row_counts[table_name] = result["COUNT"] if result else 0
                    except Exception as e:
                        logger.warning(f"Could not get row count for {table_name}: {e}")
                        row_counts[table_name] = -1

        except Exception as e:
            logger.error(f"Failed to get table row counts: {e}")

        return row_counts

    def verify_table_integrity(self, table_key: str) -> Dict[str, bool]:
        """
        Verify the integrity of a table by checking its structure and basic properties.

        Args:
            table_key: Key of the table to verify

        Returns:
            Dictionary with verification results
        """
        verification_results = {"exists": False, "has_data": False, "schema_matches": False}

        try:
            table_name = self.table_names.get(table_key, table_key)

            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor(DictCursor)

                # Check if table exists
                try:
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                    result = cursor.fetchone()
                    verification_results["exists"] = True
                    verification_results["has_data"] = result["COUNT"] > 0
                except Exception:
                    logger.warning(f"Table {table_name} does not exist or is not accessible")
                    return verification_results

                # Check schema compatibility (basic check)
                try:
                    cursor.execute(f"DESCRIBE TABLE {table_name}")
                    columns_info = cursor.fetchall()

                    # Get expected schema
                    from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

                    schemas = SemanticTableSchemas.get_all_schemas()
                    if table_key in schemas:
                        expected_columns = {col.name.upper() for col in schemas[table_key].columns}
                        actual_columns = {col["name"].upper() for col in columns_info}

                        # Check if all expected columns exist
                        verification_results["schema_matches"] = expected_columns.issubset(actual_columns)
                    else:
                        verification_results["schema_matches"] = True  # Unknown schema, assume it's ok

                except Exception as e:
                    logger.warning(f"Could not verify schema for {table_name}: {e}")
                    verification_results["schema_matches"] = False

        except Exception as e:
            logger.error(f"Error verifying table integrity for {table_key}: {e}")

        return verification_results

    def get_table_metadata(self, table_key: str) -> Dict[str, any]:
        """
        Get detailed metadata about a table.

        Args:
            table_key: Key of the table to get metadata for

        Returns:
            Dictionary with table metadata
        """
        metadata = {"table_name": "", "row_count": -1, "columns": [], "size_bytes": -1, "last_modified": None}

        try:
            table_name = self.table_names.get(table_key, table_key)
            metadata["table_name"] = table_name

            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor(DictCursor)

                # Get row count
                try:
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                    result = cursor.fetchone()
                    metadata["row_count"] = result["COUNT"] if result else 0
                except Exception as e:
                    logger.warning(f"Could not get row count for {table_name}: {e}")

                # Get column information
                try:
                    cursor.execute(f"DESCRIBE TABLE {table_name}")
                    columns_info = cursor.fetchall()
                    metadata["columns"] = [
                        {"name": col["name"], "type": col["type"], "nullable": col["null?"] == "Y"}
                        for col in columns_info
                    ]
                except Exception as e:
                    logger.warning(f"Could not get column info for {table_name}: {e}")

                # Get table size and last modified (if available)
                try:
                    cursor.execute(
                        f"""
                        SELECT 
                            BYTES,
                            LAST_ALTERED
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_SCHEMA = '{self.config.schema.upper()}' 
                        AND TABLE_NAME = '{table_name.upper()}'
                    """
                    )
                    result = cursor.fetchone()
                    if result:
                        metadata["size_bytes"] = result["BYTES"] or -1
                        metadata["last_modified"] = result["LAST_ALTERED"]
                except Exception as e:
                    logger.warning(f"Could not get table metadata for {table_name}: {e}")

        except Exception as e:
            logger.error(f"Error getting table metadata for {table_key}: {e}")

        return metadata

    def truncate_table(self, table_key: str, confirm: bool = False) -> bool:
        """
        Truncate a table (remove all data but keep structure).

        Args:
            table_key: Key of the table to truncate
            confirm: Must be True to actually perform the truncation

        Returns:
            True if successful, False otherwise
        """
        if not confirm:
            logger.warning(f"Truncate operation for {table_key} requires explicit confirmation")
            return False

        try:
            table_name = self.table_names.get(table_key, table_key)

            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                logger.info(f"Truncated table: {table_name}")
                return True

        except Exception as e:
            logger.error(f"Failed to truncate table {table_key}: {e}")
            return False
