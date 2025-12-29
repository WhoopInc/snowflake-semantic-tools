#!/usr/bin/env python3
"""
Snowflake Cortex Search Manager

Manages Cortex Search Services to enable AI-powered semantic search over metadata.

Creates and maintains Cortex Search Services that power natural language understanding
in Cortex Analyst by providing semantic search capabilities over table summaries.
This enables the AI to better understand user intent and map queries to the correct
tables based on comprehensive descriptions rather than just table names.

The service indexes table summaries with business context, making it possible for
Cortex Analyst to understand domain-specific terminology and find relevant tables
even when users use different terms than the actual table names.
"""

import time
from typing import Any, Dict, Optional

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("snowflake.cortex_search_manager")


class CortexSearchManager:
    """Manages Cortex Search services for semantic search over table summaries."""

    def __init__(self, connection_manager: ConnectionManager, config: SnowflakeConfig):
        """
        Initialize the Cortex Search manager with required dependencies.

        Args:
            connection_manager: Connection manager instance
            config: Configuration instance
        """
        self.connection_manager = connection_manager
        self.config = config

    @property
    def search_service_name(self) -> str:
        """Get the fixed search service name."""
        return "SEMANTIC_MODEL_SEARCH_SERVICE"

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

    def search_service_exists(self) -> bool:
        """
        Check if the Cortex Search service already exists.

        Returns:
            True if the service exists, False otherwise
        """
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Check if the search service exists
                cursor.execute(f"SHOW CORTEX SEARCH SERVICES LIKE '{self.search_service_name}'")
                result = cursor.fetchone()

                return result is not None

        except Exception as e:
            logger.warning(f"Failed to check if search service exists: {e}")
            return False

    def diagnose_refresh_failure(self) -> Dict[str, Any]:
        """
        Diagnose why a Cortex Search refresh might be failing.

        Returns:
            Dictionary with diagnostic information
        """
        diagnostics = {
            "source_table_exists": False,
            "source_table_accessible": False,
            "source_table_row_count": 0,
            "dynamic_tables_status": [],
            "recommendations": [],
        }

        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Check if source table exists and is accessible
                table_summary_table = self.table_names.get("table_summary_table", "SM_TABLE_SUMMARIES")

                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_summary_table}")
                    row_count = cursor.fetchone()[0]
                    diagnostics["source_table_exists"] = True
                    diagnostics["source_table_accessible"] = True
                    diagnostics["source_table_row_count"] = row_count
                    logger.debug(f"Source table {table_summary_table} exists with {row_count} rows")
                except Exception as e:
                    logger.warning(f"Source table {table_summary_table} issue: {e}")
                    diagnostics["recommendations"].append(f"Source table {table_summary_table} is not accessible")

                # Check dynamic tables related to Cortex Search
                try:
                    cursor.execute("SHOW DYNAMIC TABLES LIKE '%CORTEX_SEARCH%'")
                    dynamic_tables = cursor.fetchall()

                    for dt in dynamic_tables:
                        dt_info = {"name": dt[1], "status": "unknown"}  # Table name is usually in second column

                        # Try to get more details about each dynamic table
                        try:
                            cursor.execute(f"SELECT * FROM information_schema.dynamic_tables WHERE name = '{dt[1]}'")
                            dt_details = cursor.fetchone()
                            if dt_details:
                                dt_info["status"] = "found_in_info_schema"
                        except:
                            pass

                        diagnostics["dynamic_tables_status"].append(dt_info)

                except Exception as e:
                    logger.warning(f"Failed to check dynamic tables: {e}")
                    diagnostics["recommendations"].append("Could not check dynamic table status")

                # Add general recommendations
                if diagnostics["source_table_row_count"] == 0:
                    diagnostics["recommendations"].append("Source table is empty - may cause refresh issues")

        except Exception as e:
            logger.error(f"Failed to run diagnostics: {e}")
            diagnostics["recommendations"].append(f"Diagnostic check failed: {e}")

        return diagnostics

    def create_search_service(self) -> bool:
        """
        Create a new Cortex Search service for table summaries.

        Returns:
            True if successful, False otherwise
        """
        create_sql = None  # Initialize to avoid reference error in exception handler
        try:
            start_time = time.time()

            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Get the table summary table name (now uppercase)
                table_summary_table = self.table_names.get("table_summary_table", "SM_TABLE_SUMMARIES")

                # Build the CREATE CORTEX SEARCH SERVICE query
                # The ON clause specifies the searchable text column (TABLE_SUMMARY)
                # ATTRIBUTES specifies additional metadata columns (TABLE_NAME, DATABASE_NAME, SCHEMA_NAME)
                # The CORTEX_SEARCHABLE column ensures we only index approved tables
                # TARGET_LAG is set to 100 days since data only changes when extract runs
                create_sql = f"""
                CREATE OR REPLACE CORTEX SEARCH SERVICE {self.search_service_name}
                  ON TABLE_SUMMARY
                  ATTRIBUTES TABLE_NAME, DATABASE_NAME, SCHEMA_NAME
                  WAREHOUSE = {self.config.warehouse}
                  TARGET_LAG = '100 days'
                  AS (
                    SELECT 
                        TABLE_NAME,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        TABLE_SUMMARY
                    FROM {table_summary_table}
                    WHERE CORTEX_SEARCHABLE = TRUE
                  )
                """

                logger.info(f"Creating Cortex Search service: {self.search_service_name}")
                logger.debug(f"Using warehouse: {self.config.warehouse}")
                logger.debug(f"Source table: {table_summary_table}")

                cursor.execute(create_sql)

                elapsed_time = time.time() - start_time
                logger.info(f"Successfully created Cortex Search service in {elapsed_time:.1f}s")

                return True

        except Exception as e:
            elapsed_time = time.time() - start_time
            error_msg = str(e)
            logger.error(f"Failed to create Cortex Search service after {elapsed_time:.1f}s: {error_msg}")

            # Issue #43: Provide helpful error context
            error_lower = error_msg.lower()
            if "insufficient privileges" in error_lower or "not authorized" in error_lower:
                logger.error("This appears to be a permission issue. Your role may need:")
                logger.error("  • CREATE CORTEX SEARCH SERVICE privilege")
                logger.error("  • USAGE on the target warehouse")
                logger.error("  • SELECT on the source table")
            elif "does not exist" in error_lower:
                logger.error("The source table may not exist. Run 'sst extract' first to create it.")
            elif "warehouse" in error_lower:
                logger.error("Check that your configured warehouse exists and is accessible.")

            # Also log the SQL for debugging if it was created
            if create_sql:
                logger.debug(f"Failed SQL: {create_sql}")
            import traceback

            logger.debug(f"Traceback: {traceback.format_exc()}")

            # Store the error in a way the caller can access it
            self.last_error = error_msg
            return False

    def refresh_search_service(self, max_retries: int = 3, retry_delay: int = 5) -> bool:
        """
        Refresh an existing Cortex Search service with retry logic.

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            True if successful, False otherwise
        """
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()

                with self.connection_manager.get_connection() as conn:
                    cursor = conn.cursor()

                    # Refresh the existing search service
                    refresh_sql = f"ALTER CORTEX SEARCH SERVICE {self.search_service_name} REFRESH"

                    if attempt > 0:
                        logger.info(
                            f"Refreshing Cortex Search service (attempt {attempt + 1}/{max_retries + 1}): {self.search_service_name}"
                        )
                    else:
                        logger.info(f"Refreshing Cortex Search service: {self.search_service_name}")

                    cursor.execute(refresh_sql)

                    elapsed_time = time.time() - start_time
                    logger.info(f"Successfully refreshed Cortex Search service in {elapsed_time:.1f}s")

                    return True

            except Exception as e:
                elapsed_time = time.time() - start_time

                if attempt < max_retries:
                    logger.warning(f"Cortex Search refresh attempt {attempt + 1} failed after {elapsed_time:.1f}s: {e}")
                    logger.info(f"Retrying in {retry_delay} seconds...")

                    # Run diagnostics on first failure
                    if attempt == 0:
                        diagnostics = self.diagnose_refresh_failure()
                        if diagnostics["recommendations"]:
                            logger.info(f"Diagnostic recommendations: {'; '.join(diagnostics['recommendations'])}")

                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Failed to refresh Cortex Search service after {attempt + 1} attempts and {elapsed_time:.1f}s: {e}"
                    )

                    # Run full diagnostics on final failure
                    diagnostics = self.diagnose_refresh_failure()
                    logger.error(f"Final diagnostic results: {diagnostics}")

                    return False

        return False

    def setup_search_service(self, tables_were_swapped: bool = False) -> Dict[str, Any]:
        """
        Setup or update the Cortex Search service for table summaries.

        NOTE: This feature is currently EXPERIMENTAL and may not work in all environments.
        Cortex Search Services have specific requirements that may not be met in all
        Snowflake accounts. If setup fails, the extraction will still succeed -
        Cortex Search is an optional enhancement.

        This method will:
        1. Check if the service already exists
        2. If tables were swapped, recreate the service (table object IDs changed)
        3. If it exists and no swaps, refresh it (more efficient)
        4. If it doesn't exist, create it

        Args:
            tables_were_swapped: True if atomic table swaps just occurred

        Returns:
            Dictionary with operation results and timing information
        """
        # Issue #43: Add clear warning about experimental status
        logger.warning("NOTE: Cortex Search Service is EXPERIMENTAL and may not work in all environments.")
        logger.warning("If setup fails, extraction will still succeed. See Issue #43 for status.")
        logger.info("Setting up Cortex Search service for table summaries...")

        start_time = time.time()
        operation = "unknown"
        success = False

        try:
            # Check if service already exists
            service_exists = self.search_service_exists()

            if service_exists and tables_were_swapped:
                logger.info("Search service exists but tables were swapped, recreating service...")
                operation = "recreate_after_swap"
                success = self.create_search_service()
            elif service_exists:
                logger.info("Search service exists, refreshing...")
                operation = "refresh"
                success = self.refresh_search_service()
            else:
                logger.info("Search service does not exist, creating...")
                operation = "create"
                success = self.create_search_service()

            elapsed_time = time.time() - start_time

            if success:
                logger.info(f"Cortex Search service setup completed successfully ({operation})")
            else:
                logger.error(f"Cortex Search service setup failed ({operation})")

            result = {
                "success": success,
                "operation": operation,
                "service_name": self.search_service_name,
                "elapsed_time": elapsed_time,
                "existed_before": service_exists,
                "tables_were_swapped": tables_were_swapped,
            }

            # Add error message if available
            if not success and hasattr(self, "last_error"):
                result["error"] = self.last_error

            return result

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Cortex Search service setup error: {e}")

            return {
                "success": False,
                "operation": operation,
                "service_name": self.search_service_name,
                "elapsed_time": elapsed_time,
                "error": str(e),
                "existed_before": False,
                "tables_were_swapped": tables_were_swapped,
            }

    def get_search_service_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the search service.

        Returns:
            Dictionary with service information or None if service doesn't exist
        """
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Get detailed service information
                cursor.execute(f"DESCRIBE CORTEX SEARCH SERVICE {self.search_service_name}")

                # Fetch all rows and convert to dictionary
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                if not rows:
                    return None

                # Convert to list of dictionaries
                service_info = {}
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    # Use property name as key
                    service_info[row_dict.get("property", "unknown")] = row_dict.get("value", "")

                return service_info

        except Exception as e:
            logger.warning(f"Failed to get search service info: {e}")
            return None

    def test_search_service(self, query: str = "table") -> bool:
        """
        Test the search service with a simple query.

        Args:
            query: Test query string

        Returns:
            True if search works, False otherwise
        """
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Test query using SEARCH_PREVIEW function
                # Now includes DATABASE_NAME and SCHEMA_NAME for FQN construction
                test_sql = f"""
                SELECT PARSE_JSON(
                  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                      '{self.search_service_name}',
                                          '{{
                        "query": "{query}",
                        "columns": ["TABLE_NAME", "DATABASE_NAME", "SCHEMA_NAME", "TABLE_SUMMARY"],
                        "limit": 1
                    }}'
                  )
                )['results'] as results
                """

                cursor.execute(test_sql)
                result = cursor.fetchone()

                if result and result[0]:
                    logger.debug(f"Search service test successful with query: '{query}'")
                    return True
                else:
                    logger.warning("Search service test returned no results")
                    return False

        except Exception as e:
            logger.warning(f"Search service test failed: {e}")
            return False
