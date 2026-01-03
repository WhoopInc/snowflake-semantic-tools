#!/usr/bin/env python3
"""
Snowflake Client Orchestrator

Central hub for all Snowflake database operations with specialized manager delegation.

Implements the Facade pattern to provide a unified interface to complex Snowflake
operations while delegating specialized tasks to dedicated managers:
- Connection lifecycle and pooling
- Schema and table management
- Bulk data loading with pandas
- Cortex Search Service configuration
- Query execution and error handling

This architecture ensures clean separation of concerns and maintainable code
while providing a simple API for consumers.
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.infrastructure.snowflake.cortex_search_manager import CortexSearchManager
from snowflake_semantic_tools.infrastructure.snowflake.data_loader import DataLoader
from snowflake_semantic_tools.infrastructure.snowflake.metadata_manager import MetadataManager
from snowflake_semantic_tools.infrastructure.snowflake.schema_manager import SchemaManager
from snowflake_semantic_tools.infrastructure.snowflake.table_manager import TableManager
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("infrastructure.snowflake.client")


class SnowflakeClient:
    """
    Unified interface for all Snowflake database operations.

    Orchestrates specialized managers to provide comprehensive Snowflake
    functionality through a single, consistent API. Each manager handles
    a specific domain:

    - **ConnectionManager**: Database connections and authentication (use `client.connection_manager`)
    - **SchemaManager**: Database/schema creation and validation (use `client.schema_manager`)
    - **TableManager**: Table operations and metadata queries (use `client.table_manager`)
    - **DataLoader**: High-performance bulk data loading and semantic model extraction (use `client.data_loader`)
    - **MetadataManager**: Schema inspection, sample values, and enrichment queries (use `client.metadata_manager`)
    - **CortexSearchManager**: Cortex Search Service management (use `client.cortex_search_manager`)

    Access managers directly: `client.connection_manager.get_connection()`, `client.schema_manager.ensure_database_and_schema_exist()`, etc.

    The `execute_query()` method provides smart query execution with session handling.
    """

    def __init__(self, config: SnowflakeConfig):
        """
        Initialize the Snowflake client with all required components.

        Args:
            config: SnowflakeConfig instance
        """
        # Initialize core components with dependency injection
        self.connection_manager = ConnectionManager(config)
        self.schema_manager = SchemaManager(self.connection_manager, config)
        self.data_loader = DataLoader(self.connection_manager, config)
        self.metadata_manager = MetadataManager(self.connection_manager, config)
        self.table_manager = TableManager(self.connection_manager, config)
        self.cortex_search_manager = CortexSearchManager(self.connection_manager, config)

    @classmethod
    def from_session(cls, session, database: str = None, schema: str = None):
        """
        Create a SnowflakeClient from an existing session.

        This is a convenience method for cases where you already have a
        snowflake.connector session and want to use it with the client.

        Args:
            session: Existing snowflake.connector session
            database: Optional database name for config
            schema: Optional schema name for config

        Returns:
            SnowflakeClient instance configured to use the provided session
        """
        # Try to create config from dbt profile first
        try:
            config = SnowflakeConfig.from_dbt_profile(
                database_override=database,
                schema_override=schema,
            )
        except Exception:
            # Fallback to extracting info from the session
            session_account = getattr(session, "account", None)
            session_user = getattr(session, "user", None)
            session_role = getattr(session, "role", None)
            session_warehouse = getattr(session, "warehouse", None)
            session_database = getattr(session, "database", None) or database
            session_schema = getattr(session, "schema", None) or schema

            if not session_account or not session_user:
                raise ValueError(
                    "Session missing account/user info and no dbt profiles.yml found. "
                    "Either provide a session with account/user attributes or set up ~/.dbt/profiles.yml."
                )

            if not session_database or not session_schema:
                raise ValueError(
                    "Database and schema are required. Either set up profiles.yml "
                    "or pass database/schema parameters."
                )

            config = SnowflakeConfig(
                account=session_account,
                user=session_user,
                role=session_role,
                warehouse=session_warehouse,
                database=session_database,
                schema=session_schema,
            )

        # Create the client
        client = cls(config)

        # Store the session for direct use
        client._external_session = session

        return client

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Provides smart query execution with session handling for external sessions,
        Snowpark sessions, and standard connection manager usage.

        Args:
            query: SQL query to execute

        Returns:
            DataFrame with query results

        Raises:
            Exception: If query execution fails
        """
        try:
            # Use external session if provided
            if hasattr(self, "_external_session") and self._external_session:
                # Check if it's a Snowpark session vs raw connector session
                session = self._external_session
                if hasattr(session, "sql"):
                    # Snowpark Session - use DataFrame API
                    # Check if it's a SELECT or DESCRIBE query (can use to_pandas()) or DDL/DML (use collect())
                    query_upper = query.strip().upper()
                    if query_upper.startswith("SELECT") or query_upper.startswith("DESCRIBE"):
                        df = session.sql(query).to_pandas()
                        return df
                    else:
                        # For non-SELECT statements (CREATE, PUT, etc.), use collect()
                        result = session.sql(query).collect()
                        # Return empty DataFrame for DDL/DML operations
                        return pd.DataFrame()
                else:
                    # Raw connector session - use pandas read_sql
                    return pd.read_sql(query, session)
            else:
                # Use the connection manager
                with self.connection_manager.get_connection() as conn:
                    # Use pandas read_sql for consistency
                    return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            # Re-raise the exception so callers can handle it appropriately
            raise
