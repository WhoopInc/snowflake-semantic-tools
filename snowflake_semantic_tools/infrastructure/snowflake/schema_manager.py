#!/usr/bin/env python3
"""
Snowflake Schema Manager

Handles database and schema creation, table existence checking, table creation,
and other schema-related operations. Manages the structural aspects of the Snowflake environment.
"""

import os
from typing import List, Optional

import pandas as pd

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.infrastructure.snowflake.data_loader import build_column_definitions
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("snowflake.schema_manager")


class SchemaManager:
    """Manages Snowflake database schemas, tables, and structural operations."""

    def __init__(self, connection_manager: ConnectionManager, config: SnowflakeConfig):
        """
        Initialize the schema manager with a connection manager and config.

        Args:
            connection_manager: Connection manager instance
            config: SnowflakeConfig instance
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

    def ensure_database_and_schema_exist(self) -> bool:
        """
        Ensure the target database and schema exist, creating them if necessary.

        Returns:
            True if database and schema exist/were created successfully
        """
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Always import fresh to get the updated global config
                current_config = self.config

                database_name = current_config.snowflake_database
                schema_name = current_config.snowflake_schema

                # Check/Create Database first
                database_exists = False
                if current_config.auto_create_database:
                    logger.debug(f"Checking database '{database_name}' exists...")
                    cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
                    database_exists = cursor.fetchone() is not None

                    if not database_exists:
                        # Database doesn't exist - prompt for both database AND schema creation
                        if os.getenv("CONFIRM_DB_SCHEMA_CREATION", "true").lower() == "true":
                            import click

                            click.echo(
                                f"\nWARNING: Database '{database_name}' and schema '{schema_name}' do not exist and will be created."
                            )
                            response = input("Do you want to proceed? (y/N): ").strip().lower()
                            if response not in ["y", "yes"]:
                                raise Exception(
                                    f"Database and schema creation cancelled by user for: {database_name}.{schema_name}"
                                )
                        else:
                            logger.info(f"Auto-confirming database and schema creation due to --yes flag")

                        logger.info(f"Creating database: {database_name}")
                        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name.upper()}")
                        logger.info(f"Database '{database_name}' created successfully")
                    else:
                        logger.debug(f"Database '{database_name}' already exists")

                # Use the database
                cursor.execute(f"USE DATABASE {current_config.snowflake_database.upper()}")

                # Check/Create Schema (only if database already existed, otherwise it was handled above)
                if current_config.auto_create_schema:
                    logger.debug(f"Checking schema '{schema_name}' exists...")
                    cursor.execute(f"SHOW SCHEMAS LIKE '{schema_name}'")
                    schema_exists = cursor.fetchone() is not None

                    if not schema_exists:
                        # If database was just created, schema was already confirmed above
                        if database_exists:
                            # Database existed but schema doesn't - prompt only for schema
                            if os.getenv("CONFIRM_DB_SCHEMA_CREATION", "true").lower() == "true":
                                import click

                                click.echo(f"\nWARNING: Schema '{schema_name}' does not exist and will be created.")
                                response = input("Do you want to proceed? (y/N): ").strip().lower()
                                if response not in ["y", "yes"]:
                                    raise Exception(f"Schema creation cancelled by user for: {schema_name}")
                            else:
                                logger.info(f"Auto-confirming schema creation due to --yes flag")

                        logger.info(f"Creating schema: {schema_name}")
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name.upper()}")
                        logger.info(f"Schema '{schema_name}' created successfully")
                    else:
                        logger.debug(f"Schema '{schema_name}' already exists")

                # Use the schema
                cursor.execute(f"USE SCHEMA {current_config.snowflake_schema.upper()}")

                return True

        except Exception as e:
            logger.error(f"Failed to ensure database and schema exist: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                return cursor.fetchone() is not None
        except Exception:
            return False

    def create_table_with_schema(self, table_name: str, table_key: str) -> bool:
        """
        Create a table with the correct schema based on SemanticTableSchemas.

        Args:
            table_name: Name of the table to create
            table_key: Key in SemanticTableSchemas for the table definition

        Returns:
            True if successful, False otherwise
        """
        from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

        schemas = SemanticTableSchemas.get_all_schemas()
        if table_key not in schemas:
            logger.error(f"Unknown table key: {table_key}")
            return False

        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                schema_def = schemas[table_key]
                columns_sql = build_column_definitions(schema_def.columns)

                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"

                cursor.execute(create_sql)
                logger.debug(f"Created table: {table_name}")

                return True

        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False

    def backup_table(self, table_name: str) -> Optional[str]:
        """
        Create a backup of a table before making changes.

        Args:
            table_name: Name of the table to backup

        Returns:
            Name of the backup table or None if failed
        """
        # Always import fresh to get the updated global config
        current_config = self.config

        if not current_config.backup_before_changes:
            return None

        try:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{table_name}_backup_{timestamp}"

            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Create backup table
                cursor.execute(f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}")
                logger.info(f"Created backup table: {backup_name}")

                return backup_name

        except Exception as e:
            logger.error(f"Failed to backup table {table_name}: {e}")
            return None

    def ensure_production_tables_exist(self) -> bool:
        """
        Ensure all production tables exist with correct schemas.

        Returns:
            True if successful, False otherwise
        """
        # Always import fresh to get the updated global config
        current_config = self.config

        if not current_config.auto_create_tables:
            logger.info("Auto-create tables disabled, skipping table creation")
            return True

        failed_tables = []
        created_tables = []
        existing_tables = []

        try:
            from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas

            schemas = SemanticTableSchemas.get_all_schemas()

            # Batch check table existence for better performance
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Check all tables in one go
                tables_to_create = []
                for table_key in schemas.keys():
                    production_table = self.table_names.get(table_key, table_key)

                    try:
                        cursor.execute(f"SHOW TABLES LIKE '{production_table}'")
                        if cursor.fetchone() is not None:
                            existing_tables.append(production_table)
                            logger.debug(f"Production table {production_table} already exists")
                        else:
                            tables_to_create.append((table_key, production_table))
                    except Exception as e:
                        logger.warning(f"Error checking table {production_table}: {e}")
                        failed_tables.append(production_table)

                # Create missing tables in batch
                for table_key, production_table in tables_to_create:
                    try:
                        logger.debug(f"Creating production table: {production_table}")
                        schema_def = schemas[table_key]
                        columns_sql = build_column_definitions(schema_def.columns)
                        create_sql = f"CREATE TABLE IF NOT EXISTS {production_table} ({columns_sql})"

                        cursor.execute(create_sql)
                        created_tables.append(production_table)

                    except Exception as e:
                        failed_tables.append(production_table)
                        logger.error(f"Error creating table {production_table}: {e}")
                        # Continue with other tables

            # Log summary
            logger.info(
                f"Tables: {len(created_tables)} created, {len(existing_tables)} existed, {len(failed_tables)} failed"
            )
            if failed_tables:
                logger.warning(f"Failed tables: {', '.join(failed_tables)}")

            # Return True if we have at least some tables (created or existing)
            return len(created_tables) + len(existing_tables) > 0

        except Exception as e:
            logger.error(f"Failed to ensure production tables exist: {e}")
            return False

    def setup_database_schema(self) -> bool:
        """
        Complete database setup: ensure database/schema exist and create tables.

        Returns:
            True if setup successful, False otherwise
        """
        logger.info("Setting up database schema...")

        # Step 1: Ensure database and schema exist
        if not self.ensure_database_and_schema_exist():
            logger.error("Failed to ensure database and schema exist")
            return False

        # Step 2: Ensure production tables exist with correct schemas
        if not self.ensure_production_tables_exist():
            logger.error("Failed to ensure production tables exist")
            return False

        logger.info("Database schema setup completed successfully")
        return True
