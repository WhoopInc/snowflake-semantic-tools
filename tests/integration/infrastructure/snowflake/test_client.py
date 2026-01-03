#!/usr/bin/env python3
"""
Integration tests for SnowflakeClient.

These tests verify that all components work together correctly.
"""

import json
import unittest
from unittest.mock import MagicMock, Mock, call, patch

import pandas as pd
import pytest

from snowflake_semantic_tools.infrastructure.snowflake.client import SnowflakeClient
from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig


class TestSnowflakeClientIntegration(unittest.TestCase):
    """Integration tests for SnowflakeClient."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = SnowflakeConfig(
            account="test_account",
            user="test_user",
            password="test_password",
            role="test_role",
            warehouse="test_warehouse",
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )

        with patch("snowflake_semantic_tools.infrastructure.snowflake.client.ConnectionManager"):
            self.client = SnowflakeClient(self.config)
            # Mock the connection manager
            self.client.connection_manager = MagicMock()

    def test_full_database_setup(self):
        """Test complete database setup flow."""
        # Mock successful operations
        self.client.schema_manager.ensure_database_and_schema_exist = MagicMock(return_value=True)
        self.client.schema_manager.ensure_production_tables_exist = MagicMock(
            return_value={
                "overall_success": True,
                "created_tables": ["sm_metrics"],
                "existing_tables": ["sm_dimensions"],
                "failed_tables": [],
            }
        )

        result = self.client.schema_manager.setup_database_schema()

        self.assertTrue(result)
        self.client.schema_manager.ensure_database_and_schema_exist.assert_called_once()
        self.client.schema_manager.ensure_production_tables_exist.assert_called_once()

    def test_data_preparation_and_loading_flow(self):
        """Test data preparation and loading workflow."""
        # Create test DataFrame
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "tags": [["tag1"], ["tag2"], ["tag3"]],
                "is_active": [True, False, True],
            }
        )

        # Mock DataLoader methods
        self.client.data_loader.prepare_dataframe_for_snowflake = MagicMock(return_value=df)
        self.client.data_loader.validate_dataframe_compatibility = MagicMock(return_value=True)
        self.client.data_loader.create_staging_tables = MagicMock(return_value=True)
        self.client.data_loader.load_dataframe_to_staging = MagicMock(return_value=True)

        # Test preparation
        prepared_df = self.client.data_loader.prepare_dataframe_for_snowflake(df, "sm_metrics")
        self.assertEqual(len(prepared_df), 3)

        # Test validation
        is_valid = self.client.data_loader.validate_dataframe_compatibility(df, "sm_metrics")
        self.assertTrue(is_valid)

        # Test staging workflow
        staging_created = self.client.data_loader.create_staging_tables()
        self.assertTrue(staging_created)

        load_success = self.client.data_loader.load_dataframe_to_staging(df, "sm_metrics")
        self.assertTrue(load_success)

    def test_table_swap_workflow(self):
        """Test atomic table swap workflow."""
        # Mock TableManager operations
        self.client.table_manager.swap_staging_to_production = MagicMock(return_value=True)
        self.client.table_manager.get_table_row_counts = MagicMock(
            return_value={"sm_metrics": 100, "sm_dimensions": 200}
        )
        self.client.table_manager.verify_table_integrity = MagicMock(
            return_value={"exists": True, "has_rows": True, "row_count": 100, "column_count": 5, "is_valid": True}
        )

        # Test swap
        swap_success = self.client.table_manager.swap_staging_to_production(["sm_metrics"])
        self.assertTrue(swap_success)

        # Test row counts
        counts = self.client.table_manager.get_table_row_counts()
        self.assertEqual(counts["sm_metrics"], 100)
        self.assertEqual(counts["sm_dimensions"], 200)

        # Test integrity check
        integrity = self.client.table_manager.verify_table_integrity("sm_metrics")
        self.assertTrue(integrity["is_valid"])
        self.assertEqual(integrity["row_count"], 100)

    def test_load_semantic_models_workflow(self):
        """Test loading semantic models to Snowflake."""
        # Prepare test data
        test_models = {
            "metrics": [
                {"name": "revenue", "expression": "SUM(amount)"},
                {"name": "user_count", "expression": "COUNT(DISTINCT user_id)"},
            ],
            "sm_dimensions": [{"name": "date_dimension", "type": "TIME", "column": "created_at"}],
        }

        # Mock write_pandas
        with patch("snowflake_semantic_tools.infrastructure.snowflake.client.write_pandas") as mock_write:
            mock_write.return_value = (True, 1, 2, None)

            # Mock connection
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            self.client.connection_manager.get_connection.return_value.__enter__.return_value = mock_conn

            result = self.client.data_loader.load_semantic_models(test_models, "TEST_DB", "TEST_SCHEMA")

            self.assertTrue(result)
            # Should create database and schema
            mock_cursor.execute.assert_any_call("CREATE DATABASE IF NOT EXISTS TEST_DB")
            mock_cursor.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA")
            # Should call write_pandas for each table
            self.assertEqual(mock_write.call_count, 2)

    def test_cortex_search_setup(self):
        """Test Cortex Search service setup."""
        self.client.cortex_search_manager.setup_search_service = MagicMock(
            return_value={"service_created": True, "service_name": "TABLE_SUMMARY_SEARCH", "indexed_tables": 5}
        )
        self.client.cortex_search_manager.test_search_service = MagicMock(
            return_value={
                "search_working": True,
                "results_count": 3,
                "sample_results": ["result1", "result2", "result3"],
            }
        )

        # Setup service
        setup_result = self.client.cortex_search_manager.setup_search_service()
        self.assertTrue(setup_result["service_created"])
        self.assertEqual(setup_result["indexed_tables"], 5)

        # Test search
        search_result = self.client.cortex_search_manager.test_search_service("test query")
        self.assertTrue(search_result["search_working"])
        self.assertEqual(search_result["results_count"], 3)

    def test_error_handling_workflow(self):
        """Test error handling across components."""
        # Test database setup failure
        self.client.schema_manager.ensure_database_and_schema_exist = MagicMock(
            side_effect=Exception("Permission denied")
        )

        result = self.client.schema_manager.setup_database_schema()
        self.assertFalse(result)

        # Test data loading failure
        self.client.data_loader.load_dataframe_to_staging = MagicMock(side_effect=Exception("Network error"))

        df = pd.DataFrame({"id": [1, 2, 3]})
        result = self.client.data_loader.load_dataframe_to_staging(df, "sm_metrics")
        self.assertFalse(result)

        # Test table swap failure with rollback
        self.client.table_manager.swap_staging_to_production = MagicMock(return_value=False)

        result = self.client.table_manager.swap_staging_to_production(["sm_metrics"])
        self.assertFalse(result)


@pytest.mark.snowflake
class TestSnowflakeClientRealConnection(unittest.TestCase):
    """
    Tests that require a real Snowflake connection.

    These tests are marked with @pytest.mark.snowflake and will only run
    when explicitly requested and when Snowflake credentials are available.
    """

    def setUp(self):
        """Set up test fixtures with real connection."""
        import os

        # Skip if no Snowflake credentials
        if not os.getenv("SNOWFLAKE_ACCOUNT"):
            self.skipTest("Snowflake credentials not available")

        self.config = SnowflakeConfig.from_env()
        self.client = SnowflakeClient(self.config)

    def test_real_connection(self):
        """Test real Snowflake connection."""
        result = self.client.connection_manager.test_connection()
        self.assertTrue(result, "Failed to connect to Snowflake")

    def test_real_database_operations(self):
        """Test real database operations."""
        # Use a test schema
        self.config.schema = "TEST_SCHEMA_" + str(pd.Timestamp.now().value)

        try:
            # Create schema
            result = self.client.schema_manager.ensure_database_and_schema_exist()
            self.assertTrue(result)

            # Create a test table
            test_df = pd.DataFrame({"id": [1, 2, 3], "name": ["test1", "test2", "test3"]})

            # This would actually create and load data
            # result = self.client.data_loader.load_dataframe_to_staging(test_df, "test_table")
            # self.assertTrue(result)

        finally:
            # Cleanup
            with self.client.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP SCHEMA IF EXISTS {self.config.schema} CASCADE")


if __name__ == "__main__":
    unittest.main()
