#!/usr/bin/env python3
"""
Integration tests for Snowflake semantic view synonym constraints.

These tests verify the assumption that:
1. Duplicate TABLE synonyms within a semantic view cause Snowflake errors
2. Duplicate COLUMN synonyms across tables are ALLOWED (same concept in multiple tables)

These tests require a real Snowflake connection and are marked with @pytest.mark.snowflake.
Run with: pytest -m snowflake tests/integration/infrastructure/snowflake/test_semantic_view_synonyms.py
"""

import os
import unittest

import pytest


@pytest.mark.snowflake
class TestSemanticViewSynonymConstraints(unittest.TestCase):
    """
    Tests to verify Snowflake's synonym uniqueness constraints for semantic views.

    These tests require:
    - SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD environment variables
    - A database with tables to reference in semantic views
    - Permissions to create semantic views

    The tests validate our assumption that only TABLE-level synonyms must be unique,
    while column synonyms can be shared across tables in the same view.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures with real connection."""
        if not os.getenv("SNOWFLAKE_ACCOUNT"):
            pytest.skip("Snowflake credentials not available")

        from snowflake_semantic_tools.infrastructure.snowflake.client import SnowflakeClient
        from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig

        cls.config = SnowflakeConfig.from_env()
        cls.client = SnowflakeClient(cls.config)

        # Test tables for semantic view creation
        cls.test_database = cls.config.database
        cls.test_schema = cls.config.schema
        cls.test_view_name = "SST_TEST_SEMANTIC_VIEW"

    def tearDown(self):
        """Clean up test semantic views."""
        try:
            with self.client.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"DROP SEMANTIC VIEW IF EXISTS {self.test_database}.{self.test_schema}.{self.test_view_name}"
                )
        except Exception:
            pass  # Ignore cleanup errors

    def test_duplicate_table_synonyms_fail(self):
        """
        Test that duplicate TABLE synonyms in the same semantic view fail.

        This test creates a semantic view with two tables that have the same
        table-level synonym and verifies that Snowflake rejects it.
        """
        # Create two test tables first
        with self.client.connection_manager.get_connection() as conn:
            cursor = conn.cursor()

            # Create test tables
            cursor.execute(f"""
                CREATE OR REPLACE TABLE {self.test_database}.{self.test_schema}.SST_TEST_ORDERS (
                    order_id INT,
                    customer_id INT,
                    amount DECIMAL(10,2)
                )
            """)

            cursor.execute(f"""
                CREATE OR REPLACE TABLE {self.test_database}.{self.test_schema}.SST_TEST_ORDER_ITEMS (
                    item_id INT,
                    order_id INT,
                    product_id INT,
                    quantity INT
                )
            """)

            # Try to create semantic view with duplicate table synonyms
            # Both tables have synonym "order details"
            create_view_sql = f"""
                CREATE OR REPLACE SEMANTIC VIEW {self.test_database}.{self.test_schema}.{self.test_view_name}
                TABLES = (
                    {self.test_database}.{self.test_schema}.SST_TEST_ORDERS
                        AS orders
                        WITH SYNONYMS = ('order details', 'purchase records'),
                    {self.test_database}.{self.test_schema}.SST_TEST_ORDER_ITEMS
                        AS order_items
                        WITH SYNONYMS = ('order details', 'line items')
                )
                COMMENT = 'Test view for duplicate synonym validation'
            """

            # This should fail with a duplicate synonym error
            with pytest.raises(Exception) as exc_info:
                cursor.execute(create_view_sql)

            error_msg = str(exc_info.value).lower()
            assert "duplicate" in error_msg or "synonym" in error_msg, (
                f"Expected duplicate synonym error, got: {exc_info.value}"
            )

            # Clean up test tables
            cursor.execute(f"DROP TABLE IF EXISTS {self.test_database}.{self.test_schema}.SST_TEST_ORDERS")
            cursor.execute(f"DROP TABLE IF EXISTS {self.test_database}.{self.test_schema}.SST_TEST_ORDER_ITEMS")

    def test_duplicate_column_synonyms_succeed(self):
        """
        Test that duplicate COLUMN synonyms across tables are ALLOWED.

        This test creates a semantic view where different tables have columns
        with the same synonym (e.g., "customer identifier" for customer_id in
        both orders and customers tables). This should succeed.

        This validates our assumption that column synonyms can be shared.
        """
        with self.client.connection_manager.get_connection() as conn:
            cursor = conn.cursor()

            # Create test tables with columns that share the same semantic meaning
            cursor.execute(f"""
                CREATE OR REPLACE TABLE {self.test_database}.{self.test_schema}.SST_TEST_CUSTOMERS (
                    customer_id INT,
                    name VARCHAR(100)
                )
            """)

            cursor.execute(f"""
                CREATE OR REPLACE TABLE {self.test_database}.{self.test_schema}.SST_TEST_ORDERS_2 (
                    order_id INT,
                    customer_id INT,
                    amount DECIMAL(10,2)
                )
            """)

            # Create semantic view with duplicate COLUMN synonyms but unique table synonyms
            # Both tables have a customer_id column with synonym "customer identifier"
            create_view_sql = f"""
                CREATE OR REPLACE SEMANTIC VIEW {self.test_database}.{self.test_schema}.{self.test_view_name}
                TABLES = (
                    {self.test_database}.{self.test_schema}.SST_TEST_CUSTOMERS
                        AS customers
                        WITH SYNONYMS = ('customer master data'),
                    {self.test_database}.{self.test_schema}.SST_TEST_ORDERS_2
                        AS orders
                        WITH SYNONYMS = ('purchase transactions')
                )
                DIMENSIONS = (
                    customers.customer_id
                        AS cust_id
                        TYPE dimension
                        WITH SYNONYMS = ('customer identifier', 'client id'),
                    orders.customer_id
                        AS order_customer_id
                        TYPE dimension
                        WITH SYNONYMS = ('customer identifier', 'buyer id')
                )
                COMMENT = 'Test view for column synonym validation'
            """

            # This SHOULD succeed - column synonyms can duplicate
            try:
                cursor.execute(create_view_sql)
                # If we get here, the assumption is correct: column synonyms can duplicate
            except Exception as e:
                # If this fails due to duplicate column synonyms, our assumption is wrong
                error_msg = str(e).lower()
                if "duplicate" in error_msg and "synonym" in error_msg:
                    pytest.fail(
                        "Duplicate COLUMN synonyms also fail in Snowflake. "
                        "Update the validation logic to check column synonyms too. "
                        f"Error: {e}"
                    )
                else:
                    # Some other error, re-raise
                    raise

            # Clean up test tables
            cursor.execute(f"DROP TABLE IF EXISTS {self.test_database}.{self.test_schema}.SST_TEST_CUSTOMERS")
            cursor.execute(f"DROP TABLE IF EXISTS {self.test_database}.{self.test_schema}.SST_TEST_ORDERS_2")


if __name__ == "__main__":
    unittest.main()

