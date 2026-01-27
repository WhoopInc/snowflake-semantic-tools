#!/usr/bin/env python3
"""
Unit tests for SemanticViewBuilder

Tests the table reference extraction and validation logic in semantic view generation.
"""

import pytest

from snowflake_semantic_tools.core.generation.semantic_view_builder import SemanticViewBuilder
from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig


class TestSemanticViewBuilder:
    """Test cases for SemanticViewBuilder table reference validation."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test", user="test", password="test", role="test", warehouse="test", database="test", schema="test"
        )
        return SemanticViewBuilder(config)

    def test_extract_table_references_simple(self, builder):
        """Test extraction of simple table.column references."""
        expression = "COUNT(DISTINCT AI_MESSAGES.CONVERSATION_ID)"
        result = builder._extract_table_references_from_expression(expression)
        assert result == ["ai_messages"], f"Expected ['ai_messages'], got {result}"

    def test_extract_table_references_multiple_tables(self, builder):
        """Test extraction when multiple tables are referenced."""
        expression = """
            (COUNT(AI_CONTEXT.CONTEXT_ID)) / 
            NULLIF((COUNT(DISTINCT AI_MESSAGES_MEMBER_FACING.CONVERSATION_ID)), 0)
        """
        result = builder._extract_table_references_from_expression(expression)
        assert "ai_context" in result
        assert "ai_messages_member_facing" in result
        assert len(result) == 2, f"Expected 2 tables, got {len(result)}: {result}"

    def test_extract_table_references_complex_expression(self, builder):
        """Test extraction from complex nested expressions."""
        expression = """
            CASE 
                WHEN MEMBERSHIP_STATUS_DAILY.IS_ACTIVE = TRUE 
                THEN SUM(AI_COSTS.AMOUNT_VALUE)
                ELSE 0 
            END / NULLIF(COUNT(DISTINCT SINGLE_CUSTOMER_VIEW.USER_ID), 0)
        """
        result = builder._extract_table_references_from_expression(expression)
        assert "membership_status_daily" in result
        assert "ai_costs" in result
        assert "single_customer_view" in result
        assert len(result) == 3, f"Expected 3 tables, got {len(result)}: {result}"

    def test_extract_table_references_no_references(self, builder):
        """Test extraction when no table references exist."""
        expression = "COUNT(*)"
        result = builder._extract_table_references_from_expression(expression)
        assert result == [], f"Expected empty list, got {result}"

    def test_extract_table_references_with_spaces(self, builder):
        """Test extraction with various spacing patterns."""
        expression = "TABLE_A . COLUMN_A + TABLE_B.COLUMN_B"
        result = builder._extract_table_references_from_expression(expression)
        assert "table_a" in result
        assert "table_b" in result

    def test_extract_table_references_mixed_case(self, builder):
        """Test that extraction normalizes to lowercase."""
        expression = "AI_Messages.id + ai_messages.count + Ai_Messages.value"
        result = builder._extract_table_references_from_expression(expression)
        assert result == ["ai_messages"], f"Expected ['ai_messages'], got {result}"

    def test_extract_table_references_ignores_sql_keywords(self, builder):
        """Test that SQL keywords are not treated as table names."""
        expression = "CAST(value AS INTEGER) + EXTRACT(year FROM date)"
        result = builder._extract_table_references_from_expression(expression)
        # CAST and EXTRACT should be filtered out
        assert "cast" not in result
        assert "extract" not in result

    def test_extract_table_references_underscores_and_numbers(self, builder):
        """Test extraction of table names with underscores and numbers."""
        expression = """
            TABLE_123.column_a + 
            my_table_v2.column_b + 
            _private_table.column_c
        """
        result = builder._extract_table_references_from_expression(expression)
        assert "table_123" in result
        assert "my_table_v2" in result
        assert "_private_table" in result


class TestMetricValidationScenarios:
    """Test realistic scenarios for metric validation in semantic views."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test", user="test", password="test", role="test", warehouse="test", database="test", schema="test"
        )
        return SemanticViewBuilder(config)

    def test_scenario_missing_table_in_view(self, builder):
        """
        Test the scenario from the bug report:
        - Semantic view has: [ai_context, single_customer_view]
        - Metric references: AI_MESSAGES_MEMBER_FACING.CONVERSATION_ID
        - Should extract 'ai_messages_member_facing' as missing table
        """
        metric_expression = """
            (COUNT(AI_CONTEXT.CONTEXT_ID)) / 
            NULLIF((COUNT(DISTINCT AI_MESSAGES_MEMBER_FACING.CONVERSATION_ID)), 0)
        """

        # Extract table references
        referenced_tables = builder._extract_table_references_from_expression(metric_expression)

        # Verify we found both tables
        assert "ai_context" in referenced_tables
        assert "ai_messages_member_facing" in referenced_tables

        # Simulate semantic view with only some tables
        available_tables = {"ai_context", "single_customer_view"}

        # Check which tables are missing
        missing = [t for t in referenced_tables if t not in available_tables]

        # The metric should be rejected because ai_messages_member_facing is missing
        assert "ai_messages_member_facing" in missing
        assert len(missing) == 1

    def test_scenario_all_tables_present(self, builder):
        """
        Test valid scenario where all referenced tables are in the view.
        """
        metric_expression = """
            COUNT(DISTINCT AI_CONTEXT.USER_ID)
        """

        referenced_tables = builder._extract_table_references_from_expression(metric_expression)
        available_tables = {"ai_context", "single_customer_view"}

        missing = [t for t in referenced_tables if t not in available_tables]

        # No missing tables - metric should be included
        assert len(missing) == 0

    def test_scenario_composite_metric_multiple_missing_tables(self, builder):
        """
        Test composite metric that references multiple tables not in the view.
        """
        metric_expression = """
            (SUM(AI_COSTS.AMOUNT_VALUE) / 
            NULLIF(COUNT(DISTINCT MEMBERSHIP_STATUS_DAILY.USER_ID), 0)) *
            AVG(AI_TOKEN_USAGE.TOKEN_COUNT)
        """

        referenced_tables = builder._extract_table_references_from_expression(metric_expression)
        available_tables = {"single_customer_view"}

        missing = [t for t in referenced_tables if t not in available_tables]

        # All three tables should be missing
        assert "ai_costs" in missing
        assert "membership_status_daily" in missing
        assert "ai_token_usage" in missing
        assert len(missing) == 3


class TestSemanticViewBuilderUniqueKeys:
    """Test cases for UNIQUE key constraint generation in semantic views."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test",
            password="test",
            role="test",
            warehouse="test",
            database="test_db",
            schema="test_schema",
        )
        return SemanticViewBuilder(config)

    def test_unique_keys_sql_generation(self, builder, monkeypatch):
        """Test that UNIQUE constraint is generated correctly in SQL."""

        # Mock _get_table_info to return test data with unique_keys
        def mock_get_table_info(conn, table_name):
            return {
                "TABLE_NAME": "ORDERS",
                "DATABASE": "TEST_DB",
                "SCHEMA": "TEST_SCHEMA",
                "PRIMARY_KEY": '["order_id"]',
                "UNIQUE_KEYS": '["customer_id", "ordered_at"]',
                "DESCRIPTION": "Orders table with ASOF join support",
                "SYNONYMS": None,
            }

        monkeypatch.setattr(builder, "_get_table_info", mock_get_table_info)

        # Generate table definitions (conn=None since we're mocking _get_table_info)
        table_definitions = builder._build_tables_clause(None, ["orders"])

        # Verify output contains both PRIMARY KEY and UNIQUE
        assert "PRIMARY KEY (ORDER_ID)" in table_definitions
        assert "UNIQUE (CUSTOMER_ID, ORDERED_AT)" in table_definitions

        # Verify UNIQUE comes after PRIMARY KEY
        pk_pos = table_definitions.find("PRIMARY KEY")
        uk_pos = table_definitions.find("UNIQUE")
        assert pk_pos < uk_pos, "UNIQUE constraint should come after PRIMARY KEY"

    def test_unique_keys_without_primary_key(self, builder, monkeypatch):
        """Test UNIQUE constraint works even without PRIMARY KEY."""

        def mock_get_table_info(conn, table_name):
            return {
                "TABLE_NAME": "CUSTOMERS",
                "DATABASE": "TEST_DB",
                "SCHEMA": "TEST_SCHEMA",
                "PRIMARY_KEY": None,
                "UNIQUE_KEYS": '["email", "phone"]',
                "DESCRIPTION": "Customers table",
                "SYNONYMS": None,
            }

        monkeypatch.setattr(builder, "_get_table_info", mock_get_table_info)

        table_definitions = builder._build_tables_clause(None, ["customers"])

        # Should have UNIQUE but not PRIMARY KEY
        assert "UNIQUE (EMAIL, PHONE)" in table_definitions
        assert "PRIMARY KEY" not in table_definitions

    def test_unique_keys_none_or_empty(self, builder, monkeypatch):
        """Test that missing or empty unique_keys doesn't break generation."""

        def mock_get_table_info(conn, table_name):
            return {
                "TABLE_NAME": "PRODUCTS",
                "DATABASE": "TEST_DB",
                "SCHEMA": "TEST_SCHEMA",
                "PRIMARY_KEY": '["product_id"]',
                "UNIQUE_KEYS": None,  # No unique keys
                "DESCRIPTION": "Products table",
                "SYNONYMS": None,
            }

        monkeypatch.setattr(builder, "_get_table_info", mock_get_table_info)

        table_definitions = builder._build_tables_clause(None, ["products"])

        # Should have PRIMARY KEY but not UNIQUE
        assert "PRIMARY KEY (PRODUCT_ID)" in table_definitions
        assert "UNIQUE" not in table_definitions


class TestCAExtension:
    """Test cases for CA extension generation with sample_values for Cortex Analyst."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test",
            password="test",
            role="test",
            warehouse="test",
            database="test_db",
            schema="test_schema",
        )
        builder = SemanticViewBuilder(config)
        builder.metadata_database = "META_DB"
        builder.metadata_schema = "META_SCHEMA"
        return builder

    def test_build_ca_extension_with_sample_values(self, builder, monkeypatch):
        """Test CA extension generation with sample_values from all column types."""

        # Mock _get_dimensions to return dimensions with sample_values
        def mock_get_dimensions(conn, table_name):
            return [
                {
                    "NAME": "customer_type",
                    "EXPR": "CUSTOMER_TYPE",
                    "SAMPLE_VALUES": ["new", "returning"],
                    "DESCRIPTION": "Type of customer",
                },
                {
                    "NAME": "region",
                    "EXPR": "REGION",
                    "SAMPLE_VALUES": ["North", "South", "East", "West"],
                    "DESCRIPTION": "Sales region",
                },
            ]

        # Mock _get_time_dimensions
        def mock_get_time_dimensions(conn, table_name):
            return [
                {
                    "NAME": "order_date",
                    "EXPR": "ORDER_DATE",
                    "SAMPLE_VALUES": ["2025-01-15", "2025-02-20", "2025-03-25"],
                    "DESCRIPTION": "Order date",
                }
            ]

        # Mock _get_facts
        def mock_get_facts(conn, table_name):
            return [
                {
                    "NAME": "amount",
                    "EXPR": "AMOUNT",
                    "SAMPLE_VALUES": ["100", "250", "500"],
                    "DESCRIPTION": "Order amount",
                }
            ]

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["orders"])

        # Verify structure
        assert result.startswith("WITH EXTENSION (CA='")
        assert result.endswith("')")
        assert '"tables"' in result
        assert '"ORDERS"' in result
        assert '"dimensions"' in result
        assert '"time_dimensions"' in result
        assert '"facts"' in result
        assert '"CUSTOMER_TYPE"' in result
        assert '"ORDER_DATE"' in result
        assert '"AMOUNT"' in result
        assert '"new"' in result
        assert '"returning"' in result

    def test_build_ca_extension_empty_sample_values(self, builder, monkeypatch):
        """Test that CA extension is empty when no sample_values exist."""

        # Mock methods to return empty sample_values
        def mock_get_dimensions(conn, table_name):
            return [
                {
                    "NAME": "customer_type",
                    "EXPR": "CUSTOMER_TYPE",
                    "SAMPLE_VALUES": [],  # Empty
                    "DESCRIPTION": "Type of customer",
                }
            ]

        def mock_get_time_dimensions(conn, table_name):
            return []

        def mock_get_facts(conn, table_name):
            return [{"NAME": "amount", "EXPR": "AMOUNT", "SAMPLE_VALUES": None, "DESCRIPTION": "Order amount"}]  # None

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["orders"])

        # Should return empty string when no sample_values
        assert result == ""

    def test_build_ca_extension_escapes_single_quotes(self, builder, monkeypatch):
        """Test that single quotes in sample_values are properly escaped for SQL."""

        def mock_get_dimensions(conn, table_name):
            return [
                {
                    "NAME": "product_name",
                    "EXPR": "PRODUCT_NAME",
                    "SAMPLE_VALUES": ["John's Widget", "O'Brien's Tool", "Normal Product"],
                    "DESCRIPTION": "Product name",
                }
            ]

        def mock_get_time_dimensions(conn, table_name):
            return []

        def mock_get_facts(conn, table_name):
            return []

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["products"])

        # Single quotes should be doubled for SQL
        assert "''" in result  # Escaped quotes
        assert result.startswith("WITH EXTENSION (CA='")

    def test_build_ca_extension_mixed_scenario(self, builder, monkeypatch):
        """Test that only columns with sample_values are included."""

        def mock_get_dimensions(conn, table_name):
            return [
                {
                    "NAME": "customer_type",
                    "EXPR": "CUSTOMER_TYPE",
                    "SAMPLE_VALUES": ["new", "returning"],  # Has values
                    "DESCRIPTION": "Type of customer",
                },
                {
                    "NAME": "customer_id",
                    "EXPR": "CUSTOMER_ID",
                    "SAMPLE_VALUES": [],  # Empty - should be excluded
                    "DESCRIPTION": "Customer ID",
                },
            ]

        def mock_get_time_dimensions(conn, table_name):
            return [
                {
                    "NAME": "order_date",
                    "EXPR": "ORDER_DATE",
                    "SAMPLE_VALUES": None,  # None - should be excluded
                    "DESCRIPTION": "Order date",
                }
            ]

        def mock_get_facts(conn, table_name):
            return [
                {
                    "NAME": "amount",
                    "EXPR": "AMOUNT",
                    "SAMPLE_VALUES": ["100", "200"],  # Has values
                    "DESCRIPTION": "Amount",
                }
            ]

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["orders"])

        # Should include customer_type and amount, exclude customer_id and order_date
        assert '"CUSTOMER_TYPE"' in result
        assert '"AMOUNT"' in result
        assert '"CUSTOMER_ID"' not in result
        # time_dimensions array should not be present since no time_dims have sample_values
        assert '"time_dimensions"' not in result

    def test_build_ca_extension_multiple_tables(self, builder, monkeypatch):
        """Test CA extension with multiple tables."""

        def mock_get_dimensions(conn, table_name):
            if table_name.lower() == "orders":
                return [{"NAME": "order_status", "EXPR": "STATUS", "SAMPLE_VALUES": ["pending", "shipped"]}]
            elif table_name.lower() == "customers":
                return [{"NAME": "customer_tier", "EXPR": "TIER", "SAMPLE_VALUES": ["gold", "silver", "bronze"]}]
            return []

        def mock_get_time_dimensions(conn, table_name):
            return []

        def mock_get_facts(conn, table_name):
            return []

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["orders", "customers"])

        # Both tables should be present
        assert '"ORDERS"' in result
        assert '"CUSTOMERS"' in result
        assert '"ORDER_STATUS"' in result
        assert '"CUSTOMER_TIER"' in result

    def test_build_ca_extension_filters_null_values(self, builder, monkeypatch):
        """Test that None values in sample_values are filtered out."""

        def mock_get_dimensions(conn, table_name):
            return [
                {
                    "NAME": "status",
                    "EXPR": "STATUS",
                    "SAMPLE_VALUES": ["active", None, "inactive", None],  # Contains None
                    "DESCRIPTION": "Status",
                }
            ]

        def mock_get_time_dimensions(conn, table_name):
            return []

        def mock_get_facts(conn, table_name):
            return []

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["orders"])

        # Should include the extension (has non-null values)
        assert result.startswith("WITH EXTENSION (CA='")
        # Null should not appear in the result
        assert "null" not in result.lower() or '"sample_values":["active","inactive"]' in result

    def test_build_ca_extension_includes_is_enum(self, builder, monkeypatch):
        """Test that is_enum=true is included for enum dimensions."""

        def mock_get_dimensions(conn, table_name):
            return [
                {
                    "NAME": "customer_type",
                    "EXPR": "CUSTOMER_TYPE",
                    "SAMPLE_VALUES": ["new", "returning"],
                    "IS_ENUM": True,  # This is an enum
                    "DESCRIPTION": "Type of customer",
                },
                {
                    "NAME": "customer_name",
                    "EXPR": "CUSTOMER_NAME",
                    "SAMPLE_VALUES": ["Alice", "Bob", "Charlie"],
                    "IS_ENUM": False,  # Not an enum
                    "DESCRIPTION": "Customer name",
                },
                {
                    "NAME": "region",
                    "EXPR": "REGION",
                    "SAMPLE_VALUES": ["North", "South"],
                    "IS_ENUM": "true",  # String 'true' should also work
                    "DESCRIPTION": "Region",
                },
            ]

        def mock_get_time_dimensions(conn, table_name):
            return []

        def mock_get_facts(conn, table_name):
            return []

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["customers"])

        # Parse the JSON to verify is_enum
        import json

        start = result.find("WITH EXTENSION (CA='") + len("WITH EXTENSION (CA='")
        end = result.find("')", start)
        ca_json = result[start:end].replace("''", "'")
        parsed = json.loads(ca_json)

        dims = parsed["tables"][0]["dimensions"]

        # Find each dimension and check is_enum
        customer_type = next(d for d in dims if d["name"] == "CUSTOMER_TYPE")
        customer_name = next(d for d in dims if d["name"] == "CUSTOMER_NAME")
        region = next(d for d in dims if d["name"] == "REGION")

        # customer_type should have is_enum=true
        assert customer_type.get("is_enum") == True
        # customer_name should NOT have is_enum (it's false, so omitted)
        assert "is_enum" not in customer_name
        # region should have is_enum=true (string 'true' converted)
        assert region.get("is_enum") == True

    def test_build_ca_extension_escapes_double_quotes_in_values(self, builder, monkeypatch):
        """Test that double quotes in sample values are properly escaped for SQL.

        This is the critical fix for RCA_SEMANTIC_VIEW_INVALID_YAML_ERROR.md:
        Values like '3"' (3 inches) must be properly escaped so the JSON remains
        valid after Snowflake's SQL string parsing.
        """

        def mock_get_dimensions(conn, table_name):
            return [
                {
                    "NAME": "inseam",
                    "EXPR": "INSEAM",
                    # Values with inch marks (double quotes) - the exact issue from the RCA
                    "SAMPLE_VALUES": ['3"', '5"', '7"'],
                    "IS_ENUM": True,
                    "DESCRIPTION": "Inseam size in inches",
                },
                {
                    "NAME": "product_name",
                    "EXPR": "PRODUCT_NAME",
                    # Product name containing inch measurement
                    "SAMPLE_VALUES": ['4.0 Any-Wear Athletic Boxer (Single) 3" Inseam Black L'],
                    "DESCRIPTION": "Product name",
                },
            ]

        def mock_get_time_dimensions(conn, table_name):
            return []

        def mock_get_facts(conn, table_name):
            return []

        monkeypatch.setattr(builder, "_get_dimensions", mock_get_dimensions)
        monkeypatch.setattr(builder, "_get_time_dimensions", mock_get_time_dimensions)
        monkeypatch.setattr(builder, "_get_facts", mock_get_facts)

        result = builder._build_ca_extension(None, ["accessory_sales"])

        # Verify the extension was generated
        assert result.startswith("WITH EXTENSION (CA='")
        assert result.endswith("')")

        # The key fix: backslashes in JSON escape sequences must be doubled
        # so they survive SQL string literal parsing. When Snowflake stores
        # the CA extension, it will interpret \\ as \ and preserve the \"
        # which keeps the JSON valid.
        #
        # Without the fix, json.dumps produces: ["3\"","5\"","7\""]
        # which in SQL becomes: CA='..["3\"","5\"","7\""]..'
        # Snowflake interprets \" and stores: ["3"","5"","7""] (INVALID JSON!)
        #
        # With the fix, we produce: ["3\\\"","5\\\"","7\\\""]
        # which in SQL becomes: CA='..["3\\\"","5\\\"","7\\\""]..'
        # Snowflake interprets \\ as \ and stores: ["3\"","5\"","7\""] (VALID JSON!)

        # The result should contain double-escaped backslashes
        assert '\\\\"' in result or "3\\\\" in result  # The backslashes are doubled


class TestDeferManifestIntegration:
    """Test defer manifest integration in SemanticViewBuilder."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test",
            user="test",
            password="test",
            role="test",
            warehouse="test",
            database="SCRATCH",
            schema="DEV",
        )
        return SemanticViewBuilder(config)

    @pytest.fixture
    def mock_manifest_parser(self, mocker):
        """Create a mock ManifestParser with sample data."""
        from unittest.mock import MagicMock

        mock = MagicMock()
        mock.get_location.side_effect = lambda name: {
            "orders": {"database": "ANALYTICS", "schema": "SALES", "alias": "ORDERS"},
            "customers": {"database": "ANALYTICS", "schema": "CRM", "alias": "CUSTOMERS"},
            "products": {"database": "ANALYTICS_MART", "schema": "PRODUCT", "alias": "PRODUCTS"},
        }.get(name.lower())
        mock.get_target_name.return_value = "prod"
        mock.get_summary.return_value = {
            "loaded": True,
            "total_models": 3,
            "models_by_database": {"ANALYTICS": 2, "ANALYTICS_MART": 1},
        }
        return mock

    def test_build_tables_clause_uses_manifest_locations(self, builder, mock_manifest_parser, mocker):
        """Test that _build_tables_clause uses manifest for database/schema lookup."""
        # Mock _get_table_info to return minimal metadata
        mocker.patch.object(
            builder,
            "_get_table_info",
            side_effect=lambda conn, name: {
                "TABLE_NAME": name.upper(),
                "DATABASE": "SCRATCH",  # This should be overridden by manifest
                "SCHEMA": "DEV",  # This should be overridden by manifest
                "PRIMARY_KEY": '["ID"]',
                "SYNONYMS": "[]",
                "DESCRIPTION": "Test table",
            },
        )

        # Call _build_tables_clause with defer_manifest
        result = builder._build_tables_clause(
            conn=None,
            table_names=["orders"],
            defer_manifest=mock_manifest_parser,
        )

        # Should use ANALYTICS.SALES from manifest, not SCRATCH.DEV from metadata
        assert "ANALYTICS.SALES.ORDERS" in result
        assert "SCRATCH.DEV" not in result

    def test_build_tables_clause_different_databases(self, builder, mock_manifest_parser, mocker):
        """Test that tables from different databases are handled correctly."""
        mocker.patch.object(
            builder,
            "_get_table_info",
            side_effect=lambda conn, name: {
                "TABLE_NAME": name.upper(),
                "DATABASE": "SCRATCH",
                "SCHEMA": "DEV",
                "PRIMARY_KEY": '["ID"]',
                "SYNONYMS": "[]",
                "DESCRIPTION": "Test table",
            },
        )

        # Build for products (should be ANALYTICS_MART.PRODUCT)
        result = builder._build_tables_clause(
            conn=None,
            table_names=["products"],
            defer_manifest=mock_manifest_parser,
        )

        assert "ANALYTICS_MART.PRODUCT.PRODUCTS" in result

    def test_build_tables_clause_fallback_when_not_in_manifest(self, builder, mock_manifest_parser, mocker):
        """Test fallback to metadata when table not found in manifest."""
        mocker.patch.object(
            builder,
            "_get_table_info",
            return_value={
                "TABLE_NAME": "UNKNOWN_TABLE",
                "DATABASE": "MY_DATABASE",
                "SCHEMA": "MY_SCHEMA",
                "PRIMARY_KEY": '["ID"]',
                "SYNONYMS": "[]",
                "DESCRIPTION": "Unknown table",
            },
        )

        # Call with table not in manifest
        result = builder._build_tables_clause(
            conn=None,
            table_names=["unknown_table"],
            defer_manifest=mock_manifest_parser,
        )

        # Should fall back to metadata values
        assert "MY_DATABASE.MY_SCHEMA.UNKNOWN_TABLE" in result

    def test_build_tables_clause_without_manifest_uses_metadata(self, builder, mocker):
        """Test that without a manifest, metadata values are used."""
        mocker.patch.object(
            builder,
            "_get_table_info",
            return_value={
                "TABLE_NAME": "ORDERS",
                "DATABASE": "PRODUCTION",
                "SCHEMA": "SALES",
                "PRIMARY_KEY": '["ID"]',
                "SYNONYMS": "[]",
                "DESCRIPTION": "Orders table",
            },
        )

        result = builder._build_tables_clause(
            conn=None,
            table_names=["orders"],
            defer_manifest=None,
        )

        assert "PRODUCTION.SALES.ORDERS" in result


class TestManifestTargetValidation:
    """Test manifest target validation logic."""

    def test_target_name_available_and_matches(self, tmp_path):
        """Test when manifest has target_name that matches defer target."""
        import json

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        manifest = {
            "metadata": {"target_name": "prod", "dbt_version": "1.7.0"},
            "nodes": {
                "model.test.orders": {
                    "resource_type": "model",
                    "database": "ANALYTICS",
                    "schema": "SALES",
                    "name": "orders",
                }
            },
        }

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        parser = ManifestParser(manifest_path)
        parser.load()

        # Target name matches
        assert parser.get_target_name() == "prod"

    def test_target_name_available_and_mismatches(self, tmp_path):
        """Test when manifest has target_name that doesn't match defer target."""
        import json

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        manifest = {
            "metadata": {"target_name": "dev", "dbt_version": "1.7.0"},
            "nodes": {},
        }

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        parser = ManifestParser(manifest_path)
        parser.load()

        # Target name is "dev", not "prod"
        assert parser.get_target_name() == "dev"
        assert parser.get_target_name() != "prod"

    def test_target_name_not_available(self, tmp_path):
        """Test when manifest doesn't have target_name in metadata."""
        import json

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        manifest = {
            "metadata": {"dbt_version": "1.7.0"},  # No target_name
            "nodes": {
                "model.test.orders": {
                    "resource_type": "model",
                    "database": "SCRATCH",
                    "schema": "DEV",
                    "name": "orders",
                }
            },
        }

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        parser = ManifestParser(manifest_path)
        parser.load()

        # Should return None when target_name not available
        assert parser.get_target_name() is None

    def test_manifest_summary_includes_databases(self, tmp_path):
        """Test that manifest summary correctly lists databases."""
        import json

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        manifest = {
            "metadata": {"dbt_version": "1.7.0"},
            "nodes": {
                "model.test.orders": {
                    "resource_type": "model",
                    "database": "ANALYTICS",
                    "schema": "SALES",
                    "name": "orders",
                },
                "model.test.users": {
                    "resource_type": "model",
                    "database": "ANALYTICS",
                    "schema": "CRM",
                    "name": "users",
                },
                "model.test.products": {
                    "resource_type": "model",
                    "database": "ANALYTICS_MART",
                    "schema": "PRODUCTS",
                    "name": "products",
                },
            },
        }

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        parser = ManifestParser(manifest_path)
        parser.load()

        summary = parser.get_summary()
        assert summary["loaded"] is True
        assert summary["total_models"] == 3
        assert "ANALYTICS" in summary["models_by_database"]
        assert "ANALYTICS_MART" in summary["models_by_database"]
        assert summary["models_by_database"]["ANALYTICS"] == 2
        assert summary["models_by_database"]["ANALYTICS_MART"] == 1


class TestTableNotFoundErrorFormatting:
    """Test error message formatting for table not found errors."""

    @pytest.fixture
    def builder(self):
        """Create a SemanticViewBuilder instance for testing."""
        config = SnowflakeConfig(
            account="test", user="test", password="test", role="test", warehouse="test", database="test", schema="test"
        )
        return SemanticViewBuilder(config)

    def test_format_table_not_found_error_single_table(self, builder):
        """Test error formatting with a single table name."""
        error = ValueError("Table 'test_table' not found in database 'TEST_DB'")
        result = builder._format_table_not_found_error(error, table_names=["test_table"], view_name="test_view")

        assert "Table 'test_table' does not exist" in result
        assert "Semantic view: test_view" in result
        assert "To fix this:" in result
        assert "test_table" in result
        assert "Try: dbt run --select test_table" in result
        assert "dbt model names are case-sensitive" in result
        assert "Verify table names in your semantic view configuration" in result
        assert "Verify you have permissions" in result
        assert "Original error:" in result

    def test_format_table_not_found_error_multiple_tables(self, builder):
        """Test error formatting with multiple table names."""
        error = ValueError("Table 'table1' not found in database 'TEST_DB'")
        result = builder._format_table_not_found_error(error, table_names=["table1", "table2"], view_name="test_view")

        # When a table name appears in the error, it uses the specific table format
        assert "Table 'table1' does not exist" in result
        assert "Semantic view: test_view" in result
        assert "To fix this:" in result
        assert "table1, table2" in result
        assert "Try running each model individually" in result
        assert "dbt run --select table1" in result
        assert "dbt run --select table2" in result
        assert "dbt model names are case-sensitive" in result
        assert "you may need to run upstream models first" in result
        assert "Verify table names in your semantic view configuration" in result

    def test_format_table_not_found_error_multiple_tables_no_match(self, builder):
        """Test error formatting with multiple tables when error doesn't mention a specific table."""
        error = ValueError("Table not found in database")
        result = builder._format_table_not_found_error(error, table_names=["table1", "table2"], view_name="test_view")

        # When no table name matches the error, it uses the multiple tables format
        assert "One or more tables do not exist" in result
        assert "table1" in result
        assert "table2" in result
        assert "Semantic view: test_view" in result

    def test_format_table_not_found_error_no_table_names(self, builder):
        """Test error formatting when table names are not provided."""
        error = ValueError("Table not found in database")
        result = builder._format_table_not_found_error(error, table_names=None, view_name="test_view")

        assert "One or more dbt tables referenced" in result
        assert "Semantic view: test_view" in result
        assert "Original error:" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
