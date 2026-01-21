"""Unit tests for semantic view generation service."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from snowflake_semantic_tools.services.generate_semantic_views import (
    GenerateConfig,
    GenerateResult,
    MetadataClient,
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
)


class TestMetadataClientDescriptionQuery:
    """Test that MetadataClient queries include description."""

    def test_get_available_views_includes_description(self):
        """Verify that get_available_views query fetches description column."""
        mock_client = MagicMock()
        mock_df = pd.DataFrame(
            [
                {
                    "NAME": "test_view",
                    "TABLES": '["orders", "customers"]',
                    "DESCRIPTION": "Comprehensive analytics view for customer orders",
                }
            ]
        )
        mock_client.execute_query.return_value = mock_df

        metadata_client = MetadataClient(mock_client, "TEST_DB", "TEST_SCHEMA")
        views = metadata_client.get_available_views()

        # Verify the query was called
        mock_client.execute_query.assert_called_once()
        query = mock_client.execute_query.call_args[0][0]

        # Verify description is in the query
        assert "description" in query.lower()

        # Verify the result includes description
        assert len(views) == 1
        assert views[0]["DESCRIPTION"] == "Comprehensive analytics view for customer orders"

    def test_get_available_views_handles_null_description(self):
        """Verify null description doesn't cause errors."""
        mock_client = MagicMock()
        mock_df = pd.DataFrame(
            [
                {
                    "NAME": "test_view",
                    "TABLES": '["orders"]',
                    "DESCRIPTION": None,
                }
            ]
        )
        mock_client.execute_query.return_value = mock_df

        metadata_client = MetadataClient(mock_client, "TEST_DB", "TEST_SCHEMA")
        views = metadata_client.get_available_views()

        assert len(views) == 1
        assert views[0]["DESCRIPTION"] is None


class TestGenerateMethodDescriptionFlow:
    """Test that descriptions flow correctly through the generate() method."""

    @patch("snowflake_semantic_tools.services.generate_semantic_views.SemanticViewBuilder")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionManager")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.SnowflakeClient")
    def test_description_included_in_view_configs(
        self, mock_snowflake_client_class, mock_conn_manager_class, mock_builder_class
    ):
        """Verify that description from metadata is included in view_configs passed to execute()."""
        # Mock SnowflakeClient for metadata queries
        mock_client_instance = MagicMock()
        mock_snowflake_client_class.return_value = mock_client_instance

        # Mock the metadata query result with description
        mock_df = pd.DataFrame(
            [
                {
                    "NAME": "test_view",
                    "TABLES": '["orders", "customers"]',
                    "DESCRIPTION": "Comprehensive analytics view for customer orders",
                }
            ]
        )

        # Mock validation queries
        mock_schema_df = pd.DataFrame([{"SCHEMA_EXISTS": 1}])
        mock_tables_df = pd.DataFrame([{"TABLE_NAME": "SM_SEMANTIC_VIEWS"}])

        def query_side_effect(query):
            if "information_schema.schemata" in query:
                return mock_schema_df
            elif "information_schema.tables" in query:
                return mock_tables_df
            else:
                return mock_df

        mock_client_instance.execute_query.side_effect = query_side_effect

        # Mock SnowflakeConfig
        mock_config = MagicMock()

        # Create the service (mocked internals)
        service = SemanticViewGenerationService(mock_config)

        # Capture what gets passed to execute()
        captured_config = None

        def capture_execute(config, **kwargs):
            nonlocal captured_config
            captured_config = config
            return GenerateResult(
                success=True,
                views_generated=["test_view"],
                views_failed=[],
                errors=[],
                sql_statements={"test_view": "CREATE ..."},
            )

        with patch.object(service, "execute", side_effect=capture_execute):
            config = UnifiedGenerationConfig(
                target_database="TEST_DB",
                target_schema="TEST_SCHEMA",
                metadata_database="TEST_DB",
                metadata_schema="TEST_SCHEMA",
            )
            service.generate(config)

        # Verify the description was passed through
        assert captured_config is not None
        view_config = captured_config.views_to_generate[0]
        assert view_config.get("description") == "Comprehensive analytics view for customer orders"

    @patch("snowflake_semantic_tools.services.generate_semantic_views.SemanticViewBuilder")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionManager")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.SnowflakeClient")
    def test_empty_description_handled_gracefully(
        self, mock_snowflake_client_class, mock_conn_manager_class, mock_builder_class
    ):
        """Verify that empty/None descriptions don't cause errors."""
        mock_client_instance = MagicMock()
        mock_snowflake_client_class.return_value = mock_client_instance

        # Mock the metadata query result with None description
        mock_df = pd.DataFrame(
            [
                {
                    "NAME": "test_view",
                    "TABLES": '["orders"]',
                    "DESCRIPTION": None,
                }
            ]
        )

        mock_schema_df = pd.DataFrame([{"SCHEMA_EXISTS": 1}])
        mock_tables_df = pd.DataFrame([{"TABLE_NAME": "SM_SEMANTIC_VIEWS"}])

        def query_side_effect(query):
            if "information_schema.schemata" in query:
                return mock_schema_df
            elif "information_schema.tables" in query:
                return mock_tables_df
            else:
                return mock_df

        mock_client_instance.execute_query.side_effect = query_side_effect

        mock_config = MagicMock()
        service = SemanticViewGenerationService(mock_config)

        captured_config = None

        def capture_execute(config, **kwargs):
            nonlocal captured_config
            captured_config = config
            return GenerateResult(
                success=True,
                views_generated=["test_view"],
                views_failed=[],
                errors=[],
            )

        with patch.object(service, "execute", side_effect=capture_execute):
            config = UnifiedGenerationConfig(
                target_database="TEST_DB",
                target_schema="TEST_SCHEMA",
                metadata_database="TEST_DB",
                metadata_schema="TEST_SCHEMA",
            )
            service.generate(config)

        # Verify empty string is used as fallback for None
        view_config = captured_config.views_to_generate[0]
        assert view_config.get("description") == ""


class TestExecuteMethodDescriptionPassthrough:
    """Test that execute() passes description to build_semantic_view."""

    @patch("snowflake_semantic_tools.services.generate_semantic_views.SemanticViewBuilder")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionManager")
    def test_description_passed_to_builder(self, mock_conn_manager_class, mock_builder_class):
        """Verify execute() passes description from view_config to builder."""
        # Set up mock builder
        mock_builder_instance = MagicMock()
        mock_builder_instance.metadata_database = "TEST_DB"
        mock_builder_instance.metadata_schema = "TEST_SCHEMA"
        mock_builder_instance.build_semantic_view.return_value = {
            "success": True,
            "sql_statement": "CREATE SEMANTIC VIEW test_view ...",
        }
        mock_builder_class.return_value = mock_builder_instance

        mock_config = MagicMock()
        service = SemanticViewGenerationService(mock_config)

        # Create config with description
        view_configs = [
            {
                "name": "test_view",
                "tables": ["orders", "customers"],
                "description": "Business analytics view for order insights",
            }
        ]

        config = GenerateConfig(
            views_to_generate=view_configs,
            target_database="TEST_DB",
            target_schema="TEST_SCHEMA",
            metadata_database="TEST_DB",
            metadata_schema="TEST_SCHEMA",
            execute=False,
        )

        service.execute(config)

        # Verify build_semantic_view was called with the description
        mock_builder_instance.build_semantic_view.assert_called_once()
        call_kwargs = mock_builder_instance.build_semantic_view.call_args[1]
        assert call_kwargs["description"] == "Business analytics view for order insights"

    @patch("snowflake_semantic_tools.services.generate_semantic_views.SemanticViewBuilder")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionManager")
    def test_empty_description_passed_to_builder(self, mock_conn_manager_class, mock_builder_class):
        """Verify execute() passes empty description without error."""
        mock_builder_instance = MagicMock()
        mock_builder_instance.metadata_database = "TEST_DB"
        mock_builder_instance.metadata_schema = "TEST_SCHEMA"
        mock_builder_instance.build_semantic_view.return_value = {
            "success": True,
            "sql_statement": "CREATE SEMANTIC VIEW test_view ...",
        }
        mock_builder_class.return_value = mock_builder_instance

        mock_config = MagicMock()
        service = SemanticViewGenerationService(mock_config)

        view_configs = [
            {
                "name": "test_view",
                "tables": ["orders"],
                "description": "",  # Empty description
            }
        ]

        config = GenerateConfig(
            views_to_generate=view_configs,
            target_database="TEST_DB",
            target_schema="TEST_SCHEMA",
            metadata_database="TEST_DB",
            metadata_schema="TEST_SCHEMA",
            execute=False,
        )

        service.execute(config)

        # Verify build_semantic_view was called with empty description
        mock_builder_instance.build_semantic_view.assert_called_once()
        call_kwargs = mock_builder_instance.build_semantic_view.call_args[1]
        assert call_kwargs["description"] == ""
