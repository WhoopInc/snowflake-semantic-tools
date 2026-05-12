"""Tests for enrich_source() in MetadataEnricher."""

import copy
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.core.enrichment.metadata_enricher import MetadataEnricher
from snowflake_semantic_tools.core.enrichment.yaml_handler import YAMLHandler


@pytest.fixture
def mock_snowflake_client():
    client = MagicMock()
    client.metadata_manager.get_table_schema.return_value = [
        {"name": "ID", "column_name": "ID", "type": "NUMBER(38,0)"},
        {"name": "USER_ID", "column_name": "USER_ID", "type": "NUMBER(38,0)"},
        {"name": "AMOUNT", "column_name": "AMOUNT", "type": "NUMBER(10,2)"},
        {"name": "CREATED_AT", "column_name": "CREATED_AT", "type": "TIMESTAMP_NTZ(9)"},
    ]
    client.metadata_manager.get_sample_values_batch.return_value = {}
    return client


@pytest.fixture
def yaml_handler():
    return YAMLHandler()


@pytest.fixture
def enricher(mock_snowflake_client, yaml_handler):
    pk_validator = MagicMock()
    return MetadataEnricher(
        snowflake_client=mock_snowflake_client,
        yaml_handler=yaml_handler,
        primary_key_validator=pk_validator,
        default_database="ANALYTICS",
        default_schema="public",
    )


@pytest.fixture
def source_yaml_content():
    return {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "database": "RAW_DB",
                "schema": "PUBLIC",
                "tables": [
                    {
                        "name": "orders",
                        "description": "Raw order data",
                        "columns": [],
                    },
                ],
            },
        ],
    }


@pytest.fixture
def source_info():
    return {
        "source_name": "raw",
        "table_name": "orders",
        "database": "RAW_DB",
        "schema": "PUBLIC",
        "yaml_path": "/tmp/test_sources.yml",
    }


class TestEnrichSource:

    def test_enrich_source_success(self, enricher, source_yaml_content, source_info):
        with patch.object(enricher.yaml_handler, "read_yaml", return_value=copy.deepcopy(source_yaml_content)):
            with patch.object(enricher.yaml_handler, "write_yaml", return_value=True):
                result = enricher.enrich_source(
                    source_info,
                    components=["column-types", "data-types"],
                )

        assert result["status"] == "success"
        assert result["source"] == "raw.orders"
        assert result["columns_processed"] == 4

    def test_enrich_source_yaml_not_found(self, enricher, source_info):
        with patch.object(enricher.yaml_handler, "read_yaml", return_value=None):
            result = enricher.enrich_source(source_info, components=["column-types"])

        assert result["status"] == "error"
        assert "SST-E009" in result["error"]

    def test_enrich_source_table_not_in_yaml(self, enricher, source_info):
        yaml_content = {
            "version": 2,
            "sources": [
                {
                    "name": "raw",
                    "tables": [{"name": "different_table"}],
                },
            ],
        }
        with patch.object(enricher.yaml_handler, "read_yaml", return_value=yaml_content):
            result = enricher.enrich_source(source_info, components=["column-types"])

        assert result["status"] == "error"
        assert "not found in YAML" in result["error"]

    def test_enrich_source_table_not_in_snowflake(self, enricher, source_yaml_content, source_info):
        enricher.snowflake_client.metadata_manager.get_table_schema.return_value = None

        with patch.object(enricher.yaml_handler, "read_yaml", return_value=copy.deepcopy(source_yaml_content)):
            result = enricher.enrich_source(
                source_info,
                components=["column-types", "data-types"],
            )

        assert result["status"] == "error"
        assert "SST-E007" in result["error"]

    def test_enrich_source_write_failure(self, enricher, source_yaml_content, source_info):
        with patch.object(enricher.yaml_handler, "read_yaml", return_value=copy.deepcopy(source_yaml_content)):
            with patch.object(enricher.yaml_handler, "write_yaml", side_effect=IOError("Permission denied")):
                result = enricher.enrich_source(
                    source_info,
                    components=["column-types", "data-types"],
                )

        assert result["status"] == "error"
        assert "SST-E009" in result["error"]

    def test_enrich_source_preserves_description(self, enricher, source_info):
        yaml_content = {
            "version": 2,
            "sources": [
                {
                    "name": "raw",
                    "tables": [
                        {
                            "name": "orders",
                            "description": "My custom description",
                            "columns": [],
                        },
                    ],
                },
            ],
        }

        written_content = {}

        def capture_write(content, path):
            written_content.update(content)
            return True

        with patch.object(enricher.yaml_handler, "read_yaml", return_value=copy.deepcopy(yaml_content)):
            with patch.object(enricher.yaml_handler, "write_yaml", side_effect=capture_write):
                result = enricher.enrich_source(
                    source_info,
                    components=["column-types", "data-types"],
                )

        assert result["status"] == "success"
        table = written_content["sources"][0]["tables"][0]
        assert table["description"] == "My custom description"

    def test_enrich_source_synonym_only_mode(self, enricher, source_yaml_content, source_info):
        enricher.synonym_generator = MagicMock()
        enricher.synonym_generator.generate_table_synonyms.return_value = ["purchases", "transactions"]

        with patch.object(enricher.yaml_handler, "read_yaml", return_value=copy.deepcopy(source_yaml_content)):
            with patch.object(enricher.yaml_handler, "write_yaml", return_value=True):
                result = enricher.enrich_source(
                    source_info,
                    components=["table-synonyms"],
                )

        assert result["status"] == "success"
        enricher.snowflake_client.metadata_manager.get_table_schema.assert_not_called()

    def test_enrich_source_default_components(self, enricher, source_yaml_content, source_info):
        """Test enrichment with components=None (default: all standard components)."""
        with patch.object(enricher.yaml_handler, "read_yaml", return_value=copy.deepcopy(source_yaml_content)):
            with patch.object(enricher.yaml_handler, "write_yaml", return_value=True):
                result = enricher.enrich_source(
                    source_info,
                    components=None,
                )

        assert result["status"] == "success"
        assert result["columns_processed"] == 4
        enricher.snowflake_client.metadata_manager.get_table_schema.assert_called_once()

    def test_enrich_source_preserves_existing_column_sst(self, enricher, source_info):
        """Test that existing column_type and data_type are preserved (not overwritten)."""
        yaml_content = {
            "version": 2,
            "sources": [
                {
                    "name": "raw",
                    "tables": [
                        {
                            "name": "orders",
                            "description": "Raw orders",
                            "columns": [
                                {
                                    "name": "id",
                                    "description": "Order ID",
                                    "config": {
                                        "meta": {
                                            "sst": {
                                                "column_type": "dimension",
                                                "data_type": "NUMBER",
                                                "synonyms": ["order_identifier"],
                                            }
                                        }
                                    },
                                },
                            ],
                        },
                    ],
                },
            ],
        }

        written_content = {}

        def capture_write(content, path):
            written_content.update(content)
            return True

        with patch.object(enricher.yaml_handler, "read_yaml", return_value=copy.deepcopy(yaml_content)):
            with patch.object(enricher.yaml_handler, "write_yaml", side_effect=capture_write):
                result = enricher.enrich_source(
                    source_info,
                    components=["column-types", "data-types"],
                )

        assert result["status"] == "success"
        columns = written_content["sources"][0]["tables"][0]["columns"]
        id_col = next(c for c in columns if c["name"] == "id")
        sst = id_col["config"]["meta"]["sst"]
        assert sst["column_type"] == "dimension"
        assert sst["data_type"] == "NUMBER"
        assert sst["synonyms"] == ["order_identifier"]

    def test_enrich_source_missing_key_in_source_info(self, enricher):
        """Test error handling when source_info is missing required keys."""
        bad_info = {"source_name": "raw"}

        result = enricher.enrich_source(bad_info, components=["column-types"])

        assert result["status"] == "error"
        assert "Missing required field" in result["error"] or "KeyError" in result["error"]
