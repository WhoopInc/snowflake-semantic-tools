"""
Unit tests for MetadataEnricher component decoupling.

Tests that enrichment flags only enrich their specific components.
"""

from unittest.mock import MagicMock, Mock

import pytest

from snowflake_semantic_tools.core.enrichment.metadata_enricher import MetadataEnricher


class TestBatchFetchSamples:
    """Tests for _batch_fetch_samples component checking."""

    @pytest.fixture
    def enricher(self):
        """Create an enricher instance with mocked dependencies."""
        mock_client = Mock()
        mock_client.metadata_manager = Mock()
        mock_client.metadata_manager.get_sample_values_batch.return_value = {"ID": [1, 2]}

        mock_yaml_handler = Mock()
        mock_pk_validator = Mock()

        enricher = MetadataEnricher(
            snowflake_client=mock_client,
            yaml_handler=mock_yaml_handler,
            primary_key_validator=mock_pk_validator,
            default_database="TEST_DB",
            default_schema="test_schema",
        )
        enricher._is_pii_protected_column = Mock(return_value=False)
        return enricher

    def test_batch_fetch_skips_when_no_sample_components(self, enricher):
        """_batch_fetch_samples returns {} when neither sample-values nor detect-enums requested."""
        result = enricher._batch_fetch_samples(
            table_columns=[{"name": "id"}],
            existing_lookup={},
            model_name="test",
            schema_name="public",
            database_name="db",
            components=["column-types", "data-types"],  # No sample components
        )
        assert result == {}
        # Should NOT call Snowflake
        enricher.snowflake_client.metadata_manager.get_sample_values_batch.assert_not_called()

    def test_batch_fetch_runs_for_sample_values(self, enricher):
        """_batch_fetch_samples runs when sample-values is requested."""
        result = enricher._batch_fetch_samples(
            table_columns=[{"name": "ID"}],
            existing_lookup={},
            model_name="test",
            schema_name="public",
            database_name="db",
            components=["sample-values"],
        )
        assert result == {"ID": [1, 2]}
        enricher.snowflake_client.metadata_manager.get_sample_values_batch.assert_called_once()

    def test_batch_fetch_runs_for_detect_enums(self, enricher):
        """_batch_fetch_samples runs when detect-enums is requested (needs sample data)."""
        result = enricher._batch_fetch_samples(
            table_columns=[{"name": "ID"}],
            existing_lookup={},
            model_name="test",
            schema_name="public",
            database_name="db",
            components=["detect-enums"],  # detect-enums also needs samples
        )
        assert result == {"ID": [1, 2]}
        enricher.snowflake_client.metadata_manager.get_sample_values_batch.assert_called_once()

    def test_batch_fetch_runs_when_no_components(self, enricher):
        """_batch_fetch_samples runs when components=None (default behavior)."""
        result = enricher._batch_fetch_samples(
            table_columns=[{"name": "ID"}],
            existing_lookup={},
            model_name="test",
            schema_name="public",
            database_name="db",
            components=None,  # None = all components
        )
        assert result == {"ID": [1, 2]}
        enricher.snowflake_client.metadata_manager.get_sample_values_batch.assert_called_once()


class TestEnrichColumnTypes:
    """Tests for _enrich_column_types component checking."""

    @pytest.fixture
    def enricher(self):
        """Create an enricher instance with mocked dependencies."""
        mock_client = Mock()
        mock_yaml_handler = Mock()
        mock_pk_validator = Mock()

        return MetadataEnricher(
            snowflake_client=mock_client,
            yaml_handler=mock_yaml_handler,
            primary_key_validator=mock_pk_validator,
            default_database="TEST_DB",
            default_schema="test_schema",
        )

    def test_enrich_column_types_only_data_types(self, enricher):
        """Only data_type is set when only data-types component requested."""
        column_sst = {}
        enricher._enrich_column_types(column_sst, "test_col", "VARCHAR(100)", components=["data-types"])
        assert "data_type" in column_sst
        assert column_sst["data_type"] == "TEXT"  # Snowflake type mapping
        # column_type should not be set
        assert "column_type" not in column_sst or not column_sst.get("column_type")

    def test_enrich_column_types_only_column_types(self, enricher):
        """Only column_type is set when only column-types component requested."""
        column_sst = {}
        enricher._enrich_column_types(column_sst, "test_col", "VARCHAR(100)", components=["column-types"])
        assert "column_type" in column_sst
        assert column_sst["column_type"] == "dimension"
        # data_type should not be set
        assert "data_type" not in column_sst or not column_sst.get("data_type")

    def test_enrich_column_types_both(self, enricher):
        """Both types set when both components requested."""
        column_sst = {}
        enricher._enrich_column_types(column_sst, "test_col", "VARCHAR(100)", components=["data-types", "column-types"])
        assert column_sst["data_type"] == "TEXT"  # Snowflake type mapping
        assert column_sst["column_type"] == "dimension"

    def test_enrich_column_types_no_components_enriches_all(self, enricher):
        """No components (None) means enrich all (backward compatible)."""
        column_sst = {}
        enricher._enrich_column_types(column_sst, "test_col", "VARCHAR(100)", components=None)
        assert column_sst["data_type"] == "TEXT"  # Snowflake type mapping
        assert column_sst["column_type"] == "dimension"

    def test_enrich_column_types_preserves_existing(self, enricher):
        """Existing values are preserved even when component is requested."""
        column_sst = {"data_type": "text", "column_type": "fact"}
        enricher._enrich_column_types(column_sst, "test_col", "VARCHAR(100)", components=["data-types", "column-types"])
        # Should preserve existing values
        assert column_sst["data_type"] == "text"
        assert column_sst["column_type"] == "fact"


class TestEnrichSampleValues:
    """Tests for _enrich_sample_values component checking."""

    @pytest.fixture
    def enricher(self):
        """Create an enricher instance with mocked dependencies."""
        mock_client = Mock()
        mock_yaml_handler = Mock()
        mock_pk_validator = Mock()

        enricher = MetadataEnricher(
            snowflake_client=mock_client,
            yaml_handler=mock_yaml_handler,
            primary_key_validator=mock_pk_validator,
            default_database="TEST_DB",
            default_schema="test_schema",
        )
        enricher._determine_enum_status = Mock(return_value=(["a", "b"], True))
        return enricher

    def test_enrich_sample_values_only(self, enricher):
        """Only sample_values set when only sample-values component requested."""
        column_sst = {}
        enricher._enrich_sample_values(
            column_sst, "test_col", {"TEST_COL": ["a", "b"]}, "model", "schema", "db", components=["sample-values"]
        )
        assert "sample_values" in column_sst
        assert column_sst["sample_values"] == ["a", "b"]
        # is_enum should NOT be set
        assert "is_enum" not in column_sst

    def test_enrich_detect_enums_only(self, enricher):
        """Only is_enum set when only detect-enums component requested."""
        column_sst = {}
        enricher._enrich_sample_values(
            column_sst, "test_col", {"TEST_COL": ["a", "b"]}, "model", "schema", "db", components=["detect-enums"]
        )
        assert "is_enum" in column_sst
        assert column_sst["is_enum"] is True
        # sample_values should NOT be set
        assert "sample_values" not in column_sst

    def test_enrich_both_sample_and_enum(self, enricher):
        """Both set when both components requested."""
        column_sst = {}
        enricher._enrich_sample_values(
            column_sst,
            "test_col",
            {"TEST_COL": ["a", "b"]},
            "model",
            "schema",
            "db",
            components=["sample-values", "detect-enums"],
        )
        assert column_sst["sample_values"] == ["a", "b"]
        assert column_sst["is_enum"] is True

    def test_enrich_no_components_enriches_all(self, enricher):
        """No components (None) means enrich all (backward compatible)."""
        column_sst = {}
        enricher._enrich_sample_values(
            column_sst, "test_col", {"TEST_COL": ["a", "b"]}, "model", "schema", "db", components=None
        )
        assert column_sst["sample_values"] == ["a", "b"]
        assert column_sst["is_enum"] is True


class TestProcessSingleColumn:
    """Tests for _process_single_column component passing."""

    @pytest.fixture
    def enricher(self):
        """Create an enricher instance with mocked dependencies."""
        mock_client = Mock()
        mock_yaml_handler = Mock()
        mock_yaml_handler.ensure_column_sst_structure.side_effect = lambda x: {
            **x,
            "config": {"meta": {"sst": x.get("config", {}).get("meta", {}).get("sst", {})}},
        }
        mock_yaml_handler._order_column_sst_keys.side_effect = lambda x: x
        mock_pk_validator = Mock()

        enricher = MetadataEnricher(
            snowflake_client=mock_client,
            yaml_handler=mock_yaml_handler,
            primary_key_validator=mock_pk_validator,
            default_database="TEST_DB",
            default_schema="test_schema",
        )
        enricher._is_pii_protected_column = Mock(return_value=False)
        enricher._enrich_column_types = Mock()
        enricher._enrich_sample_values = Mock()
        return enricher

    def test_column_types_only_skips_samples(self, enricher):
        """When only column-types requested, sample enrichment is skipped."""
        enricher._process_single_column(
            table_col={"name": "ID", "type": "NUMBER"},
            existing_lookup={},
            batch_samples={},
            model_name="test",
            schema_name="public",
            database_name="db",
            idx=1,
            total=1,
            components=["column-types"],
        )

        # Column types should be called with components
        enricher._enrich_column_types.assert_called_once()

        # Sample values should NOT be called (no sample-values or detect-enums)
        enricher._enrich_sample_values.assert_not_called()

    def test_data_types_only_skips_samples(self, enricher):
        """When only data-types requested, sample enrichment is skipped."""
        enricher._process_single_column(
            table_col={"name": "ID", "type": "NUMBER"},
            existing_lookup={},
            batch_samples={},
            model_name="test",
            schema_name="public",
            database_name="db",
            idx=1,
            total=1,
            components=["data-types"],
        )

        # Column types should be called
        enricher._enrich_column_types.assert_called_once()

        # Sample values should NOT be called
        enricher._enrich_sample_values.assert_not_called()

    def test_sample_values_triggers_sample_enrichment(self, enricher):
        """When sample-values requested, sample enrichment is called."""
        enricher._process_single_column(
            table_col={"name": "ID", "type": "NUMBER"},
            existing_lookup={},
            batch_samples={},
            model_name="test",
            schema_name="public",
            database_name="db",
            idx=1,
            total=1,
            components=["sample-values"],
        )

        # Sample values SHOULD be called
        enricher._enrich_sample_values.assert_called_once()

    def test_detect_enums_triggers_sample_enrichment(self, enricher):
        """When detect-enums requested, sample enrichment is called."""
        enricher._process_single_column(
            table_col={"name": "ID", "type": "NUMBER"},
            existing_lookup={},
            batch_samples={},
            model_name="test",
            schema_name="public",
            database_name="db",
            idx=1,
            total=1,
            components=["detect-enums"],
        )

        # Sample values SHOULD be called (detect-enums needs sample data)
        enricher._enrich_sample_values.assert_called_once()

    def test_no_components_triggers_all(self, enricher):
        """When components=None, all enrichment is triggered."""
        enricher._process_single_column(
            table_col={"name": "ID", "type": "NUMBER"},
            existing_lookup={},
            batch_samples={},
            model_name="test",
            schema_name="public",
            database_name="db",
            idx=1,
            total=1,
            components=None,
        )

        # Both should be called
        enricher._enrich_column_types.assert_called_once()
        enricher._enrich_sample_values.assert_called_once()
