"""Tests for source discovery in MetadataEnrichmentService."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.services.enrich_metadata import EnrichmentConfig, MetadataEnrichmentService


@pytest.fixture
def manifest_with_sources(tmp_path):
    manifest = {
        "metadata": {},
        "nodes": {},
        "sources": {
            "source.jaffle_shop.raw.orders": {
                "source_name": "raw",
                "name": "orders",
                "database": "RAW_DB",
                "schema": "PUBLIC",
                "original_file_path": "models/staging/_sources.yml",
            },
            "source.jaffle_shop.raw.customers": {
                "source_name": "raw",
                "name": "customers",
                "database": "RAW_DB",
                "schema": "PUBLIC",
                "original_file_path": "models/staging/_sources.yml",
            },
        },
    }
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path


class TestDiscoverSourcesFromManifest:

    def test_discover_all_sources(self, manifest_with_sources, tmp_path):
        config = EnrichmentConfig(sources_only=True)
        service = MetadataEnrichmentService(config)

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        service.manifest_parser = ManifestParser(manifest_with_sources)
        service.manifest_parser.load()

        sources = service._discover_sources()
        assert len(sources) == 2
        names = {f"{s['source_name']}.{s['table_name']}" for s in sources}
        assert "raw.orders" in names
        assert "raw.customers" in names

    def test_discover_specific_source(self, manifest_with_sources):
        config = EnrichmentConfig(source_names=["raw.orders"])
        service = MetadataEnrichmentService(config)

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        service.manifest_parser = ManifestParser(manifest_with_sources)
        service.manifest_parser.load()

        sources = service._discover_sources()
        assert len(sources) == 1
        assert sources[0]["table_name"] == "orders"
        assert sources[0]["database"] == "RAW_DB"

    def test_discover_specific_source_not_found(self, manifest_with_sources):
        config = EnrichmentConfig(source_names=["raw.nonexistent"])
        service = MetadataEnrichmentService(config)

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        service.manifest_parser = ManifestParser(manifest_with_sources)
        service.manifest_parser.load()

        sources = service._discover_sources()
        assert len(sources) == 0

    def test_discover_no_manifest(self):
        config = EnrichmentConfig(source_names=["raw.orders"])
        service = MetadataEnrichmentService(config)
        service.manifest_parser = None

        sources = service._discover_sources()
        assert len(sources) == 0


class TestDiscoverSourcesFromYaml:

    def test_discover_from_yaml_files(self, tmp_path):
        models_dir = tmp_path / "models" / "staging"
        models_dir.mkdir(parents=True)

        source_yaml = models_dir / "_sources.yml"
        source_yaml.write_text(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    database: RAW_DB\n"
            "    schema: PUBLIC\n"
            "    tables:\n"
            "      - name: orders\n"
            "      - name: customers\n"
        )

        config = EnrichmentConfig(target_path=str(tmp_path / "models"), sources_only=True)
        service = MetadataEnrichmentService(config)
        service.manifest_parser = MagicMock()
        service.manifest_parser.source_locations = {}

        sources = service._discover_sources()
        assert len(sources) == 2

    def test_discover_from_yaml_no_database(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir(parents=True)

        source_yaml = models_dir / "_sources.yml"
        source_yaml.write_text("version: 2\n" "sources:\n" "  - name: raw\n" "    tables:\n" "      - name: orders\n")

        config = EnrichmentConfig(target_path=str(models_dir), sources_only=True)
        service = MetadataEnrichmentService(config)
        service.manifest_parser = MagicMock()
        service.manifest_parser.source_locations = {}

        sources = service._discover_sources()
        assert len(sources) == 0


class TestEnrichmentConfigSourceFlags:

    def test_sources_only_valid(self):
        config = EnrichmentConfig(sources_only=True)
        assert config.sources_only is True

    def test_source_names_valid(self):
        config = EnrichmentConfig(source_names=["raw.orders"])
        assert config.source_names == ["raw.orders"]

    def test_include_sources_with_path(self):
        config = EnrichmentConfig(target_path="models/", include_sources=True)
        assert config.include_sources is True

    def test_empty_config_raises(self):
        with pytest.raises(ValueError):
            EnrichmentConfig()


class TestDiscoverSourcesYamlFallbackWithDefaults:

    def test_discover_uses_config_database_schema(self, tmp_path):
        """When YAML source has no database/schema but config provides defaults."""
        models_dir = tmp_path / "models"
        models_dir.mkdir(parents=True)

        source_yaml = models_dir / "_sources.yml"
        source_yaml.write_text(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    tables:\n"
            "      - name: orders\n"
            "      - name: customers\n"
        )

        config = EnrichmentConfig(
            target_path=str(models_dir),
            sources_only=True,
            database="MY_DB",
            schema="MY_SCHEMA",
        )
        service = MetadataEnrichmentService(config)
        service.manifest_parser = MagicMock()
        service.manifest_parser.source_locations = {}

        sources = service._discover_sources()
        assert len(sources) == 2
        assert sources[0]["database"] == "MY_DB"
        assert sources[0]["schema"] == "MY_SCHEMA"

    def test_discover_skips_tables_without_name(self, tmp_path):
        """Tables without a name key are skipped."""
        models_dir = tmp_path / "models"
        models_dir.mkdir(parents=True)

        source_yaml = models_dir / "_sources.yml"
        source_yaml.write_text(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    database: DB\n"
            "    schema: SCH\n"
            "    tables:\n"
            "      - name: valid_table\n"
            "      - description: orphan with no name\n"
        )

        config = EnrichmentConfig(target_path=str(models_dir), sources_only=True)
        service = MetadataEnrichmentService(config)
        service.manifest_parser = MagicMock()
        service.manifest_parser.source_locations = {}

        sources = service._discover_sources()
        assert len(sources) == 1
        assert sources[0]["table_name"] == "valid_table"


class TestEnrichServiceSourceIntegration:
    """Integration tests for the full enrich() loop with sources."""

    def test_enrich_with_sources_only(self, tmp_path):
        """Full enrich() loop processes sources correctly."""
        manifest = {
            "metadata": {},
            "nodes": {},
            "sources": {
                "source.proj.raw.orders": {
                    "source_name": "raw",
                    "name": "orders",
                    "database": "DB",
                    "schema": "SCH",
                    "original_file_path": str(tmp_path / "_sources.yml"),
                }
            },
        }
        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text(json.dumps(manifest))

        source_yaml = tmp_path / "_sources.yml"
        source_yaml.write_text(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    database: DB\n"
            "    schema: SCH\n"
            "    tables:\n"
            "      - name: orders\n"
            "        columns: []\n"
        )

        config = EnrichmentConfig(
            sources_only=True,
            manifest_path=manifest_path,
        )
        service = MetadataEnrichmentService(config)

        mock_client = MagicMock()
        mock_client.execute_query.return_value = MagicMock(empty=False, iloc=MagicMock(__getitem__=lambda s, x: "USER"))
        mock_client.metadata_manager.get_table_schema.return_value = [
            {"name": "ID", "column_name": "ID", "type": "NUMBER(38,0)"},
        ]
        mock_client.metadata_manager.get_sample_values_batch.return_value = {}
        service.snowflake_client = mock_client

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        service.manifest_parser = ManifestParser(manifest_path)
        service.manifest_parser.load()

        from snowflake_semantic_tools.core.enrichment import MetadataEnricher, PrimaryKeyValidator, YAMLHandler

        yaml_handler = YAMLHandler()
        pk_validator = MagicMock()
        service.enricher = MetadataEnricher(
            snowflake_client=mock_client,
            yaml_handler=yaml_handler,
            primary_key_validator=pk_validator,
            default_database="DB",
            default_schema="SCH",
        )

        result = service.enrich()
        assert result.status == "complete"
        assert result.processed == 1
        assert result.total == 1

    def test_enrich_fail_fast_stops_on_source_error(self, tmp_path):
        """fail_fast=True stops processing after first source failure."""
        manifest = {
            "metadata": {},
            "nodes": {},
            "sources": {
                "source.proj.raw.orders": {
                    "source_name": "raw",
                    "name": "orders",
                    "database": "DB",
                    "schema": "SCH",
                    "original_file_path": str(tmp_path / "_sources.yml"),
                },
                "source.proj.raw.customers": {
                    "source_name": "raw",
                    "name": "customers",
                    "database": "DB",
                    "schema": "SCH",
                    "original_file_path": str(tmp_path / "_sources.yml"),
                },
            },
        }
        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text(json.dumps(manifest))

        source_yaml = tmp_path / "_sources.yml"
        source_yaml.write_text(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    database: DB\n"
            "    schema: SCH\n"
            "    tables:\n"
            "      - name: orders\n"
            "        columns: []\n"
            "      - name: customers\n"
            "        columns: []\n"
        )

        config = EnrichmentConfig(
            sources_only=True,
            manifest_path=manifest_path,
            fail_fast=True,
        )
        service = MetadataEnrichmentService(config)

        mock_client = MagicMock()
        mock_client.execute_query.return_value = MagicMock(empty=False, iloc=MagicMock(__getitem__=lambda s, x: "USER"))
        mock_client.metadata_manager.get_table_schema.return_value = None
        mock_client.metadata_manager.get_sample_values_batch.return_value = {}
        service.snowflake_client = mock_client

        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        service.manifest_parser = ManifestParser(manifest_path)
        service.manifest_parser.load()

        from snowflake_semantic_tools.core.enrichment import MetadataEnricher, PrimaryKeyValidator, YAMLHandler

        yaml_handler = YAMLHandler()
        pk_validator = MagicMock()
        service.enricher = MetadataEnricher(
            snowflake_client=mock_client,
            yaml_handler=yaml_handler,
            primary_key_validator=pk_validator,
            default_database="DB",
            default_schema="SCH",
        )

        result = service.enrich()
        assert result.status == "failed"
        assert len(result.errors) == 1
        assert result.total == 2
