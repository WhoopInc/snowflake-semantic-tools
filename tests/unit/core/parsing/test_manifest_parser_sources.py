"""Tests for ManifestParser source caching."""

import json
import os
import tempfile

import pytest

from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser


@pytest.fixture
def manifest_with_sources(tmp_path):
    """Create a manifest.json with both models and sources."""
    manifest = {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json"},
        "nodes": {
            "model.jaffle_shop.customers": {
                "resource_type": "model",
                "name": "customers",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "alias": "customers",
                "relation_name": "ANALYTICS.PUBLIC.CUSTOMERS",
                "original_file_path": "models/customers.sql",
            }
        },
        "sources": {
            "source.jaffle_shop.raw.orders": {
                "resource_type": "source",
                "source_name": "raw",
                "name": "orders",
                "database": "RAW_DB",
                "schema": "PUBLIC",
                "original_file_path": "models/staging/_sources.yml",
            },
            "source.jaffle_shop.raw.customers": {
                "resource_type": "source",
                "source_name": "raw",
                "name": "customers",
                "database": "RAW_DB",
                "schema": "PUBLIC",
                "original_file_path": "models/staging/_sources.yml",
            },
            "source.jaffle_shop.stripe.payments": {
                "resource_type": "source",
                "source_name": "stripe",
                "name": "payments",
                "database": "RAW_DB",
                "schema": "STRIPE",
                "original_file_path": "models/staging/_stripe_sources.yml",
            },
        },
    }
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path


@pytest.fixture
def manifest_without_sources(tmp_path):
    """Create a manifest.json with models but no sources."""
    manifest = {
        "metadata": {},
        "nodes": {
            "model.jaffle_shop.customers": {
                "resource_type": "model",
                "name": "customers",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "alias": "customers",
                "original_file_path": "models/customers.sql",
            }
        },
    }
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path


class TestManifestParserSourceCaching:

    def test_source_locations_populated(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        assert parser.load()
        assert len(parser.source_locations) == 3

    def test_source_location_keys(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        parser.load()
        assert "raw.orders" in parser.source_locations
        assert "raw.customers" in parser.source_locations
        assert "stripe.payments" in parser.source_locations

    def test_source_location_values(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        parser.load()
        loc = parser.source_locations["raw.orders"]
        assert loc["database"] == "RAW_DB"
        assert loc["schema"] == "PUBLIC"
        assert loc["name"] == "orders"
        assert loc["source_name"] == "raw"
        assert loc["original_file_path"] == "models/staging/_sources.yml"

    def test_get_source_location(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        parser.load()
        loc = parser.get_source_location("stripe", "payments")
        assert loc is not None
        assert loc["database"] == "RAW_DB"
        assert loc["schema"] == "STRIPE"

    def test_get_source_location_not_found(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        parser.load()
        assert parser.get_source_location("nonexistent", "table") is None

    def test_get_all_sources(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        parser.load()
        all_sources = parser.get_all_sources()
        assert len(all_sources) == 3
        assert "raw.orders" in all_sources

    def test_no_sources_in_manifest(self, manifest_without_sources):
        parser = ManifestParser(manifest_without_sources)
        parser.load()
        assert len(parser.source_locations) == 0
        assert parser.get_all_sources() == {}

    def test_models_still_cached(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        parser.load()
        assert "customers" in parser.model_locations

    def test_source_with_empty_database_skipped(self, tmp_path):
        manifest = {
            "metadata": {},
            "nodes": {},
            "sources": {
                "source.proj.raw.bad_table": {
                    "source_name": "raw",
                    "name": "bad_table",
                    "database": "",
                    "schema": "PUBLIC",
                    "original_file_path": "models/_sources.yml",
                }
            },
        }
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))
        parser = ManifestParser(manifest_path)
        parser.load()
        assert len(parser.source_locations) == 0

    def test_database_schema_uppercased(self, manifest_with_sources):
        parser = ManifestParser(manifest_with_sources)
        parser.load()
        loc = parser.source_locations["raw.orders"]
        assert loc["database"] == "RAW_DB"
        assert loc["schema"] == "PUBLIC"
