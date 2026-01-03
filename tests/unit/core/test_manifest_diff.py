"""
Tests for ManifestDiff dataclass and ManifestParser.compare_to() method.
"""

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import (
    ManifestDiff,
    ManifestParser,
)


class TestManifestDiff:
    """Tests for ManifestDiff dataclass."""

    def test_empty_diff(self):
        """Test empty ManifestDiff."""
        diff = ManifestDiff()
        assert diff.added == []
        assert diff.removed == []
        assert diff.modified == []
        assert diff.unchanged == []
        assert diff.changed == []
        assert diff.total_changes == 0
        assert diff.summary() == "no changes"

    def test_changed_property(self):
        """Test that changed returns added + modified."""
        diff = ManifestDiff(
            added=["model_a", "model_b"],
            modified=["model_c"],
            unchanged=["model_d"],
        )
        assert diff.changed == ["model_a", "model_b", "model_c"]
        assert diff.total_changes == 3

    def test_summary_with_changes(self):
        """Test summary with various changes."""
        diff = ManifestDiff(
            added=["a"],
            modified=["b", "c"],
            removed=["d"],
            unchanged=["e", "f"],
        )
        summary = diff.summary()
        assert "1 added" in summary
        assert "2 modified" in summary
        assert "1 removed" in summary
        assert "2 unchanged" in summary


class TestManifestParserComparison:
    """Tests for ManifestParser.compare_to() method."""

    def create_manifest(self, tmp_path: Path, name: str, models: dict) -> ManifestParser:
        """Helper to create a manifest file and parser."""
        manifest_dir = tmp_path / name
        manifest_dir.mkdir(exist_ok=True)
        manifest_path = manifest_dir / "manifest.json"

        # Build nodes from models dict
        # models = {"model_name": "checksum", ...}
        nodes = {}
        for model_name, checksum in models.items():
            node_id = f"model.project.{model_name}"
            nodes[node_id] = {
                "resource_type": "model",
                "name": model_name,
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "checksum": {"checksum": checksum},
            }

        manifest_data = {
            "nodes": nodes,
            "metadata": {
                "dbt_version": "1.7.0",
                "target_name": "prod",
            },
        }

        manifest_path.write_text(json.dumps(manifest_data))

        parser = ManifestParser(manifest_path)
        parser.load()
        return parser

    def test_compare_identical_manifests(self, tmp_path):
        """Test comparing identical manifests."""
        models = {"customers": "abc123", "orders": "def456"}

        parser1 = self.create_manifest(tmp_path, "manifest1", models)
        parser2 = self.create_manifest(tmp_path, "manifest2", models)

        diff = parser1.compare_to(parser2)

        assert diff.added == []
        assert diff.removed == []
        assert diff.modified == []
        assert sorted(diff.unchanged) == ["customers", "orders"]

    def test_compare_with_added_models(self, tmp_path):
        """Test comparing when current has new models."""
        current_models = {"customers": "abc123", "orders": "def456", "products": "ghi789"}
        reference_models = {"customers": "abc123", "orders": "def456"}

        current = self.create_manifest(tmp_path, "current", current_models)
        reference = self.create_manifest(tmp_path, "reference", reference_models)

        diff = current.compare_to(reference)

        assert diff.added == ["products"]
        assert diff.removed == []
        assert diff.modified == []

    def test_compare_with_removed_models(self, tmp_path):
        """Test comparing when reference has models that current doesn't."""
        current_models = {"customers": "abc123"}
        reference_models = {"customers": "abc123", "orders": "def456"}

        current = self.create_manifest(tmp_path, "current", current_models)
        reference = self.create_manifest(tmp_path, "reference", reference_models)

        diff = current.compare_to(reference)

        assert diff.added == []
        assert diff.removed == ["orders"]
        assert diff.modified == []

    def test_compare_with_modified_models(self, tmp_path):
        """Test comparing when checksums differ."""
        current_models = {"customers": "new_checksum", "orders": "def456"}
        reference_models = {"customers": "old_checksum", "orders": "def456"}

        current = self.create_manifest(tmp_path, "current", current_models)
        reference = self.create_manifest(tmp_path, "reference", reference_models)

        diff = current.compare_to(reference)

        assert diff.added == []
        assert diff.removed == []
        assert diff.modified == ["customers"]
        assert diff.unchanged == ["orders"]

    def test_compare_complex_diff(self, tmp_path):
        """Test complex comparison with all change types."""
        current_models = {
            "unchanged_model": "same123",
            "modified_model": "new456",
            "new_model": "brand789",
        }
        reference_models = {
            "unchanged_model": "same123",
            "modified_model": "old456",
            "deleted_model": "gone000",
        }

        current = self.create_manifest(tmp_path, "current", current_models)
        reference = self.create_manifest(tmp_path, "reference", reference_models)

        diff = current.compare_to(reference)

        assert diff.added == ["new_model"]
        assert diff.removed == ["deleted_model"]
        assert diff.modified == ["modified_model"]
        assert diff.unchanged == ["unchanged_model"]
        assert diff.total_changes == 2  # added + modified

    def test_compare_empty_manifests(self, tmp_path):
        """Test comparing empty manifests."""
        current = self.create_manifest(tmp_path, "current", {})
        reference = self.create_manifest(tmp_path, "reference", {})

        diff = current.compare_to(reference)

        assert diff.added == []
        assert diff.removed == []
        assert diff.modified == []
        assert diff.unchanged == []

    def test_compare_unloaded_manifests(self, tmp_path):
        """Test comparing when manifests aren't loaded."""
        parser1 = ManifestParser()
        parser2 = ManifestParser()

        diff = parser1.compare_to(parser2)

        # Should return empty diff without errors
        assert diff.added == []
        assert diff.removed == []
        assert diff.modified == []
        assert diff.unchanged == []


class TestGetModelsForTables:
    """Tests for ManifestParser.get_models_for_tables() method."""

    def test_get_models_simple(self, tmp_path):
        """Test getting models for simple table names."""
        manifest_path = tmp_path / "manifest.json"
        manifest_data = {
            "nodes": {
                "model.project.customers": {
                    "resource_type": "model",
                    "name": "customers",
                    "database": "ANALYTICS",
                    "schema": "PUBLIC",
                },
                "model.project.orders": {
                    "resource_type": "model",
                    "name": "orders",
                    "database": "ANALYTICS",
                    "schema": "PUBLIC",
                },
            },
        }
        manifest_path.write_text(json.dumps(manifest_data))

        parser = ManifestParser(manifest_path)
        parser.load()

        models = parser.get_models_for_tables(["customers", "orders"])

        assert "customers" in models
        assert "orders" in models

    def test_get_models_qualified_names(self, tmp_path):
        """Test getting models for fully qualified table names."""
        manifest_path = tmp_path / "manifest.json"
        manifest_data = {
            "nodes": {
                "model.project.customers": {
                    "resource_type": "model",
                    "name": "customers",
                    "database": "ANALYTICS",
                    "schema": "PUBLIC",
                },
            },
        }
        manifest_path.write_text(json.dumps(manifest_data))

        parser = ManifestParser(manifest_path)
        parser.load()

        # Should extract table name from fully qualified reference
        models = parser.get_models_for_tables(["ANALYTICS.PUBLIC.CUSTOMERS"])

        assert "customers" in models

    def test_get_models_case_insensitive(self, tmp_path):
        """Test that model matching is case-insensitive."""
        manifest_path = tmp_path / "manifest.json"
        manifest_data = {
            "nodes": {
                "model.project.customers": {
                    "resource_type": "model",
                    "name": "customers",
                    "database": "ANALYTICS",
                    "schema": "PUBLIC",
                },
            },
        }
        manifest_path.write_text(json.dumps(manifest_data))

        parser = ManifestParser(manifest_path)
        parser.load()

        models = parser.get_models_for_tables(["CUSTOMERS"])

        assert "customers" in models
