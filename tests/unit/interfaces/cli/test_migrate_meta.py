"""
Tests for the migrate-meta CLI command.

Tests the migration functionality from legacy meta.sst to config.meta.sst format.
"""

import tempfile
from pathlib import Path

import pytest
from ruamel.yaml import YAML

from snowflake_semantic_tools.interfaces.cli.commands.migrate_meta import (
    _migrate_node_meta,
    _migrate_yaml_file,
)


class TestMigrateNodeMeta:
    """Test suite for _migrate_node_meta function."""

    def test_migrate_model_from_meta_sst(self):
        """Test migrating model from meta.sst to config.meta.sst."""
        node = {
            "name": "test_model",
            "meta": {
                "sst": {
                    "cortex_searchable": True,
                    "primary_key": "id",
                }
            },
        }

        was_migrated, notes = _migrate_node_meta(node, "model 'test_model'")

        assert was_migrated is True
        assert len(notes) > 0
        assert "config" in node
        assert "meta" in node["config"]
        assert "sst" in node["config"]["meta"]
        assert node["config"]["meta"]["sst"]["cortex_searchable"] is True
        assert node["config"]["meta"]["sst"]["primary_key"] == "id"
        # Old location should be removed
        assert "meta" not in node

    def test_migrate_model_from_meta_genie(self):
        """Test migrating model from legacy meta.genie to config.meta.sst."""
        node = {
            "name": "test_model",
            "meta": {
                "genie": {
                    "cortex_searchable": True,
                }
            },
        }

        was_migrated, notes = _migrate_node_meta(node, "model 'test_model'")

        assert was_migrated is True
        assert "meta.genie" in notes[0]
        assert node["config"]["meta"]["sst"]["cortex_searchable"] is True
        assert "meta" not in node

    def test_no_migration_needed_already_new_format(self):
        """Test that node already in new format doesn't get migrated."""
        node = {
            "name": "test_model",
            "config": {
                "meta": {
                    "sst": {
                        "cortex_searchable": True,
                    }
                }
            },
        }

        was_migrated, notes = _migrate_node_meta(node, "model 'test_model'")

        assert was_migrated is False
        assert len(notes) == 0

    def test_no_migration_needed_no_sst(self):
        """Test that node without SST metadata doesn't get migrated."""
        node = {
            "name": "test_model",
            "description": "A model without SST metadata",
        }

        was_migrated, notes = _migrate_node_meta(node, "model 'test_model'")

        assert was_migrated is False

    def test_migrate_preserves_other_meta_fields(self):
        """Test that other fields in meta dict are preserved."""
        node = {
            "name": "test_model",
            "meta": {
                "sst": {
                    "cortex_searchable": True,
                },
                "custom_field": "should_remain",
            },
        }

        was_migrated, notes = _migrate_node_meta(node, "model 'test_model'")

        assert was_migrated is True
        # Custom field should remain
        assert node["meta"]["custom_field"] == "should_remain"
        # SST should be removed from old location
        assert "sst" not in node["meta"]

    def test_migrate_column_with_full_metadata(self):
        """Test migrating column with complete SST metadata."""
        node = {
            "name": "user_id",
            "meta": {
                "sst": {
                    "column_type": "dimension",
                    "data_type": "text",
                    "synonyms": ["customer_id"],
                    "sample_values": ["U001", "U002"],
                    "is_enum": False,
                }
            },
        }

        was_migrated, notes = _migrate_node_meta(node, "column 'orders.user_id'")

        assert was_migrated is True
        sst = node["config"]["meta"]["sst"]
        assert sst["column_type"] == "dimension"
        assert sst["data_type"] == "text"
        assert sst["synonyms"] == ["customer_id"]
        assert sst["sample_values"] == ["U001", "U002"]
        assert sst["is_enum"] is False


class TestMigrateYamlFile:
    """Test suite for _migrate_yaml_file function."""

    def test_migrate_yaml_file_with_legacy_format(self, tmp_path):
        """Test migrating a complete YAML file with legacy format."""
        yaml_content = """
version: 2

models:
  - name: orders
    description: "Orders table"
    meta:
      sst:
        cortex_searchable: true
        primary_key: id
    columns:
      - name: id
        description: "Order ID"
        meta:
          sst:
            column_type: dimension
            data_type: text
      - name: amount
        description: "Order amount"
        meta:
          sst:
            column_type: fact
            data_type: number
"""
        yaml_file = tmp_path / "test_model.yml"
        yaml_file.write_text(yaml_content)

        result = _migrate_yaml_file(yaml_file, dry_run=False)

        assert result["status"] == "migrated"
        assert result["models_migrated"] == 1
        assert result["columns_migrated"] == 2
        assert result["error"] is None

        # Verify the file was updated
        yaml = YAML()
        with open(yaml_file) as f:
            content = yaml.load(f)

        model = content["models"][0]
        assert "config" in model
        assert model["config"]["meta"]["sst"]["cortex_searchable"] is True
        # Legacy meta.sst should be removed
        assert "meta" not in model or "sst" not in model.get("meta", {})

    def test_migrate_yaml_file_dry_run(self, tmp_path):
        """Test dry run doesn't modify the file."""
        yaml_content = """
version: 2

models:
  - name: orders
    meta:
      sst:
        cortex_searchable: true
"""
        yaml_file = tmp_path / "test_model.yml"
        yaml_file.write_text(yaml_content)
        original_content = yaml_file.read_text()

        result = _migrate_yaml_file(yaml_file, dry_run=True)

        assert result["status"] == "would_migrate"
        assert result["models_migrated"] == 1
        # File should be unchanged
        assert yaml_file.read_text() == original_content

    def test_migrate_yaml_file_creates_backup(self, tmp_path):
        """Test that backup is created when requested."""
        yaml_content = """
version: 2

models:
  - name: orders
    meta:
      sst:
        cortex_searchable: true
"""
        yaml_file = tmp_path / "test_model.yml"
        yaml_file.write_text(yaml_content)

        result = _migrate_yaml_file(yaml_file, dry_run=False, backup=True)

        assert result["status"] == "migrated"
        backup_file = yaml_file.with_suffix(".yml.bak")
        assert backup_file.exists()

    def test_migrate_yaml_file_no_changes_needed(self, tmp_path):
        """Test file already in new format returns unchanged status."""
        yaml_content = """
version: 2

models:
  - name: orders
    config:
      meta:
        sst:
          cortex_searchable: true
"""
        yaml_file = tmp_path / "test_model.yml"
        yaml_file.write_text(yaml_content)

        result = _migrate_yaml_file(yaml_file, dry_run=False)

        assert result["status"] == "unchanged"
        assert result["models_migrated"] == 0
        assert result["columns_migrated"] == 0

    def test_migrate_yaml_file_handles_empty_models(self, tmp_path):
        """Test handling of file with no models."""
        yaml_content = """
version: 2
"""
        yaml_file = tmp_path / "empty.yml"
        yaml_file.write_text(yaml_content)

        result = _migrate_yaml_file(yaml_file, dry_run=False)

        assert result["status"] == "unchanged"

    def test_migrate_yaml_file_handles_invalid_yaml(self, tmp_path):
        """Test handling of invalid YAML."""
        yaml_file = tmp_path / "invalid.yml"
        yaml_file.write_text("invalid: yaml: content: [")

        result = _migrate_yaml_file(yaml_file, dry_run=False)

        assert result["status"] == "error"
        assert result["error"] is not None

