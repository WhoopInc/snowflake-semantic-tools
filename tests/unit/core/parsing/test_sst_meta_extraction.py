"""
Tests for SST metadata extraction with dbt Fusion compatibility.

Tests the get_sst_meta utility function that reads SST metadata from both:
- New format: config.meta.sst (dbt Fusion compatible)
- Legacy format: meta.sst (deprecated but supported)
"""

import pytest

from snowflake_semantic_tools.core.parsing.parsers.data_extractors import (
    clear_deprecation_warnings,
    get_sst_meta,
)


class TestGetSstMeta:
    """Test suite for get_sst_meta function."""

    def setup_method(self):
        """Clear deprecation warnings before each test."""
        clear_deprecation_warnings()

    def test_new_format_config_meta_sst(self):
        """Test reading from new config.meta.sst location (dbt Fusion compatible)."""
        node = {
            "name": "test_model",
            "config": {
                "meta": {
                    "sst": {
                        "cortex_searchable": True,
                        "primary_key": "id",
                    }
                }
            },
        }

        result = get_sst_meta(node, node_type="model", node_name="test_model")

        assert result["cortex_searchable"] is True
        assert result["primary_key"] == "id"

    def test_legacy_format_meta_sst(self):
        """Test fallback to legacy meta.sst location."""
        node = {
            "name": "test_model",
            "meta": {
                "sst": {
                    "cortex_searchable": True,
                    "primary_key": "id",
                }
            },
        }

        result = get_sst_meta(node, node_type="model", node_name="test_model")

        assert result["cortex_searchable"] is True
        assert result["primary_key"] == "id"

    def test_new_format_takes_priority(self):
        """Test that config.meta.sst takes priority over meta.sst."""
        node = {
            "name": "test_model",
            "config": {
                "meta": {
                    "sst": {
                        "cortex_searchable": True,
                        "primary_key": "new_key",
                    }
                }
            },
            "meta": {
                "sst": {
                    "cortex_searchable": False,
                    "primary_key": "old_key",
                }
            },
        }

        result = get_sst_meta(node, node_type="model", node_name="test_model")

        # New format should take priority
        assert result["cortex_searchable"] is True
        assert result["primary_key"] == "new_key"

    def test_empty_node_returns_empty_dict(self):
        """Test that empty node returns empty dict."""
        result = get_sst_meta({}, node_type="model", node_name="empty")
        assert result == {}

    def test_missing_meta_returns_empty_dict(self):
        """Test that node without any meta returns empty dict."""
        node = {"name": "test_model", "description": "A model"}

        result = get_sst_meta(node, node_type="model", node_name="test_model")

        assert result == {}

    def test_column_sst_meta_new_format(self):
        """Test reading column SST metadata from new format."""
        column = {
            "name": "user_id",
            "config": {
                "meta": {
                    "sst": {
                        "column_type": "dimension",
                        "data_type": "text",
                        "synonyms": ["customer_id"],
                    }
                }
            },
        }

        result = get_sst_meta(column, node_type="column", node_name="orders.user_id")

        assert result["column_type"] == "dimension"
        assert result["data_type"] == "text"
        assert result["synonyms"] == ["customer_id"]

    def test_column_sst_meta_legacy_format(self):
        """Test reading column SST metadata from legacy format."""
        column = {
            "name": "user_id",
            "meta": {
                "sst": {
                    "column_type": "dimension",
                    "data_type": "text",
                }
            },
        }

        result = get_sst_meta(column, node_type="column", node_name="orders.user_id")

        assert result["column_type"] == "dimension"
        assert result["data_type"] == "text"

    def test_emit_warning_can_be_disabled(self):
        """Test that deprecation warnings can be disabled."""
        node = {
            "name": "test_model",
            "meta": {
                "sst": {
                    "cortex_searchable": True,
                }
            },
        }

        # Should not emit warning when emit_warning=False
        result = get_sst_meta(node, node_type="model", node_name="test_model", emit_warning=False)

        assert result["cortex_searchable"] is True

    def test_invalid_meta_type_returns_empty_dict(self):
        """Test that invalid meta type (non-dict) returns empty dict."""
        node = {
            "name": "test_model",
            "meta": "not a dict",  # Invalid type
        }

        result = get_sst_meta(node, node_type="model", node_name="test_model")

        assert result == {}

    def test_invalid_config_type_falls_back_to_meta(self):
        """Test that invalid config type falls back to meta.sst."""
        node = {
            "name": "test_model",
            "config": "not a dict",  # Invalid type
            "meta": {
                "sst": {
                    "cortex_searchable": True,
                }
            },
        }

        result = get_sst_meta(node, node_type="model", node_name="test_model")

        assert result["cortex_searchable"] is True

    def test_invalid_sst_type_returns_empty_dict(self):
        """Test that invalid sst type (non-dict) returns empty dict."""
        node = {
            "name": "test_model",
            "meta": {
                "sst": "not a dict",  # Invalid type
            },
        }

        result = get_sst_meta(node, node_type="model", node_name="test_model")

        assert result == {}

