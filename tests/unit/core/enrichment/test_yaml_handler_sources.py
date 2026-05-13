"""Tests for YAMLHandler source support."""

import pytest

from snowflake_semantic_tools.core.enrichment.yaml_handler import YAMLHandler


@pytest.fixture
def handler():
    return YAMLHandler()


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
                        "description": "Raw orders",
                        "columns": [
                            {"name": "id", "description": "Order ID"},
                        ],
                    },
                    {
                        "name": "customers",
                        "description": "Raw customers",
                        "columns": [],
                    },
                ],
            },
            {
                "name": "stripe",
                "database": "RAW_DB",
                "schema": "STRIPE",
                "tables": [
                    {"name": "payments", "description": "Stripe payments"},
                ],
            },
        ],
    }


@pytest.fixture
def model_yaml_content():
    return {
        "version": 2,
        "models": [
            {"name": "customers", "description": "Customers model"},
        ],
    }


class TestHasSources:

    def test_has_sources_true(self, handler, source_yaml_content):
        assert handler.has_sources(source_yaml_content) is True

    def test_has_sources_false_no_key(self, handler, model_yaml_content):
        assert handler.has_sources(model_yaml_content) is False

    def test_has_sources_false_empty_list(self, handler):
        assert handler.has_sources({"sources": []}) is False

    def test_has_sources_false_not_list(self, handler):
        assert handler.has_sources({"sources": "invalid"}) is False

    def test_has_sources_false_empty_dict(self, handler):
        assert handler.has_sources({}) is False


class TestFindSourceTableInYaml:

    def test_find_existing_table(self, handler, source_yaml_content):
        table = handler.find_source_table_in_yaml(source_yaml_content, "raw", "orders")
        assert table is not None
        assert table["name"] == "orders"
        assert table["description"] == "Raw orders"

    def test_find_table_in_second_source(self, handler, source_yaml_content):
        table = handler.find_source_table_in_yaml(source_yaml_content, "stripe", "payments")
        assert table is not None
        assert table["name"] == "payments"

    def test_table_not_found(self, handler, source_yaml_content):
        assert handler.find_source_table_in_yaml(source_yaml_content, "raw", "nonexistent") is None

    def test_source_not_found(self, handler, source_yaml_content):
        assert handler.find_source_table_in_yaml(source_yaml_content, "nonexistent", "orders") is None

    def test_no_sources_key(self, handler, model_yaml_content):
        assert handler.find_source_table_in_yaml(model_yaml_content, "raw", "orders") is None


class TestFindSourceInYaml:

    def test_find_source_group(self, handler, source_yaml_content):
        source = handler.find_source_in_yaml(source_yaml_content, "raw")
        assert source is not None
        assert source["name"] == "raw"
        assert len(source["tables"]) == 2

    def test_source_group_not_found(self, handler, source_yaml_content):
        assert handler.find_source_in_yaml(source_yaml_content, "nonexistent") is None


class TestDiscoverSourceYamlFiles:

    def test_discover_in_directory(self, handler, tmp_path):
        source_yaml = tmp_path / "models" / "staging" / "_sources.yml"
        source_yaml.parent.mkdir(parents=True)
        source_yaml.write_text("version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n")

        model_yaml = tmp_path / "models" / "marts" / "customers.yml"
        model_yaml.parent.mkdir(parents=True)
        model_yaml.write_text("version: 2\nmodels:\n  - name: customers\n")

        result = handler.discover_source_yaml_files(str(tmp_path / "models"))
        assert len(result) == 1
        assert str(source_yaml) in result[0]

    def test_discover_empty_directory(self, handler, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        assert handler.discover_source_yaml_files(str(empty_dir)) == []

    def test_discover_nonexistent_directory(self, handler):
        assert handler.discover_source_yaml_files("/nonexistent/path") == []


class TestEnsureSourceSstStructure:

    def test_adds_sst_to_source_table(self, handler):
        table = {"name": "orders", "description": "Raw orders"}
        result = handler.ensure_sst_structure(table)
        assert "config" in result
        assert "meta" in result["config"]
        assert "sst" in result["config"]["meta"]

    def test_preserves_existing_sst(self, handler):
        table = {
            "name": "orders",
            "config": {"meta": {"sst": {"synonyms": ["purchases"]}}},
        }
        result = handler.ensure_sst_structure(table)
        assert result["config"]["meta"]["sst"]["synonyms"] == ["purchases"]

    def test_source_column_sst(self, handler):
        col = {"name": "id", "description": "Order ID"}
        result = handler.ensure_column_sst_structure(col)
        assert "config" in result
        assert "meta" in result["config"]
        assert "sst" in result["config"]["meta"]
