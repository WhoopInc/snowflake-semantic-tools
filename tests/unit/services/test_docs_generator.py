"""Tests for DocsGenerator."""

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.services.docs_generator import DocsConfig, DocsGenerator, DocsResult
from snowflake_semantic_tools.services.lineage_builder import LineageGraph, LineageGraphBuilder


def _test_manifest(**overrides):
    tables = {
        "tables": [
            {
                "table_name": "orders",
                "database": "DB",
                "schema": "PUBLIC",
                "primary_key": "order_id",
                "description": "Order data",
            },
            {"table_name": "customers", "database": "DB", "schema": "PUBLIC", "primary_key": "customer_id"},
        ],
        "metrics": [
            {
                "name": "total_revenue",
                "tables": ["orders"],
                "expr": "SUM(amount)",
                "description": "Total revenue",
                "synonyms": ["revenue"],
            },
        ],
        "relationships": [
            {"relationship_name": "orders_to_customers", "left_table_name": "orders", "right_table_name": "customers"},
        ],
        "filters": [
            {
                "name": "recent_orders",
                "table_name": "orders",
                "expr": "date >= CURRENT_DATE - 30",
                "description": "Last 30 days",
            },
        ],
        "custom_instructions": [
            {"name": "privacy", "sql_generation": "No PII", "question_categorization": "Reject PII questions"},
        ],
        "verified_queries": [
            {
                "name": "monthly_rev",
                "question": "What is monthly revenue?",
                "sql": "SELECT SUM(amount) FROM orders",
                "tables": ["orders"],
            },
        ],
        "semantic_views": [
            {
                "name": "sales_view",
                "tables": ["orders", "customers"],
                "description": "Sales analytics",
                "custom_instructions": ["privacy"],
            },
        ],
    }
    tables.update(overrides)
    return {"metadata": {"sst_version": "0.3.0", "generated_at": "2026-05-13T00:00:00Z"}, "tables": tables}


def _empty_manifest():
    return {"metadata": {"sst_version": "0.3.0", "generated_at": "2026-05-13T00:00:00Z"}, "tables": {}}


@pytest.fixture
def manifest():
    return _test_manifest()


@pytest.fixture
def empty_manifest():
    return _empty_manifest()


@pytest.fixture
def graph(manifest):
    return LineageGraphBuilder.from_manifest_data(manifest)


@pytest.fixture
def empty_graph(empty_manifest):
    return LineageGraphBuilder.from_manifest_data(empty_manifest)


class TestDocsGeneratorHTML:
    def test_generates_expected_files(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        result = gen.generate(DocsConfig(output_dir=tmp_path / "docs", format="html"))
        assert result.success
        assert len(result.files_created) == 3
        assert (tmp_path / "docs" / "index.html").exists()
        assert (tmp_path / "docs" / "lineage.html").exists()
        assert (tmp_path / "docs" / "data.json").exists()

    def test_index_contains_component_names(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "index.html").read_text()
        assert "total_revenue" in html
        assert "orders" in html
        assert "customers" in html
        assert "orders_to_customers" in html
        assert "sales_view" in html

    def test_index_contains_stat_cards(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "index.html").read_text()
        assert "Tables" in html
        assert "Metrics" in html
        assert "Relationships" in html

    def test_index_contains_search_data(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "index.html").read_text()
        assert "search-data" in html
        assert "search-overlay" in html

    def test_lineage_contains_graph_data(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "lineage.html").read_text()
        assert "graphData" in html or "graph_data" in html
        assert "initLineage" in html

    def test_lineage_contains_d3(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "lineage.html").read_text()
        assert "d3" in html.lower()

    def test_lineage_contains_filter_checkboxes(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "lineage.html").read_text()
        assert 'value="table"' in html
        assert 'value="metric"' in html

    def test_generates_data_json_alongside_html(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        data = json.loads((tmp_path / "docs" / "data.json").read_text())
        assert "catalog" in data
        assert "lineage" in data
        assert "summary" in data

    def test_empty_manifest_still_generates(self, tmp_path, empty_manifest, empty_graph):
        gen = DocsGenerator(empty_manifest, empty_graph)
        result = gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        assert result.success
        assert (tmp_path / "docs" / "index.html").exists()

    def test_output_dir_auto_created(self, tmp_path, manifest, graph):
        deep_path = tmp_path / "a" / "b" / "c"
        gen = DocsGenerator(manifest, graph)
        result = gen.generate(DocsConfig(output_dir=deep_path))
        assert result.success
        assert deep_path.exists()

    def test_html_includes_pico_css(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "index.html").read_text()
        assert "pico" in html.lower() or "--pico" in html or ":root" in html

    def test_html_has_dark_mode_support(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs"))
        html = (tmp_path / "docs" / "index.html").read_text()
        assert "prefers-color-scheme" in html


class TestDocsGeneratorJSON:
    def test_json_format(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        result = gen.generate(DocsConfig(output_dir=tmp_path / "docs", format="json"))
        assert result.success
        assert len(result.files_created) == 1
        assert (tmp_path / "docs" / "data.json").exists()

    def test_json_structure(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs", format="json"))
        data = json.loads((tmp_path / "docs" / "data.json").read_text())
        assert "metadata" in data
        assert "catalog" in data
        assert "lineage" in data
        assert "summary" in data

    def test_json_catalog_complete(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs", format="json"))
        data = json.loads((tmp_path / "docs" / "data.json").read_text())
        catalog = data["catalog"]
        assert len(catalog["tables"]) == 2
        assert len(catalog["metrics"]) == 1
        assert len(catalog["relationships"]) == 1
        assert len(catalog["filters"]) == 1
        assert len(catalog["semantic_views"]) == 1

    def test_json_lineage_has_nodes_and_edges(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs", format="json"))
        data = json.loads((tmp_path / "docs" / "data.json").read_text())
        lineage = data["lineage"]
        assert lineage["summary"]["node_count"] > 0
        assert lineage["summary"]["edge_count"] > 0

    def test_json_summary_counts(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        gen.generate(DocsConfig(output_dir=tmp_path / "docs", format="json"))
        data = json.loads((tmp_path / "docs" / "data.json").read_text())
        summary = data["summary"]
        assert summary["tables"] == 2
        assert summary["metrics"] == 1
        assert summary["total"] > 0


class TestDocsGeneratorEdgeCases:
    def test_invalid_format_returns_error(self, tmp_path, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        result = gen.generate(DocsConfig(output_dir=tmp_path / "docs", format="xml"))
        assert not result.success
        assert any("Unsupported format" in e for e in result.errors)

    def test_read_only_output_dir(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        result = gen.generate(DocsConfig(output_dir=Path("/proc/nonexistent/path")))
        assert not result.success
        assert len(result.errors) > 0


class TestDocsGeneratorSearchData:
    def test_search_data_includes_all_types(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        catalog = gen._build_catalog()
        search_data = gen._build_search_data(catalog)
        types_found = {item["type"] for item in search_data}
        assert "table" in types_found
        assert "metric" in types_found
        assert "relationship" in types_found
        assert "semantic_view" in types_found
        assert "filter" in types_found
        assert "custom_instruction" in types_found
        assert "verified_query" in types_found

    def test_search_data_has_metric_synonyms(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        catalog = gen._build_catalog()
        search_data = gen._build_search_data(catalog)
        metric_items = [i for i in search_data if i["type"] == "metric"]
        assert any(i["synonyms"] for i in metric_items)

    def test_search_data_has_expressions(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        catalog = gen._build_catalog()
        search_data = gen._build_search_data(catalog)
        metric_items = [i for i in search_data if i["type"] == "metric"]
        assert any(i["expression"] for i in metric_items)


class TestDocsGeneratorSummary:
    def test_summary_counts_correct(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        summary = gen._build_summary()
        assert summary["tables"] == 2
        assert summary["metrics"] == 1
        assert summary["relationships"] == 1
        assert summary["filters"] == 1
        assert summary["custom_instructions"] == 1
        assert summary["verified_queries"] == 1
        assert summary["semantic_views"] == 1
        assert summary["total"] == 8

    def test_empty_summary(self, empty_manifest, empty_graph):
        gen = DocsGenerator(empty_manifest, empty_graph)
        summary = gen._build_summary()
        assert summary["total"] == 0


class TestDocsGeneratorCardRendering:
    def test_render_table_card(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        card = gen._render_card(
            "table",
            "orders",
            "Order data",
            {"table_name": "orders", "database": "DB", "schema": "PUBLIC", "primary_key": "order_id"},
        )
        assert "orders" in card
        assert "table" in card
        assert "DB.PUBLIC" in card
        assert "PK: order_id" in card

    def test_render_metric_card(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        card = gen._render_card(
            "metric",
            "total_revenue",
            "Total revenue",
            {"name": "total_revenue", "tables": ["orders"], "expr": "SUM(amount)", "synonyms": ["revenue"]},
        )
        assert "total_revenue" in card
        assert "SUM(amount)" in card
        assert "revenue" in card

    def test_render_card_no_description(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        card = gen._render_card("filter", "test", "", {"name": "test"})
        assert "test" in card
        assert "sst-card-desc" not in card

    def test_render_relationship_card(self, manifest, graph):
        gen = DocsGenerator(manifest, graph)
        card = gen._render_card(
            "relationship",
            "orders_to_customers",
            "",
            {"relationship_name": "orders_to_customers", "left_table_name": "orders", "right_table_name": "customers"},
        )
        assert "orders" in card
        assert "customers" in card
