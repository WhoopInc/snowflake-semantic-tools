"""Tests for LineageGraphBuilder and LineageGraph."""

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.services.lineage_builder import (
    LineageEdge,
    LineageGraph,
    LineageGraphBuilder,
    LineageNode,
    _make_node_id,
    _parse_list_field,
    _safe_str,
)


def _minimal_manifest(**overrides):
    tables = {
        "tables": [],
        "metrics": [],
        "relationships": [],
        "filters": [],
        "custom_instructions": [],
        "verified_queries": [],
        "semantic_views": [],
    }
    tables.update(overrides)
    return {"metadata": {"sst_version": "0.3.0"}, "tables": tables}


class TestHelpers:
    def test_make_node_id(self):
        assert _make_node_id("table", "orders") == "table:ORDERS"
        assert _make_node_id("metric", "total_revenue") == "metric:TOTAL_REVENUE"
        assert _make_node_id("semantic_view", "My View") == "semantic_view:MY VIEW"

    def test_safe_str_none(self):
        assert _safe_str(None) == ""

    def test_safe_str_normal(self):
        assert _safe_str("hello") == "hello"

    def test_safe_str_number(self):
        assert _safe_str(42) == "42"

    def test_parse_list_field_list(self):
        assert _parse_list_field(["a", "b"]) == ["a", "b"]

    def test_parse_list_field_json_string(self):
        assert _parse_list_field('["a", "b"]') == ["a", "b"]

    def test_parse_list_field_plain_string(self):
        assert _parse_list_field("orders") == ["orders"]

    def test_parse_list_field_empty_string(self):
        assert _parse_list_field("") == []

    def test_parse_list_field_none(self):
        assert _parse_list_field(None) == []

    def test_parse_list_field_invalid_json(self):
        assert _parse_list_field("{not json}") == ["{not json}"]

    def test_parse_list_field_number(self):
        assert _parse_list_field(42) == []


class TestLineageGraph:
    def test_add_and_get_node(self):
        graph = LineageGraph()
        node = LineageNode(id="table:ORDERS", type="table", name="orders")
        graph.add_node(node)
        assert graph.get_node("table:ORDERS") is node

    def test_get_node_missing(self):
        graph = LineageGraph()
        assert graph.get_node("nonexistent") is None

    def test_add_edge_valid(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="a", type="metric", name="m"))
        graph.add_node(LineageNode(id="b", type="table", name="t"))
        graph.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="references"))
        assert len(graph.edges) == 1

    def test_add_edge_missing_source(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="b", type="table", name="t"))
        graph.add_edge(LineageEdge(source_id="missing", target_id="b", edge_type="references"))
        assert len(graph.edges) == 0

    def test_add_edge_missing_target(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="a", type="metric", name="m"))
        graph.add_edge(LineageEdge(source_id="a", target_id="missing", edge_type="references"))
        assert len(graph.edges) == 0

    def test_get_upstream_simple(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="m", type="metric", name="metric1"))
        graph.add_node(LineageNode(id="t", type="table", name="orders"))
        graph.add_edge(LineageEdge(source_id="m", target_id="t", edge_type="references"))
        upstream = graph.get_upstream("m")
        assert len(upstream) == 1
        assert upstream[0].id == "t"

    def test_get_downstream_simple(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="m", type="metric", name="metric1"))
        graph.add_node(LineageNode(id="t", type="table", name="orders"))
        graph.add_edge(LineageEdge(source_id="m", target_id="t", edge_type="references"))
        downstream = graph.get_downstream("t")
        assert len(downstream) == 1
        assert downstream[0].id == "m"

    def test_get_upstream_multihop(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="a", type="metric", name="a"))
        graph.add_node(LineageNode(id="b", type="metric", name="b"))
        graph.add_node(LineageNode(id="c", type="table", name="c"))
        graph.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="composed_of"))
        graph.add_edge(LineageEdge(source_id="b", target_id="c", edge_type="references"))
        upstream = graph.get_upstream("a")
        assert len(upstream) == 2
        ids = {n.id for n in upstream}
        assert ids == {"b", "c"}

    def test_get_upstream_depth_limited(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="a", type="metric", name="a"))
        graph.add_node(LineageNode(id="b", type="metric", name="b"))
        graph.add_node(LineageNode(id="c", type="table", name="c"))
        graph.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="composed_of"))
        graph.add_edge(LineageEdge(source_id="b", target_id="c", edge_type="references"))
        upstream = graph.get_upstream("a", depth=1)
        assert len(upstream) == 1
        assert upstream[0].id == "b"

    def test_get_upstream_missing_node(self):
        graph = LineageGraph()
        assert graph.get_upstream("nonexistent") == []

    def test_get_downstream_missing_node(self):
        graph = LineageGraph()
        assert graph.get_downstream("nonexistent") == []

    def test_traversal_handles_cycles(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="a", type="metric", name="a"))
        graph.add_node(LineageNode(id="b", type="metric", name="b"))
        graph.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="composed_of"))
        graph.add_edge(LineageEdge(source_id="b", target_id="a", edge_type="composed_of"))
        upstream = graph.get_upstream("a")
        assert len(upstream) == 1
        assert upstream[0].id == "b"

    def test_to_dict(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="t:X", type="table", name="x"))
        graph.add_node(LineageNode(id="m:Y", type="metric", name="y"))
        graph.add_edge(LineageEdge(source_id="m:Y", target_id="t:X", edge_type="references"))
        d = graph.to_dict()
        assert d["summary"]["node_count"] == 2
        assert d["summary"]["edge_count"] == 1
        assert d["summary"]["node_types"] == {"table": 1, "metric": 1}
        assert len(d["nodes"]) == 2
        assert len(d["edges"]) == 1

    def test_to_d3_json(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="t:X", type="table", name="x"))
        graph.add_node(LineageNode(id="m:Y", type="metric", name="y"))
        graph.add_edge(LineageEdge(source_id="m:Y", target_id="t:X", edge_type="references"))
        d3 = graph.to_d3_json()
        assert "nodes" in d3
        assert "links" in d3
        assert len(d3["nodes"]) == 2
        assert d3["links"][0]["source"] == "m:Y"
        assert d3["links"][0]["target"] == "t:X"
        assert d3["links"][0]["type"] == "references"

    def test_empty_graph_to_dict(self):
        graph = LineageGraph()
        d = graph.to_dict()
        assert d["summary"]["node_count"] == 0
        assert d["summary"]["edge_count"] == 0

    def test_count_by_type(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="t:A", type="table", name="a"))
        graph.add_node(LineageNode(id="t:B", type="table", name="b"))
        graph.add_node(LineageNode(id="m:C", type="metric", name="c"))
        assert graph._count_by_type() == {"table": 2, "metric": 1}


class TestLineageGraphBuilder:
    def test_from_manifest_path(self, tmp_path):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders", "database": "DB", "schema": "PUBLIC"}],
            metrics=[{"name": "total", "tables": ["orders"], "expr": "COUNT(*)"}],
        )
        path = tmp_path / "sst_manifest.json"
        path.write_text(json.dumps(manifest))
        graph = LineageGraphBuilder.from_manifest_path(path)
        assert len(graph.nodes) == 2

    def test_from_manifest_path_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Manifest not found"):
            LineageGraphBuilder.from_manifest_path(tmp_path / "missing.json")

    def test_from_manifest_path_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{invalid json")
        with pytest.raises(ValueError, match="Invalid JSON"):
            LineageGraphBuilder.from_manifest_path(path)

    def test_empty_manifest(self):
        graph = LineageGraphBuilder.from_manifest_data({})
        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0

    def test_empty_tables_section(self):
        graph = LineageGraphBuilder.from_manifest_data({"tables": {}})
        assert len(graph.nodes) == 0

    def test_table_nodes(self):
        manifest = _minimal_manifest(
            tables=[
                {"table_name": "orders", "database": "DB", "schema": "PUBLIC", "primary_key": "order_id"},
                {"table_name": "customers", "database": "DB", "schema": "PUBLIC"},
            ]
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        assert graph.get_node("table:ORDERS") is not None
        assert graph.get_node("table:CUSTOMERS") is not None
        assert graph.get_node("table:ORDERS").metadata["primary_key"] == "order_id"

    def test_table_node_skips_empty_name(self):
        manifest = _minimal_manifest(tables=[{"table_name": "", "database": "DB"}])
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        assert len(graph.nodes) == 0

    def test_metric_nodes(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            metrics=[
                {
                    "name": "total_revenue",
                    "tables": ["orders"],
                    "expr": "SUM(amount)",
                    "description": "Total revenue",
                    "synonyms": ["revenue"],
                }
            ],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        node = graph.get_node("metric:TOTAL_REVENUE")
        assert node is not None
        assert node.metadata["expression"] == "SUM(amount)"
        assert node.metadata["synonyms"] == ["revenue"]

    def test_metric_edges_to_tables(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}, {"table_name": "customers"}],
            metrics=[{"name": "cross_metric", "tables": ["orders", "customers"], "expr": "COUNT(*)"}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        edges = [e for e in graph.edges if e.source_id == "metric:CROSS_METRIC"]
        assert len(edges) == 2
        targets = {e.target_id for e in edges}
        assert targets == {"table:ORDERS", "table:CUSTOMERS"}

    def test_metric_table_name_fallback(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            metrics=[{"name": "simple", "table_name": "orders", "expr": "COUNT(*)"}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        edges = [e for e in graph.edges if e.source_id == "metric:SIMPLE"]
        assert len(edges) == 1
        assert edges[0].target_id == "table:ORDERS"

    def test_metric_composition_edges(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            metrics=[
                {"name": "total_revenue", "tables": ["orders"], "expr": "SUM(amount)"},
                {"name": "total_orders", "tables": ["orders"], "expr": "COUNT(*)"},
                {
                    "name": "avg_order_value",
                    "tables": ["orders"],
                    "expr": "{{ metric('total_revenue') }} / NULLIF({{ metric('total_orders') }}, 0)",
                },
            ],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        composed_edges = [e for e in graph.edges if e.edge_type == "composed_of"]
        assert len(composed_edges) == 2
        targets = {e.target_id for e in composed_edges}
        assert targets == {"metric:TOTAL_REVENUE", "metric:TOTAL_ORDERS"}

    def test_relationship_nodes_and_edges(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}, {"table_name": "customers"}],
            relationships=[
                {
                    "relationship_name": "orders_to_customers",
                    "left_table_name": "orders",
                    "right_table_name": "customers",
                    "join_type": "LEFT",
                    "relationship_type": "many_to_one",
                }
            ],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        rel_node = graph.get_node("relationship:ORDERS_TO_CUSTOMERS")
        assert rel_node is not None
        assert rel_node.metadata["left_table"] == "orders"
        assert rel_node.metadata["right_table"] == "customers"
        join_edges = [e for e in graph.edges if e.edge_type == "joins"]
        assert len(join_edges) == 2

    def test_filter_nodes_and_edges(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            filters=[{"name": "recent_orders", "table_name": "orders", "expr": "date >= CURRENT_DATE - 30"}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        assert graph.get_node("filter:RECENT_ORDERS") is not None
        ref_edges = [e for e in graph.edges if e.source_id == "filter:RECENT_ORDERS"]
        assert len(ref_edges) == 1
        assert ref_edges[0].target_id == "table:ORDERS"

    def test_filter_without_table_no_edge(self):
        manifest = _minimal_manifest(filters=[{"name": "orphan_filter", "table_name": "", "expr": "1=1"}])
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        assert graph.get_node("filter:ORPHAN_FILTER") is not None
        assert len(graph.edges) == 0

    def test_custom_instruction_nodes(self):
        manifest = _minimal_manifest(
            custom_instructions=[
                {"name": "privacy_rules", "question_categorization": "Reject PII", "sql_generation": "Use aliases"}
            ]
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        node = graph.get_node("custom_instruction:PRIVACY_RULES")
        assert node is not None
        assert node.metadata["question_categorization"] == "Reject PII"

    def test_verified_query_nodes_and_edges(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            verified_queries=[
                {
                    "name": "monthly_revenue",
                    "question": "What is monthly revenue?",
                    "sql": "SELECT 1",
                    "tables": ["orders"],
                }
            ],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        node = graph.get_node("verified_query:MONTHLY_REVENUE")
        assert node is not None
        ref_edges = [e for e in graph.edges if e.source_id == "verified_query:MONTHLY_REVENUE"]
        assert len(ref_edges) == 1

    def test_semantic_view_nodes_and_edges(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}, {"table_name": "customers"}],
            custom_instructions=[{"name": "rules", "sql_generation": "test"}],
            semantic_views=[
                {
                    "name": "sales_view",
                    "description": "Sales analytics",
                    "tables": ["orders", "customers"],
                    "custom_instructions": ["rules"],
                }
            ],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        node = graph.get_node("semantic_view:SALES_VIEW")
        assert node is not None
        includes = [e for e in graph.edges if e.source_id == "semantic_view:SALES_VIEW" and e.edge_type == "includes"]
        assert len(includes) == 2
        uses = [e for e in graph.edges if e.source_id == "semantic_view:SALES_VIEW" and e.edge_type == "uses"]
        assert len(uses) == 1

    def test_semantic_view_tables_json_string(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            semantic_views=[{"name": "v1", "tables": '["orders"]'}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        includes = [e for e in graph.edges if e.edge_type == "includes"]
        assert len(includes) == 1

    def test_full_graph_integration(self):
        manifest = _minimal_manifest(
            tables=[
                {"table_name": "orders", "database": "DB", "schema": "PUBLIC", "primary_key": "order_id"},
                {"table_name": "customers", "database": "DB", "schema": "PUBLIC", "primary_key": "customer_id"},
            ],
            metrics=[
                {"name": "total_revenue", "tables": ["orders"], "expr": "SUM(amount)", "description": "Total revenue"},
                {"name": "total_orders", "tables": ["orders"], "expr": "COUNT(*)", "description": "Order count"},
                {
                    "name": "aov",
                    "tables": ["orders"],
                    "expr": "{{ metric('total_revenue') }} / NULLIF({{ metric('total_orders') }}, 0)",
                },
            ],
            relationships=[
                {
                    "relationship_name": "orders_to_customers",
                    "left_table_name": "orders",
                    "right_table_name": "customers",
                }
            ],
            filters=[{"name": "active_only", "table_name": "customers", "expr": "status = 'active'"}],
            custom_instructions=[{"name": "privacy", "sql_generation": "No PII"}],
            verified_queries=[
                {
                    "name": "q1",
                    "question": "Total revenue?",
                    "sql": "SELECT SUM(amount) FROM orders",
                    "tables": ["orders"],
                }
            ],
            semantic_views=[
                {
                    "name": "customer_360",
                    "tables": ["orders", "customers"],
                    "custom_instructions": ["privacy"],
                    "description": "Customer analytics",
                }
            ],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)

        assert len(graph.nodes) == 10
        assert graph.get_node("table:ORDERS") is not None
        assert graph.get_node("table:CUSTOMERS") is not None
        assert graph.get_node("metric:TOTAL_REVENUE") is not None
        assert graph.get_node("metric:TOTAL_ORDERS") is not None
        assert graph.get_node("metric:AOV") is not None
        assert graph.get_node("relationship:ORDERS_TO_CUSTOMERS") is not None
        assert graph.get_node("filter:ACTIVE_ONLY") is not None
        assert graph.get_node("custom_instruction:PRIVACY") is not None
        assert graph.get_node("verified_query:Q1") is not None
        assert graph.get_node("semantic_view:CUSTOMER_360") is not None

        assert len(graph.edges) > 0

        sv_downstream = graph.get_downstream("table:ORDERS")
        sv_names = {n.id for n in sv_downstream}
        assert "semantic_view:CUSTOMER_360" in sv_names
        assert "metric:TOTAL_REVENUE" in sv_names

    def test_json_roundtrip(self, tmp_path):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            metrics=[{"name": "total", "tables": ["orders"], "expr": "COUNT(*)"}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        d = graph.to_dict()
        json_str = json.dumps(d)
        reloaded = json.loads(json_str)
        assert reloaded["summary"]["node_count"] == 2
        assert reloaded["summary"]["edge_count"] == 1

    def test_d3_json_structure(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            metrics=[{"name": "total", "tables": ["orders"], "expr": "COUNT(*)"}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        d3 = graph.to_d3_json()
        assert all("id" in n and "type" in n and "name" in n for n in d3["nodes"])
        assert all("source" in l and "target" in l and "type" in l for l in d3["links"])

    def test_metric_skips_empty_name(self):
        manifest = _minimal_manifest(
            tables=[{"table_name": "orders"}],
            metrics=[{"name": "", "tables": ["orders"], "expr": "COUNT(*)"}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        metric_nodes = [n for n in graph.nodes.values() if n.type == "metric"]
        assert len(metric_nodes) == 0

    def test_relationship_skips_empty_name(self):
        manifest = _minimal_manifest(
            relationships=[{"relationship_name": "", "left_table_name": "a", "right_table_name": "b"}]
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        rel_nodes = [n for n in graph.nodes.values() if n.type == "relationship"]
        assert len(rel_nodes) == 0

    def test_edge_to_nonexistent_table_skipped(self):
        manifest = _minimal_manifest(
            metrics=[{"name": "orphan", "tables": ["nonexistent"], "expr": "COUNT(*)"}],
        )
        graph = LineageGraphBuilder.from_manifest_data(manifest)
        assert graph.get_node("metric:ORPHAN") is not None
        ref_edges = [e for e in graph.edges if e.source_id == "metric:ORPHAN"]
        assert len(ref_edges) == 0
