"""
Lineage Graph Builder

Constructs a directed acyclic graph (DAG) of semantic model component
dependencies from a compiled SST manifest. Used by the docs generator
for interactive lineage visualization and by future commands for
impact analysis.

Node types: table, metric, relationship, semantic_view, filter,
            custom_instruction, verified_query

Edge types:
  - references:   metric -> table, filter -> table, verified_query -> table
  - composed_of:  metric -> metric (via {{ metric() }} templates)
  - joins:        relationship -> table (left and right)
  - includes:     semantic_view -> table
  - uses:         semantic_view -> custom_instruction
"""

import json
import re
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("lineage_builder")

METRIC_TEMPLATE_RE = re.compile(r"\{\{\s*metric\(['\"]([^'\"]+)['\"]\)\s*\}\}")


@dataclass
class LineageNode:
    id: str
    type: str
    name: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    source_id: str
    target_id: str
    edge_type: str


@dataclass
class LineageGraph:
    nodes: Dict[str, LineageNode] = field(default_factory=dict)
    edges: List[LineageEdge] = field(default_factory=list)

    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        if edge.source_id in self.nodes and edge.target_id in self.nodes:
            self.edges.append(edge)
        else:
            missing = []
            if edge.source_id not in self.nodes:
                missing.append(edge.source_id)
            if edge.target_id not in self.nodes:
                missing.append(edge.target_id)
            logger.debug(f"Skipping edge {edge.source_id} -> {edge.target_id}: missing node(s) {missing}")

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        return self.nodes.get(node_id)

    def get_upstream(self, node_id: str, depth: int = -1) -> List[LineageNode]:
        return self._traverse(node_id, direction="upstream", depth=depth)

    def get_downstream(self, node_id: str, depth: int = -1) -> List[LineageNode]:
        return self._traverse(node_id, direction="downstream", depth=depth)

    def _traverse(self, start_id: str, direction: str, depth: int) -> List[LineageNode]:
        if start_id not in self.nodes:
            return []

        adj: Dict[str, List[str]] = {}
        for edge in self.edges:
            if direction == "upstream":
                adj.setdefault(edge.source_id, []).append(edge.target_id)
            else:
                adj.setdefault(edge.target_id, []).append(edge.source_id)

        visited: Set[str] = set()
        result: List[LineageNode] = []
        queue: deque = deque()
        queue.append((start_id, 0))
        visited.add(start_id)

        while queue:
            current_id, current_depth = queue.popleft()
            if depth != -1 and current_depth > depth:
                continue

            for neighbor_id in adj.get(current_id, []):
                if neighbor_id not in visited:
                    visited.add(neighbor_id)
                    node = self.nodes.get(neighbor_id)
                    if node:
                        result.append(node)
                    if depth == -1 or current_depth + 1 < depth:
                        queue.append((neighbor_id, current_depth + 1))

        return result

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [
                {"id": n.id, "type": n.type, "name": n.name, "metadata": n.metadata} for n in self.nodes.values()
            ],
            "edges": [
                {"source_id": e.source_id, "target_id": e.target_id, "edge_type": e.edge_type} for e in self.edges
            ],
            "summary": {
                "node_count": len(self.nodes),
                "edge_count": len(self.edges),
                "node_types": self._count_by_type(),
            },
        }

    def to_d3_json(self) -> Dict[str, Any]:
        return {
            "nodes": [
                {"id": n.id, "type": n.type, "name": n.name, "metadata": n.metadata} for n in self.nodes.values()
            ],
            "links": [{"source": e.source_id, "target": e.target_id, "type": e.edge_type} for e in self.edges],
        }

    def _count_by_type(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for node in self.nodes.values():
            counts[node.type] = counts.get(node.type, 0) + 1
        return counts


def _make_node_id(node_type: str, name: str) -> str:
    return f"{node_type}:{name.upper()}"


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _parse_list_field(value: Any) -> List[str]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        return [value] if value.strip() else []
    return []


class LineageGraphBuilder:

    @staticmethod
    def from_manifest_path(manifest_path: Path) -> "LineageGraph":
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Manifest not found at {manifest_path}. "
                f"Run 'sst compile' first, or use 'sst docs generate' which auto-compiles."
            )

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in manifest {manifest_path}: {e}") from e

        return LineageGraphBuilder.from_manifest_data(data)

    @staticmethod
    def from_manifest_data(data: Dict[str, Any]) -> "LineageGraph":
        tables_data = data.get("tables", {})
        if not tables_data:
            logger.warning("Manifest contains no 'tables' section — lineage graph will be empty")
            return LineageGraph()

        graph = LineageGraph()

        LineageGraphBuilder._add_table_nodes(graph, tables_data)
        LineageGraphBuilder._add_metric_nodes(graph, tables_data)
        LineageGraphBuilder._add_relationship_nodes(graph, tables_data)
        LineageGraphBuilder._add_filter_nodes(graph, tables_data)
        LineageGraphBuilder._add_custom_instruction_nodes(graph, tables_data)
        LineageGraphBuilder._add_verified_query_nodes(graph, tables_data)
        LineageGraphBuilder._add_semantic_view_nodes(graph, tables_data)

        LineageGraphBuilder._add_metric_edges(graph, tables_data)
        LineageGraphBuilder._add_relationship_edges(graph, tables_data)
        LineageGraphBuilder._add_filter_edges(graph, tables_data)
        LineageGraphBuilder._add_verified_query_edges(graph, tables_data)
        LineageGraphBuilder._add_semantic_view_edges(graph, tables_data)

        logger.debug(f"Built lineage graph: {len(graph.nodes)} nodes, {len(graph.edges)} edges")
        return graph

    @staticmethod
    def _add_table_nodes(graph: LineageGraph, tables_data: Dict) -> None:
        for table in tables_data.get("tables", []):
            name = _safe_str(table.get("table_name"))
            if not name:
                continue
            node_id = _make_node_id("table", name)
            graph.add_node(
                LineageNode(
                    id=node_id,
                    type="table",
                    name=name,
                    metadata={
                        "database": _safe_str(table.get("database")),
                        "schema": _safe_str(table.get("schema")),
                        "primary_key": _safe_str(table.get("primary_key")),
                        "description": _safe_str(table.get("description")),
                        "source_file": _safe_str(table.get("source_file")),
                    },
                )
            )

    @staticmethod
    def _add_metric_nodes(graph: LineageGraph, tables_data: Dict) -> None:
        for metric in tables_data.get("metrics", []):
            name = _safe_str(metric.get("name"))
            if not name:
                continue
            tables = _parse_list_field(metric.get("tables", []))
            table_name = _safe_str(metric.get("table_name"))
            node_id = _make_node_id("metric", name)
            graph.add_node(
                LineageNode(
                    id=node_id,
                    type="metric",
                    name=name,
                    metadata={
                        "expression": _safe_str(metric.get("expr")),
                        "description": _safe_str(metric.get("description")),
                        "tables": tables if tables else ([table_name] if table_name else []),
                        "synonyms": metric.get("synonyms", []),
                        "source_file": _safe_str(metric.get("source_file")),
                    },
                )
            )

    @staticmethod
    def _add_relationship_nodes(graph: LineageGraph, tables_data: Dict) -> None:
        for rel in tables_data.get("relationships", []):
            name = _safe_str(rel.get("relationship_name"))
            if not name:
                continue
            node_id = _make_node_id("relationship", name)
            graph.add_node(
                LineageNode(
                    id=node_id,
                    type="relationship",
                    name=name,
                    metadata={
                        "left_table": _safe_str(rel.get("left_table_name")),
                        "right_table": _safe_str(rel.get("right_table_name")),
                        "relationship_type": _safe_str(rel.get("relationship_type")),
                        "join_type": _safe_str(rel.get("join_type")),
                        "source_file": _safe_str(rel.get("source_file")),
                    },
                )
            )

    @staticmethod
    def _add_filter_nodes(graph: LineageGraph, tables_data: Dict) -> None:
        for filt in tables_data.get("filters", []):
            name = _safe_str(filt.get("name"))
            if not name:
                continue
            node_id = _make_node_id("filter", name)
            graph.add_node(
                LineageNode(
                    id=node_id,
                    type="filter",
                    name=name,
                    metadata={
                        "expression": _safe_str(filt.get("expr")),
                        "table_name": _safe_str(filt.get("table_name")),
                        "description": _safe_str(filt.get("description")),
                        "source_file": _safe_str(filt.get("source_file")),
                    },
                )
            )

    @staticmethod
    def _add_custom_instruction_nodes(graph: LineageGraph, tables_data: Dict) -> None:
        for ci in tables_data.get("custom_instructions", []):
            name = _safe_str(ci.get("name"))
            if not name:
                continue
            node_id = _make_node_id("custom_instruction", name)
            graph.add_node(
                LineageNode(
                    id=node_id,
                    type="custom_instruction",
                    name=name,
                    metadata={
                        "question_categorization": _safe_str(ci.get("question_categorization")),
                        "sql_generation": _safe_str(ci.get("sql_generation")),
                        "source_file": _safe_str(ci.get("source_file")),
                    },
                )
            )

    @staticmethod
    def _add_verified_query_nodes(graph: LineageGraph, tables_data: Dict) -> None:
        for vq in tables_data.get("verified_queries", []):
            name = _safe_str(vq.get("name"))
            if not name:
                continue
            tables = _parse_list_field(vq.get("tables", []))
            node_id = _make_node_id("verified_query", name)
            graph.add_node(
                LineageNode(
                    id=node_id,
                    type="verified_query",
                    name=name,
                    metadata={
                        "question": _safe_str(vq.get("question")),
                        "sql": _safe_str(vq.get("sql")),
                        "tables": tables,
                        "verified_by": _safe_str(vq.get("verified_by")),
                        "verified_at": _safe_str(vq.get("verified_at")),
                        "source_file": _safe_str(vq.get("source_file")),
                    },
                )
            )

    @staticmethod
    def _add_semantic_view_nodes(graph: LineageGraph, tables_data: Dict) -> None:
        for sv in tables_data.get("semantic_views", []):
            name = _safe_str(sv.get("name"))
            if not name:
                continue
            tables = _parse_list_field(sv.get("tables", []))
            custom_instructions = _parse_list_field(sv.get("custom_instructions", []))
            node_id = _make_node_id("semantic_view", name)
            graph.add_node(
                LineageNode(
                    id=node_id,
                    type="semantic_view",
                    name=name,
                    metadata={
                        "description": _safe_str(sv.get("description")),
                        "tables": tables,
                        "custom_instructions": custom_instructions,
                        "source_file": _safe_str(sv.get("source_file")),
                    },
                )
            )

    @staticmethod
    def _add_metric_edges(graph: LineageGraph, tables_data: Dict) -> None:
        for metric in tables_data.get("metrics", []):
            name = _safe_str(metric.get("name"))
            if not name:
                continue
            metric_id = _make_node_id("metric", name)

            tables = _parse_list_field(metric.get("tables", []))
            table_name = _safe_str(metric.get("table_name"))
            all_tables = tables if tables else ([table_name] if table_name else [])
            for table in all_tables:
                table_id = _make_node_id("table", table)
                graph.add_edge(LineageEdge(source_id=metric_id, target_id=table_id, edge_type="references"))

            expr = _safe_str(metric.get("expr"))
            if expr:
                for ref_name in METRIC_TEMPLATE_RE.findall(expr):
                    ref_id = _make_node_id("metric", ref_name)
                    graph.add_edge(LineageEdge(source_id=metric_id, target_id=ref_id, edge_type="composed_of"))

    @staticmethod
    def _add_relationship_edges(graph: LineageGraph, tables_data: Dict) -> None:
        for rel in tables_data.get("relationships", []):
            name = _safe_str(rel.get("relationship_name"))
            if not name:
                continue
            rel_id = _make_node_id("relationship", name)
            left = _safe_str(rel.get("left_table_name"))
            right = _safe_str(rel.get("right_table_name"))
            if left:
                graph.add_edge(LineageEdge(source_id=rel_id, target_id=_make_node_id("table", left), edge_type="joins"))
            if right:
                graph.add_edge(
                    LineageEdge(source_id=rel_id, target_id=_make_node_id("table", right), edge_type="joins")
                )

    @staticmethod
    def _add_filter_edges(graph: LineageGraph, tables_data: Dict) -> None:
        for filt in tables_data.get("filters", []):
            name = _safe_str(filt.get("name"))
            table = _safe_str(filt.get("table_name"))
            if not name or not table:
                continue
            graph.add_edge(
                LineageEdge(
                    source_id=_make_node_id("filter", name),
                    target_id=_make_node_id("table", table),
                    edge_type="references",
                )
            )

    @staticmethod
    def _add_verified_query_edges(graph: LineageGraph, tables_data: Dict) -> None:
        for vq in tables_data.get("verified_queries", []):
            name = _safe_str(vq.get("name"))
            if not name:
                continue
            vq_id = _make_node_id("verified_query", name)
            tables = _parse_list_field(vq.get("tables", []))
            for table in tables:
                graph.add_edge(
                    LineageEdge(source_id=vq_id, target_id=_make_node_id("table", table), edge_type="references")
                )

    @staticmethod
    def _add_semantic_view_edges(graph: LineageGraph, tables_data: Dict) -> None:
        for sv in tables_data.get("semantic_views", []):
            name = _safe_str(sv.get("name"))
            if not name:
                continue
            sv_id = _make_node_id("semantic_view", name)
            tables = _parse_list_field(sv.get("tables", []))
            for table in tables:
                graph.add_edge(
                    LineageEdge(source_id=sv_id, target_id=_make_node_id("table", table), edge_type="includes")
                )
            custom_instructions = _parse_list_field(sv.get("custom_instructions", []))
            for ci in custom_instructions:
                graph.add_edge(
                    LineageEdge(source_id=sv_id, target_id=_make_node_id("custom_instruction", ci), edge_type="uses")
                )
