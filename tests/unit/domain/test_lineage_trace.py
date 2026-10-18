from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.lineage.graph import LineageGraph, LineageNode
from snowflake_semantic_tools.domain.lineage.trace import (
    DOWNSTREAM,
    SELF,
    UPSTREAM,
    TreeRow,
    changed_nodes,
    find_nodes,
    member_node,
    near_miss,
    trace,
    tree,
)
from snowflake_semantic_tools.domain.state import ArtifactEntry
from tests.helpers.lineage import lineage_manifest

_GRAPH = LineageGraph(
    {
        "model:orders": LineageNode("model:orders", "model", "orders", "DB.S.ORDERS"),
        "metric:revenue": LineageNode("metric:revenue", "metric", "revenue"),
        "semantic_view:orders": LineageNode("semantic_view:orders", "semantic_view", "orders"),
        "agent:helper": LineageNode("agent:helper", "agent", "helper"),
    },
    {
        "metric:revenue": ("model:orders",),
        "semantic_view:orders": ("metric:revenue", "model:orders"),
        "agent:helper": ("semantic_view:orders",),
    },
    {
        "model:orders": ("metric:revenue", "semantic_view:orders"),
        "metric:revenue": ("semantic_view:orders",),
        "semantic_view:orders": ("agent:helper",),
    },
)


def test_a_node_is_found_by_its_key_or_by_its_name() -> None:
    types = ("semantic_view", "agent")
    assert find_nodes(_GRAPH, "MODEL:Orders", types) == ("model:orders",)
    assert find_nodes(_GRAPH, "metric:rev*", types) == ("metric:revenue",)
    assert find_nodes(_GRAPH, "orders", types) == ("model:orders", "semantic_view:orders")
    assert find_nodes(_GRAPH, "view:orders", types) == ()
    assert near_miss(_GRAPH, "model:order") == "model:orders"
    assert near_miss(_GRAPH, "zzz") is None


def test_a_trace_lists_its_starts_then_each_direction_by_depth_with_the_edges_between() -> None:
    traced = trace(_GRAPH, ("metric:revenue", "nope"), changed=frozenset(("semantic_view:orders",)))
    assert [(item.node.key, item.direction, item.depth, item.changed) for item in traced.nodes] == [
        ("metric:revenue", SELF, 0, False),
        ("model:orders", UPSTREAM, 1, False),
        ("semantic_view:orders", DOWNSTREAM, 1, True),
        ("agent:helper", DOWNSTREAM, 2, False),
    ]
    assert traced.edges == (
        ("semantic_view:orders", "agent:helper"),
        ("model:orders", "metric:revenue"),
        ("metric:revenue", "semantic_view:orders"),
        ("model:orders", "semantic_view:orders"),
    )
    upstream = trace(_GRAPH, ("agent:helper",), downstream=False, depth=1)
    assert [item.node.key for item in upstream.nodes] == ["agent:helper", "semantic_view:orders"]


def test_a_view_s_member_fingerprint_names_the_member_lineage_keys() -> None:
    assert member_node("metric:orders.revenue") == "metric:revenue"
    assert member_node("metric:revenue") == "metric:revenue"
    assert member_node("dimension:orders.state") == "dimension:orders.state"
    assert member_node("table:orders") is None


def test_what_changed_is_each_artifact_by_fingerprint_and_each_member_by_what_its_views_record() -> None:
    previous = lineage_manifest()
    view = previous.artifacts["semantic_view:sales"]
    before = replace(
        view, member_fingerprints=(("metric:orders.revenue", "a"), ("metric:per_order", "b"), ("table:orders", "t"))
    )
    after = replace(
        view,
        fingerprint="0" * 64,
        member_fingerprints=(("metric:orders.revenue", "z"), ("metric:per_order", "b"), ("dimension:events.kind", "d")),
    )
    old = replace(previous, artifacts={**previous.artifacts, "semantic_view:sales": before})
    new = replace(
        previous,
        artifacts={
            **previous.artifacts,
            "semantic_view:sales": after,
            "tool:new": ArtifactEntry("tool", "new", "f", (), (), (), "X.Y.Z", 1),
        },
    )
    assert changed_nodes(old, new) == {"semantic_view:sales", "tool:new", "metric:revenue", "dimension:events.kind"}
    # A previous manifest with no member fingerprints says nothing of its members.
    assert changed_nodes(previous, new) == {"semantic_view:sales", "tool:new"}


def test_a_tree_shows_each_subtree_once_and_stops_at_its_depth() -> None:
    assert tree(_GRAPH, ("model:orders",), upstream=False) == (
        TreeRow("metric:revenue", 1),
        TreeRow("semantic_view:orders", 2),
        TreeRow("agent:helper", 3),
        TreeRow("semantic_view:orders", 1, repeated=True),
    )
    assert tree(_GRAPH, ("agent:helper",), upstream=True, depth=1) == (TreeRow("semantic_view:orders", 1),)
    # A start reached again from another start is a repeat, and the starts go in key order.
    assert tree(_GRAPH, ("model:orders", "metric:revenue"), upstream=False, depth=1) == (
        TreeRow("semantic_view:orders", 1),
        TreeRow("metric:revenue", 1, repeated=True),
        TreeRow("semantic_view:orders", 1, repeated=True),
    )
