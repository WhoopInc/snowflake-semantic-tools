from __future__ import annotations

import dataclasses
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.lineage.graph import (
    EDGE_KINDS,
    OTHER_PROJECT,
    OTHER_TEAM,
    RAW_SOURCE,
    UNKNOWN_READS,
    GraphSelector,
    LineageGraph,
    dbt_lineage,
    manifest_lineage,
    node_key,
    parse_graph_selector,
)
from snowflake_semantic_tools.domain.model.dbt import DbtNode
from snowflake_semantic_tools.domain.state import ArtifactEntry
from tests.helpers.lineage import lineage_manifest


def test_a_manifest_s_lineage_runs_from_dbt_through_members_to_artifacts() -> None:
    graph = manifest_lineage(lineage_manifest())
    assert graph.parents["test:unique"] == ("model:orders",)
    assert graph.nodes["model:stg_orders"].relation is None
    assert graph.nodes["model:orders"].package == "p"
    assert graph.parents["model:orders"] == ("model:stg_orders",)
    assert graph.parents["model:stg_orders"] == ("source:raw.orders",)
    assert graph.children["model:orders"] == (
        "exposure:board",
        "metric:revenue",
        "model:built",
        "semantic_view:sales",
        "test:unique",
        "tool:status",
    )
    assert graph.parents["metric:per_order"] == ("metric:revenue",)
    assert graph.parents["dimension:events.kind"] == ("source:raw.events",)
    assert graph.parents["agent:helper"] == ("semantic_view:sales", "tool:status", "tool_ref:partner")
    assert graph.parents["coco_desktop_profile:p"] == ("agent:helper",)
    assert graph.parents["snapshot:history"] == ("model:shared",)


def test_where_a_walk_ends_at_someone_else_s_work_the_node_says_so() -> None:
    graph = manifest_lineage(lineage_manifest())
    markers = {key: node.marker for key, node in graph.nodes.items() if node.marker}
    assert markers == {
        "source:raw.orders": RAW_SOURCE,
        "source:raw.events": RAW_SOURCE,
        "model:shared": OTHER_PROJECT,
        "tool_ref:partner": OTHER_TEAM,
        "tool:lookup": UNKNOWN_READS,
    }


def test_a_manifest_without_dbt_s_graph_still_has_the_models_artifacts_read() -> None:
    graph = manifest_lineage(lineage_manifest(dbt_nodes={}))
    assert graph.nodes["model:orders"].kind == "model"
    assert "model:orders" not in graph.parents


def test_a_walk_goes_as_far_as_its_operators_say() -> None:
    graph = manifest_lineage(lineage_manifest())
    assert graph.reach(("model:orders",), upstream=True) == {
        "model:orders": 0,
        "model:stg_orders": 1,
        "source:raw.orders": 2,
    }
    assert graph.reach(("model:orders", "nope"), upstream=True, depth=1) == {"model:orders": 0, "model:stg_orders": 1}
    downstream = graph.walk(("model:orders",), GraphSelector("model:orders", downstream=None))
    assert {"metric:per_order", "agent:helper", "coco_desktop_profile:p"} <= downstream
    assert "model:stg_orders" not in downstream
    both = graph.walk(("metric:revenue",), GraphSelector("metric:revenue", upstream=1, downstream=1))
    assert both == {"metric:revenue", "model:orders", "metric:per_order", "semantic_view:sales"}
    assert graph.walk(("nope",), GraphSelector("nope")) == frozenset()


def test_dbt_s_graph_alone_keys_each_node_by_its_resource_type() -> None:
    graph = dbt_lineage(
        (
            DbtNode("seed.p.codes", "seed", "Codes", relation="DB.S.CODES"),
            DbtNode("model.p.orders", "model", "orders", depends_on=("seed.p.codes", "model.p.gone")),
            DbtNode("test.p.t", "test", "t", depends_on=("model.p.orders",)),
            DbtNode("analysis.p.a", "analysis", "a"),
        )
    )
    assert sorted(graph.nodes) == ["model:orders", "seed:codes", "test:t"]
    assert (graph.nodes["seed:codes"].kind, graph.parents["model:orders"]) == ("seed", ("seed:codes",))
    assert graph.nodes["model:orders"].marker is None
    assert node_key("snapshot", "Hist") == "snapshot:hist"
    assert LineageGraph().reach(("x",), upstream=False) == {}


@pytest.mark.parametrize(
    ("value", "parsed"),
    [
        ("orders", None),
        ("+orders", GraphSelector("orders", None, 0)),
        ("orders+", GraphSelector("orders", 0, None)),
        ("2+model:orders+3", GraphSelector("model:orders", 2, 3)),
        ("+type:agent+", GraphSelector("type:agent", None, None)),
    ],
)
def test_dbt_s_graph_operators_parse_and_write_back(value: str, parsed: GraphSelector | None) -> None:
    assert parse_graph_selector(value) == parsed
    assert parsed is None or parsed.text == value


@pytest.mark.parametrize("value", ["or+ders", "++orders", "+", "orders++"])
def test_a_misplaced_graph_operator_is_refused(value: str) -> None:
    with pytest.raises(ValueError, match="before or after one selector"):
        parse_graph_selector(value)


def test_each_edge_records_how_its_reader_uses_what_it_reads() -> None:
    graph = manifest_lineage(lineage_manifest())
    assert graph.kind("model:stg_orders", "model:orders") == "builds"
    assert graph.kind("model:orders", "test:unique") == "tests"
    assert graph.kind("model:orders", "exposure:board") == "exposes"
    assert graph.kind("model:orders", "metric:revenue") == "reads"
    assert graph.kind("model:orders", "semantic_view:sales") == "reads"
    assert graph.kind("metric:revenue", "metric:per_order") == "uses"
    assert graph.kind("metric:revenue", "semantic_view:sales") == "attaches"
    assert graph.kind("tool:status", "agent:helper") == "calls"
    assert graph.kind("tool_ref:partner", "agent:helper") == "calls"
    assert graph.kind("semantic_view:sales", "agent:helper") == "calls"
    # No phrase is recorded for a profile reading an agent, and none for a pair with no edge.
    assert graph.kind("agent:helper", "coco_desktop_profile:p") == "depends"
    assert graph.kind("model:orders", "agent:helper") == "depends"
    assert set(graph.kinds.values()) <= set(EDGE_KINDS)


_PROFILE = "cowork_profile:everyone"


def test_a_cowork_profile_calls_the_agents_it_routes_to_and_loads_its_skills() -> None:
    # CoWork profiles are a Snowflake private preview; another team's agent arrives as a tool_ref:.
    document = lineage_manifest()
    profile = ArtifactEntry(
        "cowork_profile",
        "everyone",
        "f" * 64,
        (),
        (),
        ("agent:helper", "skill:notes"),
        "SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE",
        1,
        "AGENT",
        references=("tool_ref:partner",),
    )
    skill = ArtifactEntry("skill", "notes", "f" * 64, (), (), (), "DB.S.NOTES", 1, "CORTEX EXTENSION")
    # A profile's eval tests the profile, as an agent's eval tests its agent.
    evaluation = ArtifactEntry("eval", "cowork_profile:everyone", "f" * 64, (), (), (_PROFILE,), "DB.S.EVAL", 1, "")
    artifacts = MappingProxyType(
        {**document.artifacts, _PROFILE: profile, "skill:notes": skill, "eval:cowork_profile:everyone": evaluation}
    )
    graph = manifest_lineage(dataclasses.replace(document, artifacts=artifacts))
    assert graph.parents[_PROFILE] == ("agent:helper", "skill:notes", "tool_ref:partner")
    assert graph.kind(_PROFILE, "eval:cowork_profile:everyone") == "tests"
    assert graph.kind("agent:helper", _PROFILE) == "calls"
    assert graph.kind("tool_ref:partner", _PROFILE) == "calls"
    assert graph.kind("skill:notes", _PROFILE) == "loads"
    assert set(graph.kinds.values()) <= set(EDGE_KINDS)


def test_a_column_a_member_reads_sits_between_its_model_and_the_member() -> None:
    document = lineage_manifest()
    revenue = document.members["metric:revenue"]
    assert isinstance(revenue, dict)
    members = {**document.members, "metric:revenue": {**revenue, "columns": ["orders.total"]}}
    graph = manifest_lineage(dataclasses.replace(document, members=members))
    assert graph.nodes["column:orders.total"].kind == "column"
    assert graph.kind("model:orders", "column:orders.total") == "contains"
    assert graph.kind("column:orders.total", "metric:revenue") == "reads"
