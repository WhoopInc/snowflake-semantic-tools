from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.lineage.graph import (
    GraphSelector,
    LineageGraph,
    dbt_lineage,
    manifest_lineage,
    node_key,
    parse_graph_selector,
)
from snowflake_semantic_tools.domain.model.dbt import DbtNode
from snowflake_semantic_tools.domain.state import ArtifactEntry, Manifest


def _entry(
    kind: str, name: str, *, depends_on: tuple[str, ...] = (), references: tuple[str, ...] = ()
) -> ArtifactEntry:
    return ArtifactEntry(kind, name, "f" * 64, (), (), depends_on, f"DB.S.{name.upper()}", 1, references=references)


def _manifest(*, dbt_nodes: dict[str, object] | None = None) -> Manifest:
    return Manifest(
        schema_version=5,
        manifest_id="",
        generator=MappingProxyType({}),
        project=MappingProxyType({}),
        sources=MappingProxyType({}),
        artifacts=MappingProxyType(
            {
                "semantic_view:sales": _entry("semantic_view", "sales"),
                "tool:status": _entry("tool", "status"),
                "agent:helper": _entry(
                    "agent", "helper", depends_on=("semantic_view:sales", "tool:status"), references=("command:x",)
                ),
                "profile:p": _entry("profile", "p", references=("agent:helper",)),
            }
        ),
        members=MappingProxyType(
            {
                "metric:revenue": {"attached_to": ["semantic_view:sales"], "tables": ["orders"]},
                "metric:per_order": {"attached_to": ["semantic_view:sales"], "uses": ["metric:revenue"]},
                "dimension:events.kind": {"attached_to": ["semantic_view:sales"], "tables": ["raw.events"]},
            }
        ),
        dbt_models=MappingProxyType(
            {
                "orders": {"relation": "DB.S.ORDERS", "referenced_by": ["semantic_view:sales", "tool:status"]},
                "raw.events": {"relation": "RAW.APP.EVENTS", "referenced_by": ["semantic_view:sales"]},
            }
        ),
        files=MappingProxyType({}),
        diagnostics_summary=MappingProxyType({}),
        dbt_nodes=MappingProxyType(
            dbt_nodes
            if dbt_nodes is not None
            else {
                "model.p.orders": {
                    "kind": "model",
                    "name": "orders",
                    "package": "p",
                    "relation": "DB.S.ORDERS",
                    "depends_on": ["model.p.stg_orders"],
                },
                "model.p.stg_orders": {
                    "kind": "model",
                    "name": "stg_orders",
                    "package": "p",
                    "relation": None,
                    "depends_on": ["source.p.raw.orders", "model.p.stg_orders"],
                },
                "source.p.raw.orders": {"kind": "source", "name": "raw.orders", "relation": "RAW.APP.ORDERS"},
                "test.p.unique": {"kind": "test", "name": "unique", "depends_on": ["model.p.orders"]},
                "exposure.p.board": {"kind": "exposure", "name": "board", "depends_on": ["model.p.orders"]},
                "seed.p.broken": "not a node",
            }
        ),
    )


def test_a_manifest_s_lineage_runs_from_dbt_through_members_to_artifacts() -> None:
    graph = manifest_lineage(_manifest())
    assert "test:unique" not in graph.nodes
    assert graph.nodes["model:stg_orders"].relation is None
    assert graph.nodes["model:orders"].package == "p"
    assert graph.parents["model:orders"] == ("model:stg_orders",)
    assert graph.parents["model:stg_orders"] == ("source:raw.orders",)
    assert graph.children["model:orders"] == ("exposure:board", "metric:revenue", "semantic_view:sales", "tool:status")
    assert graph.parents["metric:per_order"] == ("metric:revenue",)
    assert graph.parents["dimension:events.kind"] == ("source:raw.events",)
    assert graph.parents["agent:helper"] == ("semantic_view:sales", "tool:status")
    assert graph.parents["profile:p"] == ("agent:helper",)
    assert graph.nodes["source:raw.events"].kind == "source"


def test_a_manifest_without_dbt_s_graph_still_has_the_models_artifacts_read() -> None:
    graph = manifest_lineage(_manifest(dbt_nodes={}))
    assert graph.nodes["model:orders"].kind == "model"
    assert "model:orders" not in graph.parents


def test_a_walk_goes_as_far_as_its_operators_say() -> None:
    graph = manifest_lineage(_manifest())
    assert graph.reach(("model:orders",), upstream=True) == {
        "model:orders": 0,
        "model:stg_orders": 1,
        "source:raw.orders": 2,
    }
    assert graph.reach(("model:orders", "nope"), upstream=True, depth=1) == {"model:orders": 0, "model:stg_orders": 1}
    downstream = graph.walk(("model:orders",), GraphSelector("model:orders", downstream=None))
    assert {"metric:per_order", "agent:helper", "profile:p"} <= downstream
    assert "model:stg_orders" not in downstream
    both = graph.walk(("metric:revenue",), GraphSelector("metric:revenue", upstream=1, downstream=1))
    assert both == {"metric:revenue", "model:orders", "metric:per_order", "semantic_view:sales"}
    assert graph.walk(("nope",), GraphSelector("nope")) == frozenset()


def test_dbt_s_graph_alone_keys_seeds_and_snapshots_as_models() -> None:
    graph = dbt_lineage(
        (
            DbtNode("seed.p.codes", "seed", "Codes", relation="DB.S.CODES"),
            DbtNode("model.p.orders", "model", "orders", depends_on=("seed.p.codes", "model.p.gone")),
            DbtNode("test.p.t", "test", "t", depends_on=("model.p.orders",)),
        )
    )
    assert sorted(graph.nodes) == ["model:codes", "model:orders"]
    assert (graph.nodes["model:codes"].kind, graph.parents["model:orders"]) == ("seed", ("model:codes",))
    assert node_key("snapshot", "Hist") == "model:hist"
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
