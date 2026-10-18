"""A small compiled manifest with every kind of lineage edge, for the lineage graph's tests."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.state import ArtifactEntry, Manifest


def _entry(
    kind: str, name: str, *, depends_on: tuple[str, ...] = (), references: tuple[str, ...] = ()
) -> ArtifactEntry:
    return ArtifactEntry(kind, name, "f" * 64, (), (), depends_on, f"DB.S.{name.upper()}", 1, references=references)


def lineage_manifest(*, dbt_nodes: dict[str, object] | None = None) -> Manifest:
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
