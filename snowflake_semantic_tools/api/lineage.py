"""What one node of a project's lineage reads and what reads it, as `sst lineage --output json` says.

`ProjectSession.lineage` traces the graph of the manifest the session's compile publishes, as
`sst lineage --no-manifest` does, and returns it as these records; nothing is written.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.lineage.graph import LineageGraph
from snowflake_semantic_tools.domain.lineage.trace import Trace


@dataclass(frozen=True, slots=True)
class LineageNode:
    """One node a trace reached.

    Attributes:
        key: How a selector names it: an artifact key, a member key such as `metric:<name>`, or
            a dbt node such as `model:<name>`.
        kind: What it is: an artifact type, a member kind, or a dbt resource type such as `seed`.
        name: Its name as the graph keys it, casefolded.
        direction: `self` for a node the trace started from, else `upstream` or `downstream`.
        depth: How many steps it is from the nearest start; 0 for a start.
        relation: The relation a dbt node builds or reads, or the object an artifact publishes
            to; None for a member, an exposure, or a dbt node with none.
        marker: Why a walk ends at it, such as a raw source; None when nothing does.
    """

    key: str
    kind: str
    name: str
    direction: str
    depth: int
    relation: str | None
    marker: str | None


@dataclass(frozen=True, slots=True)
class LineageEdge:
    """One edge a trace walked: `source` is read by `target`.

    Attributes:
        source: The key of the node read.
        target: The key of the node that reads it.
        kind: How it reads it, a key of the lineage graph's edge kinds, such as `builds`.
    """

    source: str
    target: str
    kind: str


@dataclass(frozen=True, slots=True)
class Lineage:
    """A trace of one node: every node it reached and every edge it walked, in trace order.

    Attributes:
        node: The node asked for, as written.
        start: The keys it names, which the trace started from; a bare name may name several.
        nodes: Each node the trace reached, the starts first.
        edges: Each edge the trace walked.
    """

    node: str
    start: tuple[str, ...]
    nodes: tuple[LineageNode, ...]
    edges: tuple[LineageEdge, ...]


def lineage_of(graph: LineageGraph, node: str, start: tuple[str, ...], traced: Trace) -> Lineage:
    """Return a trace as records, in the order and with the fields `sst lineage --output json` lists."""
    return Lineage(
        node=node,
        start=start,
        nodes=tuple(
            LineageNode(
                key=item.node.key,
                kind=item.node.kind,
                name=item.node.name,
                direction=item.direction,
                depth=item.depth,
                relation=item.node.relation,
                marker=item.node.marker,
            )
            for item in traced.nodes
        ),
        edges=tuple(
            LineageEdge(source=parent, target=child, kind=graph.kind(parent, child)) for parent, child in traced.edges
        ),
    )
