"""The lineage graph: dbt's nodes, SST's members and artifacts, and what reads what, offline.

Every node is keyed so a selector can name it: an artifact by its key (`semantic_view:sales`), a
member by its member key (`metric:total_revenue`), a dbt model, seed or snapshot as
`model:<name>`, a source as `source:<source>.<table>`, and an exposure as `exposure:<name>`, each
name casefolded. An edge runs from what is read to what reads it: a model to the models built
from it, to the members whose tables it holds and to the artifacts that read it; a member to the
members that use it and the views it attaches to; and an artifact to the artifacts that depend
on it or name it. dbt's tests are not lineage and are left out.

`parse_graph_selector` reads dbt's `+` operators: `+X` adds what X reads, `X+` what reads X, and
a number before or after the `+` bounds how many steps the walk takes.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.dbt import DbtNode
from snowflake_semantic_tools.domain.state.manifest import Manifest

# The member kinds a manifest keys its members by, so a selector can name one.
MEMBER_KINDS = ("custom_instruction", "dimension", "fact", "filter", "metric", "relationship", "verified_query")
# The dbt kinds a node key names, with the resource types each covers.
DBT_KINDS = ("model", "source", "exposure")
_RELATIONS = frozenset(("model", "seed", "snapshot"))
_GRAPH_SELECTOR = re.compile(r"(?:(\d*)\+)?([^+]+?)(?:\+(\d*))?")


@dataclass(frozen=True, slots=True)
class LineageNode:
    """One node of lineage.

    Attributes:
        key: How a selector names it, as the module says.
        kind: What it is: an artifact type, a member kind, or a dbt resource type such as `seed`.
        name: Its name as the graph keys it, casefolded.
        relation: The relation a dbt node builds or reads, or the object an artifact publishes
            to; None for a member, an exposure, or a dbt node with none.
        package: The dbt package that defines a dbt node; None for anything else.
    """

    key: str
    kind: str
    name: str
    relation: str | None = None
    package: str | None = None


@dataclass(frozen=True, slots=True)
class LineageGraph:
    """Lineage's nodes, by key, and each node's edges both ways.

    Attributes:
        parents: By key, the nodes it reads, sorted.
        children: By key, the nodes that read it, sorted.
    """

    nodes: Mapping[str, LineageNode] = field(default_factory=lambda: MappingProxyType({}))
    parents: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    children: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))

    def reach(self, start: Iterable[str], *, upstream: bool, depth: int | None = None) -> dict[str, int]:
        """Return each node a walk from `start` reaches, with the fewest steps it took; `start` at 0.

        Args:
            upstream: Walk to what the nodes read; else to what reads them.
            depth: The most steps to take; None walks as far as the graph goes.
        """
        edges = self.parents if upstream else self.children
        reached = {key: 0 for key in start if key in self.nodes}
        frontier = sorted(reached)
        steps = 0
        while frontier and (depth is None or steps < depth):
            steps += 1
            frontier = sorted({edge for key in frontier for edge in edges.get(key, ()) if edge not in reached})
            reached.update((key, steps) for key in frontier)
        return reached

    def walk(self, start: Iterable[str], selector: GraphSelector) -> frozenset[str]:
        """Return `start` with what the selector's operators reach from it, both ways."""
        origin = {key for key in start if key in self.nodes}
        found = set(origin)
        if selector.upstream != 0:
            found.update(self.reach(origin, upstream=True, depth=selector.upstream))
        if selector.downstream != 0:
            found.update(self.reach(origin, upstream=False, depth=selector.downstream))
        return frozenset(found)


@dataclass(frozen=True, slots=True)
class GraphSelector:
    """A selector with dbt's `+` operators: the selector they apply to, and how far each walks.

    Attributes:
        core: The selector the operators apply to, such as `model:orders`.
        upstream, downstream: How many steps to walk each way; 0 for none, None for all.
    """

    core: str
    upstream: int | None = 0
    downstream: int | None = 0

    @property
    def text(self) -> str:
        """Return the selector as `parse_graph_selector` reads it: `[N]+core[+N]`."""
        up = "" if self.upstream == 0 else f"{self.upstream or ''}+"
        down = "" if self.downstream == 0 else f"+{self.downstream or ''}"
        return f"{up}{self.core}{down}"


def parse_graph_selector(value: str) -> GraphSelector | None:
    """Return the operators a selector writes, or None when it writes none.

    Raises:
        ValueError: a `+` sits anywhere but first or last, or the selector is only operators.
    """
    if "+" not in value:
        return None
    found = _GRAPH_SELECTOR.fullmatch(value)
    if found is None:
        raise ValueError("a graph operator goes before or after one selector: +x, x+, 2+x or x+3")
    up, core, down = found.groups()
    return GraphSelector(core, _depth(up), _depth(down))


def _depth(written: str | None) -> int | None:
    """Return how far one operator walks: 0 without it, None when it writes no number."""
    if written is None:
        return 0
    return int(written) if written else None


def node_key(kind: str, name: str) -> str:
    """Return the key of a dbt node: `model:` for a model, seed or snapshot, else its kind."""
    return f"{'model' if kind in _RELATIONS else kind}:{name.casefold()}"


def dbt_lineage(nodes: Iterable[DbtNode]) -> LineageGraph:
    """Return the lineage of dbt's nodes alone, as a run with no SST artifacts reads it."""
    builder = _Builder()
    builder.add_dbt(
        (node.unique_id, node.kind, node.name, node.package, node.relation, node.depends_on) for node in nodes
    )
    return builder.graph()


def manifest_lineage(manifest: Manifest) -> LineageGraph:
    """Return the lineage a compiled manifest records: dbt's graph, its members and its artifacts.

    A manifest that records no dbt graph, such as one migrated from schema 4, still has a node for
    each dbt model or source an artifact reads, with no edge between them.
    """
    builder = _Builder()
    builder.add_dbt(_recorded_dbt(manifest.dbt_nodes))
    tables = {node.name: node.key for node in builder.nodes.values() if node.kind in (*_RELATIONS, "source")}

    def table(name: str) -> str:
        if name not in tables:
            kind = "source" if "." in name else "model"
            tables[name] = builder.node(LineageNode(f"{kind}:{name}", kind, name)).key
        return tables[name]

    for key, entry in manifest.artifacts.items():
        builder.node(LineageNode(key, entry.type, entry.name, entry.publish_target))
    for name, recorded in manifest.dbt_models.items():
        model = table(str(name).casefold())
        for artifact in _strings(recorded, "referenced_by"):
            builder.edge(model, artifact)
    for key, recorded in manifest.members.items():
        kind, _, name = key.partition(":")
        builder.node(LineageNode(key, kind, name))
        for model in _strings(recorded, "tables"):
            builder.edge(table(model), key)
        for used in _strings(recorded, "uses"):
            builder.edge(used, key)
        for view in _strings(recorded, "attached_to"):
            builder.edge(key, view)
    for key, entry in manifest.artifacts.items():
        for upstream in (*entry.depends_on, *entry.references):
            if upstream in manifest.artifacts:
                builder.edge(upstream, key)
    return builder.graph()


def _recorded_dbt(
    nodes: Mapping[str, object],
) -> Iterable[tuple[str, str, str, str | None, str | None, tuple[str, ...]]]:
    """Yield each node a manifest's `dbt_nodes` records, as `_Builder.add_dbt` reads one."""
    for unique_id, recorded in nodes.items():
        if not isinstance(recorded, Mapping):
            continue
        package, relation = recorded.get("package"), recorded.get("relation")
        yield (
            unique_id,
            str(recorded.get("kind") or ""),
            str(recorded.get("name") or ""),
            package if isinstance(package, str) else None,
            relation if isinstance(relation, str) else None,
            _strings(recorded, "depends_on"),
        )


def _strings(recorded: object, key: str) -> tuple[str, ...]:
    """Return the strings a manifest entry lists under `key`; none when it lists none."""
    listed = recorded.get(key) if isinstance(recorded, Mapping) else None
    return tuple(str(item) for item in listed) if isinstance(listed, list) else ()


@dataclass(slots=True)
class _Builder:
    """Collect nodes and edges, then freeze them into a graph; an edge to an unknown node is dropped."""

    nodes: dict[str, LineageNode] = field(default_factory=dict)
    edges: set[tuple[str, str]] = field(default_factory=set)

    def node(self, node: LineageNode) -> LineageNode:
        """Add `node` unless one of its key is there; return the one kept."""
        return self.nodes.setdefault(node.key, node)

    def edge(self, upstream: str, downstream: str) -> None:
        self.edges.add((upstream, downstream))

    def add_dbt(self, nodes: Iterable[tuple[str, str, str, str | None, str | None, tuple[str, ...]]]) -> None:
        """Add dbt's models, seeds, snapshots, sources and exposures, and the edges between them."""
        keys: dict[str, str] = {}
        depends: dict[str, tuple[str, ...]] = {}
        for unique_id, kind, name, package, relation, depends_on in sorted(nodes):
            if kind not in (*_RELATIONS, "source", "exposure"):
                continue
            node = self.node(LineageNode(node_key(kind, name), kind, name.casefold(), relation, package))
            keys[unique_id] = node.key
            depends[unique_id] = depends_on
        for unique_id, depends_on in depends.items():
            for upstream in depends_on:
                if upstream in keys:
                    self.edge(keys[upstream], keys[unique_id])

    def graph(self) -> LineageGraph:
        parents: dict[str, set[str]] = {}
        children: dict[str, set[str]] = {}
        for upstream, downstream in self.edges:
            if upstream in self.nodes and downstream in self.nodes and upstream != downstream:
                parents.setdefault(downstream, set()).add(upstream)
                children.setdefault(upstream, set()).add(downstream)
        return LineageGraph(
            MappingProxyType(dict(sorted(self.nodes.items()))),
            MappingProxyType({key: tuple(sorted(value)) for key, value in sorted(parents.items())}),
            MappingProxyType({key: tuple(sorted(value)) for key, value in sorted(children.items())}),
        )


# What a selector resolves a graph from: built only when a selector writes a graph operator.
GraphSource = Callable[[], LineageGraph]
