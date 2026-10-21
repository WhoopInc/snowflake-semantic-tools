"""The lineage graph: dbt's nodes, SST's members and artifacts, and what reads what, offline.

Every node is keyed so a selector can name it: an artifact by its key (`semantic_view:sales`), a
member by its member key (`metric:total_revenue`), a column a member reads as
`column:<table>.<column>` (its table a model's name or a source's `<source>.<table>`), a tool
member another team owns as `tool_ref:<name>`, and a dbt node by its resource type and name, as
`model:<name>`, `seed:`,
`snapshot:`, `test:` and `exposure:`, a source as `source:<source>.<table>`; each name casefolded.
An edge runs from what is read to what reads it: a model to the models built from it, its tests
and exposures, its columns, the members whose tables it holds and the artifacts that read it; a
column to the members that read it; a member to the members that use it and the views it
attaches to; and an artifact to the artifacts that depend on it or name it, an agent naming the
tool members it calls and a CoWork profile the agents it routes to.

Each edge records how the reader uses what it reads, as one of `EDGE_KINDS`: a model is built
from the nodes it selects from, a test tests and an exposure exposes its models, a member or an
artifact reads a model's table, a member uses another and attaches to its view, an agent loads
a skill and calls a view, a tool or another agent as a tool, a CoWork profile's router calls
each agent it routes to, an eval tests its agent, a plugin bundles a skill and a profile
includes a plugin or a skill; anything else depends on it.

Where a walk ends at someone else's work, the node says so in its `marker`: a source is a raw
source, a model of another dbt package that reads nothing here is defined in another project, a
`tool_ref:` is owned by another team, and a procedure or function that reads no model has a body
that names no `ref()`, so what it reads is unknown.

`parse_graph_selector` reads dbt's `+` operators: `+X` adds what X reads, `X+` what reads X, and
a number before or after the `+` bounds how many steps the walk takes.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.dbt import DbtNode
from snowflake_semantic_tools.domain.state.manifest import Manifest

# The member kinds a manifest keys its members by, so a selector can name one.
MEMBER_KINDS = ("custom_instruction", "dimension", "fact", "filter", "metric", "relationship", "verified_query")
# The dbt kinds a node key names, with the resource types each covers.
DBT_KINDS = ("model", "seed", "snapshot", "source", "test", "exposure")
_RELATIONS = frozenset(("model", "seed", "snapshot"))
# A tool member another team owns, which an agent calls but SST never publishes.
TOOL_REF = "tool_ref"
# A column of a model or source that a member reads.
COLUMN = "column"
RAW_SOURCE = "raw source"
OTHER_PROJECT = "defined in another project; upstream continues there"
OTHER_TEAM = "owned by another team"
UNKNOWN_READS = "reads: unknown; body names no ref()"
_ROUTINES = frozenset(("PROCEDURE", "FUNCTION"))
_GRAPH_SELECTOR = re.compile(r"(?:(\d*)\+)?([^+]+?)(?:\+(\d*))?")
# Each edge kind, as the reader's side says it and as the side it reads says it.
EDGE_KINDS: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "builds": ("built from it", "builds it"),
        "tests": ("tests it", "tested by it"),
        "exposes": ("exposes it", "exposed by it"),
        "reads": ("reads it", "read by it"),
        "uses": ("uses it", "used by it"),
        "contains": ("a column of it", "has it"),
        "attaches": ("holds it", "attaches to it"),
        "loads": ("loads it", "loaded by it"),
        "calls": ("calls it as a tool", "a tool it calls"),
        "bundles": ("bundles it", "bundled by it"),
        "includes": ("includes it", "included by it"),
        "depends": ("depends on it", "needed by it"),
    }
)
# What an artifact reading another artifact does with it, by (read type, reader type).
_ARTIFACT_EDGES: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        ("skill", "agent"): "loads",
        ("semantic_view", "agent"): "calls",
        ("tool", "agent"): "calls",
        ("agent", "agent"): "calls",
        (TOOL_REF, "agent"): "calls",
        ("agent", "eval"): "tests",
        # Snowflake private preview (PrPr): CoWork profiles. A profile's router tool calls each
        # agent it routes to, another team's through its `tool_ref:`, and it loads skills as an
        # agent does.
        ("agent", "cowork_profile"): "calls",
        (TOOL_REF, "cowork_profile"): "calls",
        ("skill", "cowork_profile"): "loads",
        ("skill", "plugin"): "bundles",
        ("plugin", "profile"): "includes",
        ("skill", "profile"): "includes",
    }
)


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
        marker: Why a walk ends here, as the module lists them; None for a node that says nothing.
    """

    key: str
    kind: str
    name: str
    relation: str | None = None
    package: str | None = None
    marker: str | None = None


@dataclass(frozen=True, slots=True)
class LineageGraph:
    """Lineage's nodes, by key, and each node's edges both ways.

    Attributes:
        parents: By key, the nodes it reads, sorted.
        children: By key, the nodes that read it, sorted.
        kinds: By `(read, reader)`, how the reader uses what it reads: a key of `EDGE_KINDS`.
    """

    nodes: Mapping[str, LineageNode] = field(default_factory=lambda: MappingProxyType({}))
    parents: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    children: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    kinds: Mapping[tuple[str, str], str] = field(default_factory=lambda: MappingProxyType({}))

    def kind(self, read: str, reader: str) -> str:
        """Return how `reader` uses `read`, a key of `EDGE_KINDS`; `depends` for an edge none was recorded for."""
        return self.kinds.get((read, reader), "depends")

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
    """Return the key of a dbt node: its resource type and its name, casefolded."""
    return f"{kind}:{name.casefold()}"


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
    read: set[str] = set()

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
            builder.edge(model, artifact, "reads")
            read.add(artifact)
    for key, recorded in manifest.members.items():
        kind, _, name = key.partition(":")
        builder.node(LineageNode(key, kind, name))
        for model in _strings(recorded, "tables"):
            builder.edge(table(model), key, "reads")
        for column in _strings(recorded, "columns"):
            column_key = builder.node(LineageNode(f"{COLUMN}:{column}", COLUMN, column)).key
            builder.edge(table(column.rsplit(".", 1)[0]), column_key, "contains")
            builder.edge(column_key, key, "reads")
        for used in _strings(recorded, "uses"):
            builder.edge(used, key, "uses")
        for view in _strings(recorded, "attached_to"):
            builder.edge(key, view, "attaches")
    for key, entry in manifest.artifacts.items():
        for upstream in (*entry.depends_on, *entry.references):
            if upstream.startswith(f"{TOOL_REF}:"):
                name = upstream.partition(":")[2]
                builder.node(LineageNode(upstream, TOOL_REF, name, marker=OTHER_TEAM))
            if upstream in manifest.artifacts or upstream.startswith(f"{TOOL_REF}:"):
                read_type = manifest.artifacts[upstream].type if upstream in manifest.artifacts else TOOL_REF
                builder.edge(upstream, key, _ARTIFACT_EDGES.get((read_type, entry.type), "depends"))
    project = str(manifest.project.get("dbt_project_name") or "")
    return builder.graph(lambda node, parents: _marker(node, parents, read=read, project=project, manifest=manifest))


def _marker(
    node: LineageNode, parents: tuple[str, ...], *, read: set[str], project: str, manifest: Manifest
) -> str | None:
    """Say why a walk ends at `node`, as the module lists the ends; None for a node that is no end."""
    if node.marker is not None:
        return node.marker
    if node.kind == "source":
        return RAW_SOURCE
    if node.kind in _RELATIONS and not parents and project and node.package not in (None, project):
        return OTHER_PROJECT
    entry = manifest.artifacts.get(node.key)
    if entry is not None and entry.object_type in _ROUTINES and node.key not in read:
        return UNKNOWN_READS
    return None


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
    edges: dict[tuple[str, str], str] = field(default_factory=dict)

    def node(self, node: LineageNode) -> LineageNode:
        """Add `node` unless one of its key is there; return the one kept."""
        return self.nodes.setdefault(node.key, node)

    def edge(self, upstream: str, downstream: str, kind: str) -> None:
        """Add an edge from what is read to its reader; the first kind given an edge is kept."""
        self.edges.setdefault((upstream, downstream), kind)

    def add_dbt(self, nodes: Iterable[tuple[str, str, str, str | None, str | None, tuple[str, ...]]]) -> None:
        """Add dbt's models, seeds, snapshots, sources, tests and exposures, and the edges between them."""
        keys: dict[str, str] = {}
        depends: dict[str, tuple[str, ...]] = {}
        for unique_id, kind, name, package, relation, depends_on in sorted(nodes):
            if kind not in DBT_KINDS:
                continue
            node = self.node(LineageNode(node_key(kind, name), kind, name.casefold(), relation, package))
            keys[unique_id] = node.key
            depends[unique_id] = depends_on
        for unique_id, depends_on in depends.items():
            kind = self.nodes[keys[unique_id]].kind
            edge = "tests" if kind == "test" else "exposes" if kind == "exposure" else "builds"
            for upstream in depends_on:
                if upstream in keys:
                    self.edge(keys[upstream], keys[unique_id], edge)

    def graph(self, marker: Callable[[LineageNode, tuple[str, ...]], str | None] | None = None) -> LineageGraph:
        """Freeze the nodes and edges; `marker` says why a walk ends at a node, given its parents."""
        parents: dict[str, set[str]] = {}
        children: dict[str, set[str]] = {}
        kinds: dict[tuple[str, str], str] = {}
        for (upstream, downstream), kind in self.edges.items():
            if upstream in self.nodes and downstream in self.nodes and upstream != downstream:
                parents.setdefault(downstream, set()).add(upstream)
                children.setdefault(upstream, set()).add(downstream)
                kinds[(upstream, downstream)] = kind
        nodes = {
            key: replace(node, marker=marker(node, tuple(sorted(parents.get(key, ()))))) if marker is not None else node
            for key, node in sorted(self.nodes.items())
        }
        return LineageGraph(
            MappingProxyType(nodes),
            MappingProxyType({key: tuple(sorted(value)) for key, value in sorted(parents.items())}),
            MappingProxyType({key: tuple(sorted(value)) for key, value in sorted(children.items())}),
            MappingProxyType(dict(sorted(kinds.items()))),
        )


# What a selector resolves a graph from: built only when a selector writes a graph operator.
GraphSource = Callable[[], LineageGraph]
