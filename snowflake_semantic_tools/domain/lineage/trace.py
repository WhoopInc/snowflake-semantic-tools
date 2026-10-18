"""Trace one node of lineage: what it reads, what reads it, and what changed since a manifest.

`find_nodes` reads the NODE `sst lineage` is given, `trace` walks from it, and `changed_nodes`
compares two manifests: an artifact changed when its fingerprint did, and a member when one of
the fingerprints its views record of it did. dbt's nodes carry no fingerprint, so none is marked.
"""

from __future__ import annotations

import difflib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase

from snowflake_semantic_tools.domain.lineage.graph import DBT_KINDS, MEMBER_KINDS, LineageGraph, LineageNode
from snowflake_semantic_tools.domain.state import Manifest

SELF, UPSTREAM, DOWNSTREAM = "self", "upstream", "downstream"
# The definition kinds a view's member fingerprints are keyed by whose name lineage keys by the
# member's own name, without the table a fact or dimension is qualified by.
_UNQUALIFIED = frozenset(("metric", "filter", "relationship", "verified_query"))


@dataclass(frozen=True, slots=True)
class TracedNode:
    """One node a trace reached.

    Attributes:
        direction: `self` for a node the trace started from, else `upstream` or `downstream`.
        depth: How many steps from the nearest start; 0 for a start.
        changed: It changed since the manifest the trace compares with.
    """

    node: LineageNode
    direction: str
    depth: int
    changed: bool = False


@dataclass(frozen=True, slots=True)
class Trace:
    """What a trace reached, starts first, then upstream and downstream by depth and key.

    Attributes:
        edges: Each edge of the graph between two nodes the trace holds, as `(reads, read by)`,
            sorted.
    """

    nodes: tuple[TracedNode, ...]
    edges: tuple[tuple[str, str], ...]


def find_nodes(graph: LineageGraph, text: str, artifact_types: Iterable[str]) -> tuple[str, ...]:
    """Return the keys of the nodes `text` names, sorted; none when it names none.

    A `<kind>:<name>` of an artifact type, a dbt kind or a member kind names the node of that key,
    its name globbable; any other text names every node of that name.
    """
    kind, separator, _ = text.partition(":")
    if separator and kind.casefold() in (*artifact_types, *DBT_KINDS, *MEMBER_KINDS):
        pattern = text.casefold()
        return tuple(key for key in graph.nodes if fnmatchcase(key, pattern))
    name = text.casefold()
    return tuple(key for key, node in graph.nodes.items() if fnmatchcase(node.name, name))


def near_miss(graph: LineageGraph, text: str) -> str | None:
    """Return the key of the node `text` most nearly names, for a refusal to suggest; None for none close."""
    found = difflib.get_close_matches(text.casefold(), list(graph.nodes), n=1, cutoff=0.6)
    return found[0] if found else None


def trace(
    graph: LineageGraph,
    start: Iterable[str],
    *,
    upstream: bool = True,
    downstream: bool = True,
    depth: int | None = None,
    changed: frozenset[str] = frozenset(),
) -> Trace:
    """Walk from `start` each way asked, at most `depth` steps; a node reached both ways is listed twice.

    Args:
        changed: The keys to mark changed, as `changed_nodes` finds them.
    """
    starts = sorted(key for key in set(start) if key in graph.nodes)
    found: list[TracedNode] = [TracedNode(graph.nodes[key], SELF, 0, key in changed) for key in starts]
    for direction, wanted in ((UPSTREAM, upstream), (DOWNSTREAM, downstream)):
        if not wanted:
            continue
        reached = graph.reach(starts, upstream=direction == UPSTREAM, depth=depth)
        found.extend(
            TracedNode(graph.nodes[key], direction, steps, key in changed)
            for key, steps in sorted(reached.items(), key=lambda item: (item[1], item[0]))
            if key not in starts
        )
    held = {item.node.key for item in found}
    edges = tuple((parent, key) for key in sorted(held) for parent in graph.parents.get(key, ()) if parent in held)
    return Trace(tuple(found), edges)


def member_node(definition_key: str) -> str | None:
    """Return the lineage key of the member a view's fingerprint key names; None for the view's own part.

    A metric, filter, relationship or verified query is keyed by its own name; a fact or
    dimension by its table and name, as lineage keys it. A table, a constraint, an instruction
    or the view's comment is part of the view itself.
    """
    kind, _, name = definition_key.partition(":")
    if kind in _UNQUALIFIED:
        return f"{kind}:{name.rsplit('.', 1)[-1]}"
    if kind in ("fact", "dimension"):
        return definition_key
    return None


def changed_nodes(previous: Manifest, current: Manifest) -> frozenset[str]:
    """Return the artifacts and members that changed from `previous` to `current`.

    An artifact changed when `previous` lacks it or records another fingerprint. A member changed
    when a view's fingerprint of it is new or differs; one the previous manifest recorded no
    member fingerprints for is never marked, as nothing says what it was.
    """
    changed = {
        key
        for key, entry in current.artifacts.items()
        if key not in previous.artifacts or previous.artifacts[key].fingerprint != entry.fingerprint
    }
    for key, entry in current.artifacts.items():
        before = previous.artifacts.get(key)
        if before is None or not before.member_fingerprints:
            continue
        old = _by_member(before.member_fingerprints)
        for member, fingerprints in _by_member(entry.member_fingerprints).items():
            if old.get(member) != fingerprints:
                changed.add(member)
    return frozenset(changed)


def _by_member(fingerprints: Iterable[tuple[str, str]]) -> Mapping[str, tuple[tuple[str, str], ...]]:
    """Group a view's member fingerprints by the lineage member they belong to."""
    grouped: dict[str, list[tuple[str, str]]] = {}
    for key, fingerprint in fingerprints:
        member = member_node(key)
        if member is not None:
            grouped.setdefault(member, []).append((key, fingerprint))
    return {member: tuple(sorted(items)) for member, items in grouped.items()}
