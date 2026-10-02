"""How deep `metric()` references nest, measured without recursion.

A metric that references another metric expands it, and that one may reference a third. The
expansion is bounded: a chain longer than `MAX_METRIC_DEPTH` hops is refused (SST-REF900)
rather than followed. A cycle is not a depth problem -- SST-REF005 reports it -- so a
reference back into the chain being measured counts as no further hop.
"""

from __future__ import annotations

from collections.abc import Mapping

MAX_METRIC_DEPTH = 32


def reference_depths(graph: Mapping[str, tuple[str, ...]]) -> dict[str, int]:
    """Return, for each metric in `graph`, the most `metric()` hops its expansion takes.

    The walk is iterative, so no chain is too long to measure. A reference to a name that is
    not in `graph` takes no hop: it does not resolve, which is another code's report.

    Args:
        graph: Each metric's name, mapped to the names of the metrics it references.
    """
    depths: dict[str, int] = {}
    on_path: set[str] = set()
    for root in sorted(graph):
        stack: list[tuple[str, int]] = [(root, 0)]
        while stack:
            name, index = stack.pop()
            if index == 0:
                if name in depths:
                    continue
                on_path.add(name)
            references = graph[name]
            if index < len(references):
                stack.append((name, index + 1))
                child = references[index]
                if child in graph and child not in depths and child not in on_path:
                    stack.append((child, 0))
                continue
            on_path.discard(name)
            depths[name] = max((depths[child] + 1 for child in references if child in depths), default=0)
    return depths


def over_deep(graph: Mapping[str, tuple[str, ...]], limit: int = MAX_METRIC_DEPTH) -> tuple[tuple[str, int], ...]:
    """Return each metric whose expansion takes more than `limit` hops, with its depth, by name."""
    return tuple((name, depth) for name, depth in sorted(reference_depths(graph).items()) if depth > limit)
