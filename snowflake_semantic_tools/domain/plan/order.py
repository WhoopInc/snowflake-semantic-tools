"""Order a plan's changes: dependencies first, then DDL position, then key.

`render_order` is the order plan classifies rendered artifacts in, before any dependency is
known. `topological_order` orders the changes of a plan, and `dependency_waves` groups them
for apply, which starts a wave only once the waves before it finish. Both ignore a
dependency on a key outside the changes: it is not this plan's to order.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, RenderedArtifact
from snowflake_semantic_tools.domain.model.registry import Registry


def render_order(artifact: RenderedArtifact, registry: Registry) -> tuple[int, str]:
    """Return the key plan classifies rendered artifacts by: DDL position, then artifact key.

    Raises:
        KeyError: the registry has no entry for the artifact's type.
    """
    return registry.artifacts[artifact.artifact_type].ddl_position, artifact.key


def topological_order(changes: tuple[Change, ...]) -> tuple[tuple[Change, ...], tuple[str, ...]]:
    """Order changes so each follows the changes it depends on, or name the cycle that prevents it.

    Each pass places every change whose dependencies are already placed, sorted by DDL
    position and then key. A prune sorts by its position negated: positions being positive,
    prunes come first, highest position first, so an object drops before what it was built on.

    Returns:
        The ordered changes and an empty cycle; or no changes and the keys around the cycle,
        its first key repeated at the end.
    """
    by_key = {change.key: change for change in changes}
    dependencies = {
        key: {dependency for dependency in change.depends_on if dependency in by_key} for key, change in by_key.items()
    }
    ordered: list[Change] = []
    remaining = set(by_key)
    while remaining:
        ready = sorted(
            (key for key in remaining if not dependencies[key].intersection(remaining)),
            key=lambda key: (
                -by_key[key].order if by_key[key].action is Action.PRUNE else by_key[key].order,
                key,
            ),
        )
        if not ready:
            cycle = _cycle_path(dependencies, remaining)
            return (), cycle
        ordered.extend(by_key[key] for key in ready)
        remaining.difference_update(ready)
    return tuple(ordered), ()


def _cycle_path(dependencies: Mapping[str, set[str]], remaining: set[str]) -> tuple[str, ...]:
    """Follow each key's smallest remaining dependency from the smallest key until a key repeats.

    Every remaining key has a remaining dependency, or it would have been placed, so the
    walk always closes a loop; the path returned is that loop alone.
    """
    start = min(remaining)
    path: list[str] = []
    seen_at: dict[str, int] = {}
    current = start
    while current not in seen_at:
        seen_at[current] = len(path)
        path.append(current)
        candidates = sorted(dependencies[current].intersection(remaining))
        assert candidates, "cycle search reached a node with no remaining dependency"
        current = candidates[0]
    return tuple(path[seen_at[current] :] + [current])


def dependency_waves(changes: tuple[Change, ...]) -> tuple[tuple[Change, ...], ...]:
    """Group changes into waves, each depending only on changes in the waves before it.

    A wave lists its changes by key.

    Raises:
        ValueError: the changes' dependencies form a cycle.
    """
    by_key = {change.key: change for change in changes}
    remaining = set(by_key)
    waves: list[tuple[Change, ...]] = []
    while remaining:
        ready = tuple(
            by_key[key] for key in sorted(remaining) if not set(by_key[key].depends_on).intersection(remaining)
        )
        if not ready:
            raise ValueError("dependency cycle")
        waves.append(ready)
        remaining.difference_update(change.key for change in ready)
    return tuple(waves)
