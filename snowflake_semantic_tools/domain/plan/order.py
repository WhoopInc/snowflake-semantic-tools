"""Order a plan's changes: dependencies first, then DDL position, then key.

`render_order` is the order plan classifies rendered artifacts in, before any dependency is
known. `topological_order` orders the changes of a plan, `order_changes` wraps it with the
checks that report an order that cannot be computed or is wrong, and `dependency_waves`
groups the changes for apply, which starts a wave only once the waves before it finish. All
ignore a dependency on a key outside the changes: it is not this plan's to order.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
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


def order_changes(changes: tuple[Change, ...]) -> tuple[tuple[Change, ...], Diagnostic | None]:
    """Order changes as `topological_order` does, or report why no order can be computed.

    An order is computable only when each key names one change: two changes for one key,
    compared casefolded, would both address one object. The computed order is then checked
    again, so a defect in ordering is reported rather than applied.

    Returns:
        The ordered changes and None; or no changes and the diagnostic that stopped them.

    Diagnostics:
        SST-PLN005: the changes' dependencies form a cycle.
        SST-PLN022: more than one change carries the same key.
        SST-PLN900: the computed order places a change before one it depends on.
    """
    seen: dict[str, str] = {}
    for change in changes:
        folded = change.key.casefold()
        if folded in seen:
            return (), D("SST-PLN022", detail=f"'{seen[folded]}' and '{change.key}' are one artifact key")
        seen[folded] = change.key
    ordered, cycle = topological_order(changes)
    if cycle:
        return (), D("SST-PLN005", cycle=" -> ".join(cycle))
    violation = order_violation(ordered)
    if violation is not None:
        return (), D("SST-PLN900", value=violation)
    return ordered, None


def order_violation(ordered: tuple[Change, ...]) -> str | None:
    """Name the first change placed before a change in `ordered` it depends on; None when there is none."""
    position = {change.key: index for index, change in enumerate(ordered)}
    for index, change in enumerate(ordered):
        for dependency in change.depends_on:
            if position.get(dependency, -1) > index:
                return f"'{change.key}' before its dependency '{dependency}'"
    return None


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
