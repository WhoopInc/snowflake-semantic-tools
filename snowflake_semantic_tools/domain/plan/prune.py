"""Add the orphans plan may drop, and block the changes that are unsafe to run.

`plan_prunes` proposes dropping an object SST owns that nothing renders any more, and only
when its ownership marker and state agree it is exactly what apply created; it also counts,
per schema, the live objects the project does not declare. `block_unsafe`
then blocks a write that pins a version this plan neither observes nor publishes, and each
change that depends on a blocked one. A blocked change stays in the plan, so it is reported.
"""

from __future__ import annotations

from collections.abc import Container, Mapping
from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import ArtifactType, Registry
from snowflake_semantic_tools.domain.state import State


def plan_prunes(
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    state: State,
    registry: Registry,
    prune_types: frozenset[str] | None,
    prune_keys: frozenset[str] | None,
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Propose dropping each observed object of a prunable type that nothing renders any more.

    A candidate is pruned only when state recorded applying exactly the object observed:
    its marker's manifest and fingerprint, its DDL hash, and its name. Anything less is
    skipped and reported, never dropped.

    Args:
        prune_types: The artifact types to consider; None considers every prunable type.
        prune_keys: The artifact keys to consider; None considers every key.

    Returns:
        The PRUNE changes in key order, and the diagnostics: those for the candidates
        skipped, in key order, then one per schema holding a candidate, in schema order.

    Diagnostics:
        SST-PLN003: the candidate carries no SST ownership marker.
        SST-PLN004: state has no entry for the marked candidate, or its entry disagrees.
        SST-PLN021: a schema holds live objects the project does not declare. Names compare
            folded, so a declaration that differs from its object only by case is not one.
    """
    changes: list[Change] = []
    diagnostics: list[Diagnostic] = []
    undeclared: dict[str, int] = {}
    for key, observed in sorted(observation.artifacts.items()):
        artifact_type = _prunable_type(key, rendered, registry, prune_types, prune_keys)
        if artifact_type is None:
            continue
        scope = SchemaScope.from_qualified_name(observed.qualified_name).sql
        undeclared[scope] = undeclared.get(scope, 0) + 1
        if observed.marker is None:
            diagnostics.append(D("SST-PLN003", value=observed.qualified_name.sql))
            continue
        applied = state.applied.get(key)
        if (
            applied is None
            or applied.manifest_id != observed.marker.manifest_id
            or applied.fingerprint != observed.marker.fingerprint
            or applied.ddl_sha256 != observed.marker.fingerprint
            or QualifiedName.parse(applied.qualified_name).folded != observed.qualified_name.folded
        ):
            diagnostics.append(D("SST-PLN004", value=observed.qualified_name.sql))
            continue
        changes.append(
            Change(
                key,
                split_artifact_key(key)[0],
                Action.PRUNE,
                ChangeReason.ORPHANED,
                None,
                observed,
                (),
                artifact_type.ddl_position,
                DiagnosticBag(),
            )
        )
    reconciled = (D("SST-PLN021", count=count, value=scope) for scope, count in sorted(undeclared.items()))
    return tuple(changes), (*diagnostics, *reconciled)


def _prunable_type(
    key: str,
    rendered: Mapping[str, RenderedArtifact],
    registry: Registry,
    prune_types: frozenset[str] | None,
    prune_keys: frozenset[str] | None,
) -> ArtifactType | None:
    """Return the prunable type of an observed key prune may consider; None when it must skip it.

    A rendered key is no orphan, and a key outside the requested scope, of a type the
    registry does not know, or of a type that is never pruned is not a candidate.
    """
    if key in rendered:
        return None
    artifact_type_name = split_artifact_key(key)[0]
    if prune_keys is not None and key not in prune_keys:
        return None
    artifact_type = registry.artifacts.get(artifact_type_name)
    if prune_types is not None and artifact_type_name not in prune_types:
        return None
    if artifact_type is None or not artifact_type.prunable:
        return None
    return artifact_type


def block_unsafe(changes: tuple[Change, ...], registry: Registry) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Block each write that pins an unplanned version, then everything depending on a blocked one.

    Runs once every change is decided: a pinned version counts as planned only when a change
    for its key is in `changes`. Blocking follows the dependency graph all the way down, so a
    change that depends on a blocked one is blocked, and so is whatever depends on it.

    Returns:
        The changes in their given order, and the diagnostics for the unplanned pins.

    Diagnostics:
        SST-PLN030: a create or update pins the version of an artifact this plan leaves out.
    """
    planned = {change.key for change in changes}
    pinned: list[Change] = []
    diagnostics: list[Diagnostic] = []
    for change in changes:
        checked, missing = _require_pinned(change, planned, registry)
        pinned.append(checked)
        diagnostics.extend(missing)
    decided = tuple(pinned)
    blocked = {change.key for change in decided if change.action is Action.BLOCKED}
    while True:
        decided = tuple(_block_dependents(change, blocked) for change in decided)
        reached = {change.key for change in decided if change.action is Action.BLOCKED}
        if reached == blocked:
            return decided, tuple(diagnostics)
        blocked = reached


def _require_pinned(change: Change, planned: set[str], registry: Registry) -> tuple[Change, tuple[Diagnostic, ...]]:
    """Block a write that pins a version this plan neither observes nor publishes.

    A pinned dependency in the plan is either NOOP, which proves its version
    exists, or published first. One outside it -- left out by `--select` or
    `--exclude` -- is unverified, and Snowflake accepts a pin to a missing version.
    """
    if change.action not in (Action.CREATE, Action.UPDATE):
        return change, ()
    pinned = registry.artifacts[change.artifact_type].pins_versions_of
    missing = tuple(
        D("SST-PLN030", artifact=change.key, value=dependency)
        for dependency in change.depends_on
        if split_artifact_key(dependency)[0] in pinned and dependency not in planned
    )
    if not missing:
        return change, ()
    blocked = replace(
        change,
        action=Action.BLOCKED,
        reason=ChangeReason.DEPENDENCY_BLOCKED,
        diagnostics=DiagnosticBag((*change.diagnostics, *missing)),
    )
    return blocked, missing


def block_unpublished_dependencies(
    changes: tuple[Change, ...], observed: Container[str], compiled: Container[str], registry: Registry
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Block each write whose dependency the project compiles, this plan leaves out, and Snowflake lacks.

    The dependency was left out by `--select` or `--exclude` and does not exist yet, so the
    write would publish before what it depends on. A pinned version is SST-PLN030's, and a
    dependency the project does not compile is not this plan's to order.

    Diagnostics:
        SST-VAL015: once per such dependency of a create or update.
    """
    planned = {change.key for change in changes}
    decided: list[Change] = []
    diagnostics: list[Diagnostic] = []
    for change in changes:
        pinned = registry.artifacts[change.artifact_type].pins_versions_of
        missing = tuple(
            D(
                "SST-VAL015",
                subject=change.key,
                type=change.artifact_type,
                name=split_artifact_key(change.key)[1],
                blocker=dependency,
            )
            for dependency in change.depends_on
            if change.action in (Action.CREATE, Action.UPDATE)
            and dependency in compiled
            and dependency not in planned
            and dependency not in observed
            and split_artifact_key(dependency)[0] not in pinned
        )
        if missing:
            change = replace(
                change,
                action=Action.BLOCKED,
                reason=ChangeReason.DEPENDENCY_BLOCKED,
                diagnostics=DiagnosticBag((*change.diagnostics, *missing)),
            )
        decided.append(change)
        diagnostics.extend(missing)
    return tuple(decided), tuple(diagnostics)


def _block_dependents(change: Change, blocked: set[str]) -> Change:
    if change.action is not Action.BLOCKED and blocked.intersection(change.depends_on):
        return replace(change, action=Action.BLOCKED, reason=ChangeReason.DEPENDENCY_BLOCKED)
    return change
