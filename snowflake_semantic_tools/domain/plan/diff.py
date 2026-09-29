"""Pure diff, prune, and dependency-order logic."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import Mapping

from ..model.diagnostic import D, Diagnostic, DiagnosticBag
from ..model.identifier import QualifiedName, TargetIdentity
from ..model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    ChangeSet,
    CompositePlan,
    ObservedArtifact,
    RenderedArtifact,
    SnowflakeObservation,
)
from ..model.registry import Registry
from ..state.model import Manifest, State, content_hash


def build_changeset(
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    manifest: Manifest,
    state: State,
    registry: Registry,
    target: TargetIdentity,
    *,
    blocked: Mapping[str, DiagnosticBag] | None = None,
    include_prune: bool = False,
    full: bool = True,
    prune_types: frozenset[str] | None = None,
    prune_keys: frozenset[str] | None = None,
    composite_plans: Mapping[str, CompositePlan] | None = None,
) -> ChangeSet:
    blocked = blocked or MappingProxyType({})
    composite_plans = composite_plans or MappingProxyType({})
    diagnostics: list[Diagnostic] = []
    changes: list[Change] = []
    if state.manifest_id and state.manifest_id != manifest.manifest_id:
        diagnostics.append(D("SST-MAN021", found=state.manifest_id, expected=manifest.manifest_id))

    for key, artifact in sorted(rendered.items(), key=lambda item: _render_order(item[1], registry)):
        artifact_type = registry.artifacts[artifact.artifact_type]
        artifact_diagnostics = blocked.get(key, DiagnosticBag())
        observed = observation.artifacts.get(key)
        if artifact_diagnostics.has_errors:
            changes.append(
                Change(
                    key,
                    artifact.artifact_type,
                    Action.BLOCKED,
                    ChangeReason.VALIDATION_ERRORS,
                    artifact,
                    observed,
                    artifact.depends_on,
                    artifact_type.ddl_position,
                    artifact_diagnostics,
                )
            )
            continue
        composite_plan = composite_plans.get(key)
        if composite_plan is not None:
            changes.append(
                Change(
                    key,
                    artifact.artifact_type,
                    composite_plan.action,
                    composite_plan.reason,
                    artifact,
                    None,
                    artifact.depends_on,
                    artifact_type.ddl_position,
                    composite_plan.diagnostics,
                    composite_observation=composite_plan.observation,
                )
            )
            diagnostics.extend(composite_plan.diagnostics)
            continue
        if observed is None:
            changes.append(_change(artifact, None, Action.CREATE, ChangeReason.NOT_PRESENT, registry))
            continue
        expected_object_type = artifact.object_type or artifact_type.object_type
        if observed.object_type.upper() != expected_object_type.upper():
            diagnostic = D(
                "SST-PLN002",
                artifact=key,
                value=observed.qualified_name.sql,
                found=observed.object_type,
            )
            diagnostics.append(diagnostic)
            changes.append(
                _change(
                    artifact,
                    observed,
                    Action.BLOCKED,
                    ChangeReason.VALIDATION_ERRORS,
                    registry,
                    DiagnosticBag((diagnostic,)),
                )
            )
            continue
        if observed.raw_name != artifact.target.name.folded and not artifact.target.name.quoted:
            diagnostics.append(
                D(
                    "SST-PLN023",
                    artifact=key,
                    value=artifact.target.name.value,
                    found=observed.raw_name,
                )
            )
        recorded = state.applied.get(key)
        if recorded is None:
            diagnostic = D(
                "SST-PLN024",
                artifact=key,
                value=observed.qualified_name.sql,
            )
            diagnostics.append(diagnostic)
            change = _change(
                artifact,
                observed,
                Action.BLOCKED,
                ChangeReason.UNMANAGED_OBJECT,
                registry,
                DiagnosticBag((diagnostic,)),
            )
        elif QualifiedName.parse(recorded.qualified_name).folded != artifact.target.folded:
            diagnostic = D(
                "SST-PLN025",
                artifact=key,
                found=recorded.qualified_name,
                expected=artifact.target.sql,
            )
            diagnostics.append(diagnostic)
            change = _change(
                artifact,
                observed,
                Action.BLOCKED,
                ChangeReason.TARGET_MOVED,
                registry,
                DiagnosticBag((diagnostic,)),
            )
        elif (
            QualifiedName.parse(recorded.qualified_name).folded != observed.qualified_name.folded
            or observed.marker is None
            or observed.marker.manifest_id != recorded.manifest_id
            or observed.marker.fingerprint != recorded.fingerprint
        ):
            diagnostics.append(
                D(
                    "SST-PLN014",
                    artifact=key,
                )
            )
            change = _change(
                artifact,
                observed,
                Action.BLOCKED,
                ChangeReason.VALIDATION_ERRORS,
                registry,
                DiagnosticBag((diagnostics[-1],)),
            )
        elif recorded.manifest_id != manifest.manifest_id:
            change = _change(
                artifact,
                observed,
                Action.UPDATE,
                ChangeReason.STATE_MANIFEST_MISMATCH,
                registry,
            )
        elif recorded.fingerprint != artifact.fingerprint:
            change = _change(
                artifact,
                observed,
                Action.UPDATE,
                ChangeReason.FINGERPRINT_DIFFERS,
                registry,
            )
        else:
            change = _change(artifact, observed, Action.NOOP, ChangeReason.UNCHANGED, registry)
        changes.append(change)
        if change.action is Action.UPDATE and observed.explicit_grants:
            diagnostics.append(
                D(
                    "SST-PLN013",
                    artifact=key,
                    count=len(observed.explicit_grants),
                    value=observed.qualified_name.sql,
                )
            )

    if include_prune:
        changes.extend(_prunes(rendered, observation, state, registry, diagnostics, prune_types, prune_keys))

    planned = {change.key for change in changes}
    changes = [_require_pinned(change, planned, registry, diagnostics) for change in changes]
    blocked_keys = {change.key for change in changes if change.action is Action.BLOCKED}
    changes = [_block_dependents(change, blocked_keys) for change in changes]
    ordered, cycle = topological_order(tuple(changes))
    if cycle:
        diagnostic = D("SST-PLN005", cycle=" -> ".join(cycle))
        diagnostics.append(diagnostic)
        return ChangeSet(
            manifest.manifest_id,
            target,
            (),
            DiagnosticBag(diagnostics),
            observation.fetched_at,
            full,
        )
    identity = {
        "manifest_id": manifest.manifest_id,
        "target": target.as_dict(),
        "observation_at": observation.fetched_at,
        "changes": [
            {
                "key": change.key,
                "action": change.action.value,
                "reason": change.reason.value,
                "fingerprint": change.rendered.fingerprint if change.rendered else None,
            }
            for change in ordered
        ],
    }
    return ChangeSet(
        manifest.manifest_id,
        target,
        ordered,
        DiagnosticBag(diagnostics),
        observation.fetched_at,
        full,
        content_hash(identity),
    )


def _render_order(artifact: RenderedArtifact, registry: Registry) -> tuple[int, str]:
    return registry.artifacts[artifact.artifact_type].ddl_position, artifact.key


def _change(
    artifact: RenderedArtifact,
    observed: ObservedArtifact | None,
    action: Action,
    reason: ChangeReason,
    registry: Registry,
    diagnostics: DiagnosticBag = DiagnosticBag(),
) -> Change:
    effective = artifact.for_action(action, observed)
    return Change(
        effective.key,
        effective.artifact_type,
        action,
        reason,
        effective,
        observed,
        effective.depends_on,
        registry.artifacts[effective.artifact_type].ddl_position,
        diagnostics,
    )


def _prunes(
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    state: State,
    registry: Registry,
    diagnostics: list[Diagnostic],
    prune_types: frozenset[str] | None,
    prune_keys: frozenset[str] | None,
) -> tuple[Change, ...]:
    changes: list[Change] = []
    for key, observed in sorted(observation.artifacts.items()):
        if key in rendered:
            continue
        artifact_type_name = key.split(":", 1)[0]
        if prune_keys is not None and key not in prune_keys:
            continue
        artifact_type = registry.artifacts.get(artifact_type_name)
        if prune_types is not None and artifact_type_name not in prune_types:
            continue
        if artifact_type is None or not artifact_type.prunable:
            continue
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
                artifact_type_name,
                Action.PRUNE,
                ChangeReason.ORPHANED,
                None,
                observed,
                (),
                artifact_type.ddl_position,
                DiagnosticBag(),
            )
        )
    return tuple(changes)


def _require_pinned(change: Change, planned: set[str], registry: Registry, diagnostics: list[Diagnostic]) -> Change:
    """Block a write that pins a version this plan neither observes nor publishes.

    A pinned dependency in the plan is either NOOP, which proves its version
    exists, or published first. One outside it -- left out by `--select` or
    `--exclude` -- is unverified, and Snowflake accepts a pin to a missing version.
    """
    if change.action not in (Action.CREATE, Action.UPDATE):
        return change
    pinned = registry.artifacts[change.artifact_type].pins_versions_of
    missing = [
        D("SST-PLN030", artifact=change.key, value=dependency)
        for dependency in change.depends_on
        if dependency.split(":", 1)[0] in pinned and dependency not in planned
    ]
    if not missing:
        return change
    diagnostics.extend(missing)
    return replace(
        change,
        action=Action.BLOCKED,
        reason=ChangeReason.DEPENDENCY_BLOCKED,
        diagnostics=DiagnosticBag((*change.diagnostics, *missing)),
    )


def _block_dependents(change: Change, blocked: set[str]) -> Change:
    if change.action is not Action.BLOCKED and blocked.intersection(change.depends_on):
        return replace(change, action=Action.BLOCKED, reason=ChangeReason.DEPENDENCY_BLOCKED)
    return change


def topological_order(changes: tuple[Change, ...]) -> tuple[tuple[Change, ...], tuple[str, ...]]:
    by_key = {change.key: change for change in changes}
    dependencies = {
        key: set(dependency for dependency in change.depends_on if dependency in by_key)
        for key, change in by_key.items()
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
