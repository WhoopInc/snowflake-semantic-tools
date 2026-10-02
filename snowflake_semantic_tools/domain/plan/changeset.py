"""Build a plan: one change per artifact, the unsafe ones blocked, all in dependency order.

`build_changeset` runs its phases in a fixed order, each reading what the ones before it
decided:

1. `classify` decides each rendered artifact, in DDL order;
2. `plan_prunes` adds the orphans SST may drop, when prune is asked for;
3. `block_unsafe` blocks a write that pins an unplanned version, and everything that depends on a
   blocked change -- after prune, so every change it checks against is decided;
4. `topological_order` orders the changes, whose decisions hash into the plan id.

The plan's diagnostics follow the same order, after the warning that state was recorded
against another manifest.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Change,
    ChangeSet,
    CompositePlan,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import Registry
from snowflake_semantic_tools.domain.plan.classify import classify
from snowflake_semantic_tools.domain.plan.order import render_order, topological_order
from snowflake_semantic_tools.domain.plan.prune import block_unsafe, plan_prunes
from snowflake_semantic_tools.domain.state import Manifest, State, content_hash


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
    """Decide one change per rendered artifact and order the changes into the plan apply runs.

    Plan writes nothing, and a change it cannot make safely stays in the plan as BLOCKED.

    Args:
        blocked: What validation reported for each artifact key; an error blocks the artifact.
        full: Whether the plan covers every artifact rather than a selection.
        prune_types: The types prune considers; None considers every prunable type.
        prune_keys: The keys prune considers; None considers every key.
        composite_plans: What each composite lifecycle handler planned, by artifact key.

    Returns:
        The changes in dependency order and the plan id hashed from them; on a dependency
        cycle, no changes and an empty plan id.

    Raises:
        KeyError: the registry has no entry for a rendered artifact's type.

    Diagnostics:
        SST-MAN021: state was recorded against another manifest.
        SST-PLN005: the changes' dependencies form a cycle.

    Each phase's own codes follow, in phase order: see `classify`, `plan_prunes`, and
    `block_unsafe`.
    """
    drift = _manifest_drift(manifest, state)
    changes, classified = _classify_all(rendered, observation, manifest, state, registry, blocked, composite_plans)
    pruned, skipped = (
        plan_prunes(rendered, observation, state, registry, prune_types, prune_keys) if include_prune else ((), ())
    )
    safe, unpinned = block_unsafe((*changes, *pruned), registry)
    return _ordered(safe, (*drift, *classified, *skipped, *unpinned), manifest, target, observation, full)


def _manifest_drift(manifest: Manifest, state: State) -> tuple[Diagnostic, ...]:
    if state.manifest_id and state.manifest_id != manifest.manifest_id:
        return (D("SST-MAN021", found=state.manifest_id, expected=manifest.manifest_id),)
    return ()


def _classify_all(
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    manifest: Manifest,
    state: State,
    registry: Registry,
    blocked: Mapping[str, DiagnosticBag] | None,
    composite_plans: Mapping[str, CompositePlan] | None,
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Classify every rendered artifact in DDL order, collecting the changes and what they reported."""
    validation: Mapping[str, DiagnosticBag] = blocked or MappingProxyType({})
    handled: Mapping[str, CompositePlan] = composite_plans or MappingProxyType({})
    changes: list[Change] = []
    diagnostics: list[Diagnostic] = []
    for key, artifact in sorted(rendered.items(), key=lambda item: render_order(item[1], registry)):
        decision = classify(
            key,
            artifact,
            registry,
            observed=observation.artifacts.get(key),
            recorded=state.applied.get(key),
            manifest_id=manifest.manifest_id,
            validation=validation.get(key, DiagnosticBag()),
            composite=handled.get(key),
        )
        changes.append(decision.change)
        diagnostics.extend(decision.diagnostics)
    return tuple(changes), tuple(diagnostics)


def _ordered(
    changes: tuple[Change, ...],
    diagnostics: tuple[Diagnostic, ...],
    manifest: Manifest,
    target: TargetIdentity,
    observation: SnowflakeObservation,
    full: bool,
) -> ChangeSet:
    """Order the changes by dependency into the plan; a cycle leaves it with no changes and no id."""
    ordered, cycle = topological_order(changes)
    if cycle:
        failed = DiagnosticBag((*diagnostics, D("SST-PLN005", cycle=" -> ".join(cycle))))
        return ChangeSet(manifest.manifest_id, target, (), failed, observation.fetched_at, full)
    plan_id = _plan_id(manifest, target, observation, ordered)
    return ChangeSet(
        manifest.manifest_id, target, ordered, DiagnosticBag(diagnostics), observation.fetched_at, full, plan_id
    )


def _plan_id(
    manifest: Manifest,
    target: TargetIdentity,
    observation: SnowflakeObservation,
    ordered: tuple[Change, ...],
) -> str:
    """Hash what identifies a plan: its manifest, target, observation time, and each decision."""
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
    return content_hash(identity)
