"""Build a plan: one change per artifact, the unsafe ones blocked, all in dependency order.

`build_changeset` runs its phases in a fixed order, each reading what the ones before it
decided:

1. `classify` decides each rendered artifact, in DDL order;
2. `plan_prunes` adds the orphans SST may drop, when prune is asked for;
3. `block_unpublished_dependencies` blocks a write whose dependency would not be published
   before it;
4. `check_preflight` blocks the writes the preflight read shows Snowflake would refuse, when
   plan read one;
5. `block_unsafe` blocks a write that pins an unplanned version, and everything that depends on a
   blocked change -- after prune and preflight, so every change it checks against is decided;
6. `order_changes` orders the changes, whose decisions hash into the plan id.

The plan's diagnostics follow the same order.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Change,
    ChangeSet,
    CompositePlan,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import Registry
from snowflake_semantic_tools.domain.plan.classify import classify
from snowflake_semantic_tools.domain.plan.order import order_changes, render_order
from snowflake_semantic_tools.domain.plan.preflight import Preflight, check_preflight, schemas_to_create
from snowflake_semantic_tools.domain.plan.prune import block_unpublished_dependencies, block_unsafe, plan_prunes
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
    preflight: Preflight | None = None,
    overwrite: bool = False,
) -> ChangeSet:
    """Decide one change per rendered artifact and order the changes into the plan apply runs.

    Plan writes nothing, and a change it cannot make safely stays in the plan as BLOCKED.

    Args:
        blocked: What validation reported for each artifact key; an error blocks the artifact.
        full: Whether the plan covers every artifact rather than a selection.
        prune_types: The types prune considers; None considers every prunable type.
        prune_keys: The keys prune considers; None considers every key.
        composite_plans: What each composite lifecycle handler planned, by artifact key.
        preflight: What plan read about the target before deciding; None skips the check.
        overwrite: Update each object changed out of band instead of blocking it, as `classify` does.

    Returns:
        The changes in dependency order and the plan id hashed from them; when no order can
        be computed, no changes and an empty plan id.

    Raises:
        KeyError: the registry has no entry for a rendered artifact's type.

    Diagnostics:
        Each phase's own codes, in phase order: see `classify`, `plan_prunes`,
        `block_unpublished_dependencies`, `check_preflight`, `block_unsafe`, and `order_changes`.
    """
    changes, classified = _classify_all(rendered, observation, state, registry, blocked, composite_plans, overwrite)
    pruned, skipped = (
        plan_prunes(rendered, observation, state, registry, prune_types, prune_keys) if include_prune else ((), ())
    )
    ordered_safely, unordered = block_unpublished_dependencies(
        changes, observation.artifacts, manifest.artifacts, registry
    )
    candidates = (*ordered_safely, *pruned)
    decided, preflighted = check_preflight(candidates, preflight) if preflight else (candidates, ())
    safe, unpinned = block_unsafe(decided, registry)
    diagnostics = (*classified, *skipped, *unordered, *preflighted, *unpinned)
    schemas = schemas_to_create(safe, preflight) if preflight else ()
    return _ordered(safe, diagnostics, manifest, target, observation, full, schemas)


def _classify_all(
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    state: State,
    registry: Registry,
    blocked: Mapping[str, DiagnosticBag] | None,
    composite_plans: Mapping[str, CompositePlan] | None,
    overwrite: bool = False,
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
            validation=validation.get(key, DiagnosticBag()),
            composite=handled.get(key),
            overwrite=overwrite,
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
    schemas: tuple[SchemaScope, ...] = (),
) -> ChangeSet:
    """Order the changes by dependency into the plan; without an order, it has no changes and no id."""
    ordered, unordered = order_changes(changes)
    if unordered is not None:
        failed = DiagnosticBag((*diagnostics, unordered))
        return ChangeSet(manifest.manifest_id, target, (), failed, observation.fetched_at, full)
    plan_id = _plan_id(manifest.manifest_id, target, observation.fetched_at, ordered, schemas)
    return ChangeSet(
        manifest.manifest_id,
        target,
        ordered,
        DiagnosticBag(diagnostics),
        observation.fetched_at,
        full,
        plan_id,
        schemas,
    )


def narrowed(changeset: ChangeSet, changes: tuple[Change, ...], diagnostics: DiagnosticBag) -> ChangeSet:
    """Return the plan with only `changes`, in its order, and `diagnostics`, its id hashed anew."""
    plan_id = _plan_id(changeset.manifest_id, changeset.target, changeset.observation_at, changes)
    return replace(changeset, changes=changes, diagnostics=diagnostics, plan_id=plan_id)


def _plan_id(
    manifest_id: str,
    target: TargetIdentity,
    observation_at: str,
    ordered: tuple[Change, ...],
    schemas: tuple[SchemaScope, ...] = (),
) -> str:
    """Hash what identifies a plan: its manifest, target, observation time, each decision, and its schemas.

    A plan that creates no schema hashes as it did before schemas were planned.
    """
    identity = {
        "manifest_id": manifest_id,
        "target": target.as_dict(),
        "observation_at": observation_at,
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
    if schemas:
        identity["schemas"] = [scope.sql for scope in schemas]
    return content_hash(identity)
