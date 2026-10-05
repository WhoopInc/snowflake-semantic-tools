"""Plan rendered artifacts against a target: read what it holds, then decide each change.

`PlanArtifacts.read` observes what `app.observe` finds in Snowflake and, given a preflight
port, what `app.preflight` reads about the target. `plan_changes` decides the change set from
such a `TargetReading` without reading anything, so a plan can be decided from a recorded
reading as well as from a live one. `PlanArtifacts.run` does both.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Protocol

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.observe import DEFAULT_OBSERVE_OPTIONS, ObserveOptions, observe
from snowflake_semantic_tools.app.preflight import read_preflight
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    ChangeSet,
    CompositeObservation,
    CompositePlan,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, ArtifactLifecycle, Registry
from snowflake_semantic_tools.domain.plan import build_changeset
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort
from snowflake_semantic_tools.domain.state import DEACTIVATED, Manifest, State


class PlanReadPort(SnowflakePort, PreflightPort, Protocol):
    """A session a plan reads on: validation's checks, observation, and the preflight reads."""


@dataclass(frozen=True, slots=True)
class TargetReading:
    """What a plan read of the target before deciding.

    Attributes:
        observation: Every object observed, keyed by artifact key.
        preflight: The preflight facts; None when no preflight reads were made.
        failures: The reads Snowflake refused, as SST-PLN001, and the advisory reads the role
            may not make, as SST-VAL020.
    """

    observation: SnowflakeObservation
    preflight: Preflight | None = None
    failures: DiagnosticBag = DiagnosticBag()


class PlanArtifacts:
    """Plan the changes that bring a target to the rendered artifacts, writing nothing.

    Composite artifacts are planned by their lifecycle handlers, keyed by artifact type; every
    other artifact is planned from what `observe` finds in Snowflake.

    Args:
        readers: Sessions, opened from `port` and `preflight`, to run the observation's
            listings and each phase of preflight reads on concurrently; None reads on `port`
            and `preflight`, one at a time. Either way the change set is the same.
        observe_options: Which per-object reads the observation makes, as `ObserveOptions` says.
    """

    def __init__(
        self,
        port: CatalogPort,
        *,
        registry: Registry = SEMANTIC_REGISTRY,
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
        preflight: PreflightPort | None = None,
        readers: Fanout[PlanReadPort] | None = None,
        observe_options: ObserveOptions = DEFAULT_OBSERVE_OPTIONS,
    ) -> None:
        self._port = port
        self._registry = registry
        self._lifecycle_handlers = dict(lifecycle_handlers or {})
        self._preflight = preflight
        self._readers = readers
        self._observe_options = observe_options

    def run(
        self,
        rendered: Mapping[str, RenderedArtifact],
        manifest: Manifest,
        state: State,
        target: TargetIdentity,
        *,
        fetched_at: str,
        blocked: Mapping[str, DiagnosticBag] | None = None,
        include_prune: bool = False,
        full: bool = True,
        prune_types: frozenset[str] | None = None,
        prune_keys: frozenset[str] | None = None,
        observation_targets: tuple[QualifiedName, ...] | None = None,
    ) -> ChangeSet:
        """Compute the change set for the rendered artifacts from what Snowflake shows now.

        Steps, in order:

        1. Composite plans: each composite artifact's handler plans it from its state entry.
        2. The reading of the target, as `read` makes it.
        3. The change set, as `plan_changes` decides it.

        Args:
            blocked: Diagnostics by artifact key that make its change BLOCKED.
            prune_types, prune_keys: Narrow the prunes to these artifact types and keys; None
                does not narrow.
            observation_targets: The objects whose schemas are observed; None observes the
                schemas of the rendered artifacts.

        Diagnostics:
            SST-PLN001: observing Snowflake, or a preflight read, failed.
        """
        composite_plans = self.composite_plans(rendered, manifest, state)
        reading = self.read(
            rendered,
            state,
            target,
            fetched_at=fetched_at,
            include_prune=include_prune,
            prune_types=prune_types,
            observation_targets=observation_targets,
        )
        return plan_changes(
            rendered,
            reading,
            manifest,
            state,
            target,
            registry=self._registry,
            lifecycle_handlers=self._lifecycle_handlers,
            composite_plans=composite_plans,
            blocked=blocked,
            include_prune=include_prune,
            full=full,
            prune_types=prune_types,
            prune_keys=prune_keys,
        )

    def composite_plans(
        self,
        rendered: Mapping[str, RenderedArtifact],
        manifest: Manifest,
        state: State,
    ) -> dict[str, CompositePlan]:
        """Ask each composite artifact's handler to plan it, in rendered order."""
        return {
            key: self._lifecycle_handlers[artifact.artifact_type].plan(
                artifact,
                state.applied.get(key),
                manifest,
            )
            for key, artifact in rendered.items()
            if artifact.artifact_type in self._lifecycle_handlers
        }

    def read(
        self,
        rendered: Mapping[str, RenderedArtifact],
        state: State,
        target: TargetIdentity,
        *,
        fetched_at: str,
        include_prune: bool = False,
        prune_types: frozenset[str] | None = None,
        observation_targets: tuple[QualifiedName, ...] | None = None,
    ) -> TargetReading:
        """Read the target for a plan: observe it, then preflight it when given a preflight port.

        The observation covers every type rendered and, when pruning, every type a prune may
        remove, in the schemas of `observation_targets`, else of the rendered artifacts. The
        preflight read is `read_preflight`'s.

        Diagnostics:
            SST-PLN001: observing Snowflake, or a preflight read, failed.
        """
        observation, failures = observe(
            self._port,
            self._registry,
            observation_targets or tuple(artifact.target for artifact in rendered.values()),
            fetched_at=fetched_at,
            artifact_types=self._observed_artifact_types(rendered, include_prune, prune_types),
            observed_object_types=self._observed_object_types(rendered),
            desired_artifacts=rendered,
            readers=self._readers,
            options=self._observe_options,
        )
        if self._preflight is None:
            return TargetReading(observation, None, failures)
        preflight, refused = read_preflight(
            self._preflight,
            rendered,
            observation,
            state,
            target,
            include_prune=include_prune,
            readers=self._readers,
        )
        return TargetReading(observation, preflight, DiagnosticBag((*failures, *refused)))

    def _observed_artifact_types(
        self,
        rendered: Mapping[str, RenderedArtifact],
        include_prune: bool,
        prune_types: frozenset[str] | None,
    ) -> frozenset[str]:
        """Return the types to observe: those rendered and, when pruning, those a prune may remove."""
        prunable: set[str] = set()
        if include_prune:
            prunable = set(prune_types) if prune_types is not None else set(self._registry.artifacts)
        return frozenset({artifact.artifact_type for artifact in rendered.values()} | prunable)

    def _observed_object_types(self, rendered: Mapping[str, RenderedArtifact]) -> dict[str, frozenset[str]]:
        """Return, for every registered type, the object types its rendered artifacts are published as."""
        return {
            artifact_type: frozenset(
                artifact.object_type
                for artifact in rendered.values()
                if artifact.artifact_type == artifact_type and artifact.object_type
            )
            for artifact_type in self._registry.artifacts
        }


def plan_changes(
    rendered: Mapping[str, RenderedArtifact],
    reading: TargetReading,
    manifest: Manifest,
    state: State,
    target: TargetIdentity,
    *,
    registry: Registry = SEMANTIC_REGISTRY,
    lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
    composite_plans: Mapping[str, CompositePlan] | None = None,
    blocked: Mapping[str, DiagnosticBag] | None = None,
    include_prune: bool = False,
    full: bool = True,
    prune_types: frozenset[str] | None = None,
    prune_keys: frozenset[str] | None = None,
) -> ChangeSet:
    """Decide the change set from a reading of the target, reading nothing more.

    Steps, in order:

    1. The change set, from the reading's observation and preflight, the manifest and state.
    2. Composite prunes, when pruning: a composite artifact state records, that nothing
       rendered or planned names, is reported by its handler as a prune.
    3. The reading's failures, ahead of every other diagnostic.

    Args:
        lifecycle_handlers: The composite artifact types' handlers, by type, which report the
            composite prunes; None reports none.
        composite_plans: Each composite artifact's plan, by key.
    """
    changeset: ChangeSet = build_changeset(
        rendered,
        reading.observation,
        manifest,
        state,
        registry,
        target,
        blocked=blocked,
        include_prune=include_prune,
        full=full,
        prune_types=prune_types,
        prune_keys=prune_keys,
        composite_plans=dict(composite_plans or {}),
        preflight=reading.preflight,
    )
    if include_prune:
        prunes = _composite_prunes(lifecycle_handlers or {}, rendered, state, changeset, prune_types, prune_keys)
        changeset = _with_composite_prunes(changeset, prunes)
    return _with_observation_failures(changeset, reading.failures)


def unrecorded_composites(
    rendered: Mapping[str, RenderedArtifact], registry: Registry = SEMANTIC_REGISTRY
) -> dict[str, CompositePlan]:
    """Block each composite artifact a plan from a recorded reading renders, by key.

    A composite artifact's handler observes its resources itself, live, so no recorded
    reading holds what one would need to decide it.

    Diagnostics:
        SST-PLN001: one per composite artifact, which blocks it.
    """
    plans: dict[str, CompositePlan] = {}
    for key, artifact in rendered.items():
        if registry.artifacts[artifact.artifact_type].lifecycle is not ArtifactLifecycle.COMPOSITE:
            continue
        diagnostic = D(
            "SST-PLN001",
            subject=key,
            value=key,
            detail="a recorded observation holds no composite artifact; plan it without --use-cached-state",
        )
        observation = CompositeObservation(key, diagnostics=DiagnosticBag((diagnostic,)))
        plans[key] = CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, observation.diagnostics)
    return plans


def _composite_prunes(
    handlers: Mapping[str, CompositeLifecycleHandler],
    rendered: Mapping[str, RenderedArtifact],
    state: State,
    changeset: ChangeSet,
    prune_types: frozenset[str] | None,
    prune_keys: frozenset[str] | None,
) -> tuple[Change, ...]:
    """Report, in key order, the prunes of composite artifacts that only state still records.

    Observation cannot see a composite artifact, so its handler reports the prune from the
    state entry. A deactivated entry is already retired, and the prune filters apply.
    """
    existing_keys = {change.key for change in changeset.changes}
    return tuple(
        handler.report_prune(key, entry)
        for key, entry in sorted(state.applied.items())
        if key not in rendered
        and key not in existing_keys
        and entry.outcome != DEACTIVATED
        and (handler := handlers.get(key.split(":", 1)[0])) is not None
        and (prune_types is None or key.split(":", 1)[0] in prune_types)
        and (prune_keys is None or key in prune_keys)
    )


def _with_composite_prunes(changeset: ChangeSet, prunes: tuple[Change, ...]) -> ChangeSet:
    """Append the composite prunes, and their diagnostics after the plan's; unchanged without any."""
    if not prunes:
        return changeset
    return replace(
        changeset,
        changes=(*changeset.changes, *prunes),
        diagnostics=DiagnosticBag((*changeset.diagnostics, *(item for prune in prunes for item in prune.diagnostics))),
    )


def _with_observation_failures(changeset: ChangeSet, diagnostics: DiagnosticBag) -> ChangeSet:
    """Report the observation's failures ahead of the plan's own diagnostics; unchanged without any."""
    if not diagnostics:
        return changeset
    return replace(changeset, diagnostics=DiagnosticBag((*diagnostics, *changeset.diagnostics)))
