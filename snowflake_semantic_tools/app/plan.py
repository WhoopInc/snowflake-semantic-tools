"""Observe live state and compute a deterministic, non-writing ChangeSet."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import Mapping

from ..domain.model.artifact_key import artifact_key
from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from ..domain.model.identifier import QualifiedName, SchemaScope, TargetIdentity
from ..domain.model.lifecycle import (
    Change,
    ChangeSet,
    CompositePlan,
    GrantRow,
    ObservedArtifact,
    RenderedArtifact,
    ShowRow,
    SnowflakeObservation,
    extract_marker,
)
from ..domain.model.registry import SEMANTIC_REGISTRY, ArtifactType, Registry
from ..domain.plan import build_changeset
from ..domain.ports.lifecycle import CompositeLifecycleHandler
from ..domain.ports.snowflake import SnowflakePort, SnowflakePortError
from ..domain.state import DEACTIVATED, Manifest, State

_FoldedName = tuple[str, str, str]


def observe(
    port: SnowflakePort,
    registry: Registry,
    targets: tuple[QualifiedName, ...],
    *,
    fetched_at: str,
    artifact_types: frozenset[str] | None = None,
    observed_object_types: Mapping[str, frozenset[str]] | None = None,
    desired_artifacts: Mapping[str, RenderedArtifact] | None = None,
) -> tuple[SnowflakeObservation, DiagnosticBag]:
    """Observe every object of the requested artifact types in the targets' schemas, by artifact key.

    The types are observed in DDL order, each object type in every schema the targets name,
    once per schema. A failed read is reported and observation goes on, so one missing
    privilege never hides the rest.

    Args:
        artifact_types: The types to observe; None observes every registered type.
        observed_object_types: The object types to list for each artifact type; a type this
            leaves out or empty lists the registry's object types instead.
        desired_artifacts: The rendered artifacts; the grants of an object one of them may
            replace are read, and its routine signature addresses it.

    Diagnostics:
        SST-PLN001: listing an object type in a schema, or reading an object's grants, failed.
    """
    found: dict[str, ObservedArtifact] = {}
    diagnostics: list[Diagnostic] = []
    scopes = tuple(dict.fromkeys(SchemaScope.from_qualified_name(target) for target in targets))
    desired = {artifact.target.folded: artifact for artifact in (desired_artifacts or {}).values()}
    for artifact_type in sorted(registry.artifacts.values(), key=lambda item: item.ddl_position):
        if artifact_types is not None and artifact_type.name not in artifact_types:
            continue
        for object_type in _active_object_types(artifact_type, observed_object_types):
            for scope in scopes:
                observed, scope_diagnostics = _observe_scope(port, artifact_type, object_type, scope, desired)
                diagnostics.extend(scope_diagnostics)
                found.update((artifact.key, artifact) for artifact in observed)
    return SnowflakeObservation(MappingProxyType(found), fetched_at), DiagnosticBag(diagnostics)


def _active_object_types(
    artifact_type: ArtifactType,
    observed_object_types: Mapping[str, frozenset[str]] | None,
) -> tuple[str, ...]:
    """Return the object types to list for an artifact type, dropping the empty name of a composite."""
    configured_object_types = (
        (tuple(sorted(observed_object_types.get(artifact_type.name, ()))) if observed_object_types is not None else ())
        or artifact_type.object_types
        or ((artifact_type.object_type,) if artifact_type.object_type else ())
    )
    return tuple(object_type for object_type in configured_object_types if object_type)


def _observe_scope(
    port: SnowflakePort,
    artifact_type: ArtifactType,
    object_type: str,
    scope: SchemaScope,
    desired: Mapping[_FoldedName, RenderedArtifact],
) -> tuple[tuple[ObservedArtifact, ...], tuple[Diagnostic, ...]]:
    """Observe the objects of one type in one schema, in the order SHOW lists them."""
    try:
        rows = port.show_objects(object_type, scope)
    except SnowflakePortError as exc:
        return (), (D("SST-PLN001", value=f"{object_type} in {scope.sql}", detail=str(exc)),)
    observed: list[ObservedArtifact] = []
    diagnostics: list[Diagnostic] = []
    for row in rows:
        artifact, row_diagnostics = _observe_row(port, artifact_type, object_type, row, desired)
        diagnostics.extend(row_diagnostics)
        observed.append(artifact)
    return tuple(observed), tuple(diagnostics)


def _observe_row(
    port: SnowflakePort,
    artifact_type: ArtifactType,
    object_type: str,
    row: ShowRow,
    desired: Mapping[_FoldedName, RenderedArtifact],
) -> tuple[ObservedArtifact, tuple[Diagnostic, ...]]:
    """Observe one listed object: its ownership marker, its grants, and whether an agent is live."""
    desired_artifact = desired.get(row.qualified_name.folded)
    key = artifact_key(artifact_type.name, row.qualified_name.artifact_component)
    grants, diagnostics = _replaceable_grants(port, artifact_type, object_type, row, desired_artifact)
    artifact = ObservedArtifact(
        key=key,
        raw_name=row.name,
        qualified_name=row.qualified_name,
        object_type=row.object_type,
        owner=row.owner,
        created_on=row.created_on,
        comment=row.comment,
        marker=extract_marker(row.comment),
        grants=grants,
        has_live_version=(port.agent_has_live_version(row.qualified_name) if object_type == "AGENT" else False),
    )
    return artifact, diagnostics


def _replaceable_grants(
    port: SnowflakePort,
    artifact_type: ArtifactType,
    object_type: str,
    row: ShowRow,
    desired_artifact: RenderedArtifact | None,
) -> tuple[tuple[GrantRow, ...] | None, tuple[Diagnostic, ...]]:
    """Read the grants of an object this plan may replace; None when they are not read or cannot be."""
    # Grants matter only for an object this plan may replace. A prune
    # candidate or an unrelated object is never replaced, and an
    # unrelated routine has no known signature to address it by.
    if not artifact_type.replaces_on_update or desired_artifact is None:
        return None, ()
    try:
        grants = port.show_grants(object_type, row.qualified_name, desired_artifact.routine_signature)
    except SnowflakePortError as exc:
        return None, (D("SST-PLN001", value=f"grants on {row.qualified_name.sql}", detail=str(exc)),)
    return tuple(sorted(grants)), ()


class PlanArtifacts:
    """Plan the changes that bring a target to the rendered artifacts, writing nothing.

    Composite artifacts are planned by their lifecycle handlers, keyed by artifact type; every
    other artifact is planned from what `observe` finds in Snowflake.
    """

    def __init__(
        self,
        port: SnowflakePort,
        *,
        registry: Registry = SEMANTIC_REGISTRY,
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
    ) -> None:
        self._port = port
        self._registry = registry
        self._lifecycle_handlers = dict(lifecycle_handlers or {})

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
        2. Observation of every type rendered and, when pruning, every type a prune may remove,
           in the schemas of `observation_targets`, else of the rendered artifacts.
        3. The change set, from the observation, the manifest and state.
        4. Composite prunes, when pruning: a composite artifact state records, that nothing
           rendered or planned names, is reported by its handler as a prune.
        5. The observation's failures, reported ahead of every other diagnostic.

        Args:
            blocked: Diagnostics by artifact key that make its change BLOCKED.
            prune_types, prune_keys: Narrow the prunes to these artifact types and keys; None
                does not narrow.
            observation_targets: The objects whose schemas are observed; None observes the
                schemas of the rendered artifacts.

        Diagnostics:
            SST-PLN001: observing Snowflake failed, as `observe` reports it.
        """
        composite_plans = self._composite_plans(rendered, manifest, state)
        observation, diagnostics = self._observe(rendered, fetched_at, include_prune, prune_types, observation_targets)
        changeset: ChangeSet = build_changeset(
            rendered,
            observation,
            manifest,
            state,
            self._registry,
            target,
            blocked=blocked,
            include_prune=include_prune,
            full=full,
            prune_types=prune_types,
            prune_keys=prune_keys,
            composite_plans=composite_plans,
        )
        if include_prune:
            prunes = self._composite_prunes(rendered, state, changeset, prune_types, prune_keys)
            changeset = _with_composite_prunes(changeset, prunes)
        return _with_observation_failures(changeset, diagnostics)

    def _composite_plans(
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

    def _observe(
        self,
        rendered: Mapping[str, RenderedArtifact],
        fetched_at: str,
        include_prune: bool,
        prune_types: frozenset[str] | None,
        observation_targets: tuple[QualifiedName, ...] | None,
    ) -> tuple[SnowflakeObservation, DiagnosticBag]:
        """Observe the types the plan needs, in the schemas of the targets, else of the rendered artifacts."""
        return observe(
            self._port,
            self._registry,
            observation_targets or tuple(artifact.target for artifact in rendered.values()),
            fetched_at=fetched_at,
            artifact_types=self._observed_artifact_types(rendered, include_prune, prune_types),
            observed_object_types=self._observed_object_types(rendered),
            desired_artifacts=rendered,
        )

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

    def _composite_prunes(
        self,
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
            and (handler := self._lifecycle_handlers.get(key.split(":", 1)[0])) is not None
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
