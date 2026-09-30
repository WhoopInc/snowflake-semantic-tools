"""Observe live state and compute a deterministic, non-writing ChangeSet."""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from ..domain.model.artifact_key import artifact_key
from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from ..domain.model.identifier import QualifiedName, SchemaScope, TargetIdentity
from ..domain.model.lifecycle import (
    ChangeSet,
    GrantRow,
    ObservedArtifact,
    RenderedArtifact,
    SnowflakeObservation,
    extract_marker,
)
from ..domain.model.registry import SEMANTIC_REGISTRY, Registry
from ..domain.plan import build_changeset
from ..domain.ports.lifecycle import CompositeLifecycleHandler
from ..domain.ports.snowflake import SnowflakePort, SnowflakePortError
from ..domain.state import DEACTIVATED, Manifest, State


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
    found: dict[str, ObservedArtifact] = {}
    diagnostics: list[Diagnostic] = []
    scopes = tuple(dict.fromkeys(SchemaScope.from_qualified_name(target) for target in targets))
    desired = {artifact.target.folded: artifact for artifact in (desired_artifacts or {}).values()}
    for artifact_type in sorted(registry.artifacts.values(), key=lambda item: item.ddl_position):
        if artifact_types is not None and artifact_type.name not in artifact_types:
            continue
        configured_object_types = (
            (
                tuple(sorted(observed_object_types.get(artifact_type.name, ())))
                if observed_object_types is not None
                else ()
            )
            or artifact_type.object_types
            or ((artifact_type.object_type,) if artifact_type.object_type else ())
        )
        active_object_types = tuple(object_type for object_type in configured_object_types if object_type)
        if not active_object_types:
            continue
        for object_type in active_object_types:
            for scope in scopes:
                try:
                    rows = port.show_objects(object_type, scope)
                except SnowflakePortError as exc:
                    diagnostics.append(
                        D(
                            "SST-PLN001",
                            value=f"{object_type} in {scope.sql}",
                            detail=str(exc),
                        )
                    )
                    continue
                for row in rows:
                    desired_artifact = desired.get(row.qualified_name.folded)
                    key = artifact_key(artifact_type.name, row.qualified_name.artifact_component)
                    grants: tuple[GrantRow, ...] | None = None
                    # Grants matter only for an object this plan may replace. A prune
                    # candidate or an unrelated object is never replaced, and an
                    # unrelated routine has no known signature to address it by.
                    if artifact_type.replaces_on_update and desired_artifact is not None:
                        try:
                            grants = tuple(
                                sorted(
                                    port.show_grants(
                                        object_type,
                                        row.qualified_name,
                                        desired_artifact.routine_signature if desired_artifact else (),
                                    )
                                )
                            )
                        except SnowflakePortError as exc:
                            diagnostics.append(
                                D("SST-PLN001", value=f"grants on {row.qualified_name.sql}", detail=str(exc))
                            )
                    found[key] = ObservedArtifact(
                        key=key,
                        raw_name=row.name,
                        qualified_name=row.qualified_name,
                        object_type=row.object_type,
                        owner=row.owner,
                        created_on=row.created_on,
                        comment=row.comment,
                        marker=extract_marker(row.comment),
                        grants=grants,
                        has_live_version=(
                            port.agent_has_live_version(row.qualified_name) if object_type == "AGENT" else False
                        ),
                    )
    return SnowflakeObservation(MappingProxyType(found), fetched_at), DiagnosticBag(diagnostics)


class PlanArtifacts:
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
        composite_plans = {
            key: self._lifecycle_handlers[artifact.artifact_type].plan(
                artifact,
                state.applied.get(key),
                manifest,
            )
            for key, artifact in rendered.items()
            if artifact.artifact_type in self._lifecycle_handlers
        }
        observation, diagnostics = observe(
            self._port,
            self._registry,
            observation_targets or tuple(artifact.target for artifact in rendered.values()),
            fetched_at=fetched_at,
            artifact_types=frozenset(
                {artifact.artifact_type for artifact in rendered.values()}
                | (
                    set(prune_types)
                    if include_prune and prune_types is not None
                    else set(self._registry.artifacts) if include_prune else set()
                )
            ),
            observed_object_types={
                artifact_type: frozenset(
                    artifact.object_type
                    for artifact in rendered.values()
                    if artifact.artifact_type == artifact_type and artifact.object_type
                )
                for artifact_type in self._registry.artifacts
            },
            desired_artifacts=rendered,
        )
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
            existing_keys = {change.key for change in changeset.changes}
            composite_prunes = tuple(
                handler.report_prune(key, entry)
                for key, entry in sorted(state.applied.items())
                if key not in rendered
                and key not in existing_keys
                and entry.outcome != DEACTIVATED
                and (handler := self._lifecycle_handlers.get(key.split(":", 1)[0])) is not None
                and (prune_types is None or key.split(":", 1)[0] in prune_types)
                and (prune_keys is None or key in prune_keys)
            )
            if composite_prunes:
                changeset = ChangeSet(
                    changeset.manifest_id,
                    changeset.target,
                    (*changeset.changes, *composite_prunes),
                    DiagnosticBag(
                        (*changeset.diagnostics, *(item for prune in composite_prunes for item in prune.diagnostics))
                    ),
                    changeset.observation_at,
                    changeset.full,
                    changeset.plan_id,
                )
        if diagnostics:
            changeset = ChangeSet(
                changeset.manifest_id,
                changeset.target,
                changeset.changes,
                DiagnosticBag((*diagnostics, *changeset.diagnostics)),
                changeset.observation_at,
                changeset.full,
                changeset.plan_id,
            )
        return changeset
