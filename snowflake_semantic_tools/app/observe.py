"""Observe what Snowflake holds for a plan: each object of the planned types, its marker and grants.

`observe` lists each artifact type's objects in every schema the targets name, once per
schema, reading an object's grants only when the plan may replace it. A failed read is
reported and observation goes on, so one missing privilege never hides the rest. Each
listing, with the reads of the objects it lists, is one unit a `Fanout` may run on a
session of its own; the units are merged in the order they would run one at a time.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    GrantRow,
    ObservedArtifact,
    RenderedArtifact,
    ShowRow,
    SnowflakeObservation,
    extract_marker,
)
from snowflake_semantic_tools.domain.model.registry import ArtifactType, Registry
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

_FoldedName = tuple[str, str, str]
# One listing: an artifact type's objects of one object type in one schema.
_Unit = tuple[ArtifactType, str, SchemaScope]


def observe(
    port: CatalogPort,
    registry: Registry,
    targets: tuple[QualifiedName, ...],
    *,
    fetched_at: str,
    artifact_types: frozenset[str] | None = None,
    observed_object_types: Mapping[str, frozenset[str]] | None = None,
    desired_artifacts: Mapping[str, RenderedArtifact] | None = None,
    readers: Fanout[CatalogPort] | None = None,
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
        readers: Sessions to run the listings on concurrently; None lists on `port`, one
            at a time. Either way the observation and its diagnostics are the same.

    Diagnostics:
        SST-PLN001: listing an object type in a schema, or reading an object's grants, failed.
    """
    found: dict[str, ObservedArtifact] = {}
    diagnostics: list[Diagnostic] = []
    scopes = tuple(dict.fromkeys(SchemaScope.from_qualified_name(target) for target in targets))
    desired = {artifact.target.folded: artifact for artifact in (desired_artifacts or {}).values()}
    units: list[_Unit] = [
        (artifact_type, object_type, scope)
        for artifact_type in sorted(registry.artifacts.values(), key=lambda item: item.ddl_position)
        if artifact_types is None or artifact_type.name in artifact_types
        for object_type in _active_object_types(artifact_type, observed_object_types)
        for scope in scopes
    ]
    results = (readers or Fanout(port)).map(
        lambda session, unit: _observe_scope(session, unit[0], unit[1], unit[2], desired), units
    )
    for observed, scope_diagnostics in results:
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
    port: CatalogPort,
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
    port: CatalogPort,
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
    port: CatalogPort,
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
