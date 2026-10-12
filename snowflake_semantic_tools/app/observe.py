"""Observe what Snowflake holds for a plan: each object of the planned types, its marker and grants.

`observe` lists each artifact type's objects in every schema the targets name, once per
schema, reading an object's grants only when the plan may replace it, and its definition only
when asked to capture it. A failed read is reported and observation goes on, so one missing
privilege never hides the rest. Every listing is one unit a `Fanout` may run on a session of its
own, and then so are the reads of each object listed; both are merged in the order they would
run one at a time, each listing followed by the reads of what it lists.

`observe_replaced_tags` then reads the tags of each semantic view the plan will replace, which
only deciding the plan from what `observe` found can tell.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.prior import prior_definition
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    GrantRow,
    ObservedArtifact,
    RenderedArtifact,
    ShowRow,
    SnowflakeObservation,
    extract_marker,
)
from snowflake_semantic_tools.domain.model.registry import ArtifactType, Registry
from snowflake_semantic_tools.domain.plan import classify
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersions, CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry

_FoldedName = tuple[str, str, str]
# One listing: an artifact type's objects of one object type in one schema.
_Unit = tuple[ArtifactType, str, SchemaScope]
# The object types whose replace drops every tag it does not name (docs/design/snowflake-behaviour.md).
_TAGS_REPLACED = frozenset(("SEMANTIC VIEW",))


@dataclass(frozen=True, slots=True)
class ObserveOptions:
    """Which per-object reads an observation makes, beyond listing each object and its marker.

    Attributes:
        grants: Read the grants of each object the plan may replace, one SHOW GRANTS each, so
            the plan can report what a replace would drop, and the tags of each semantic view it
            will replace, one TAG_REFERENCES read each, so it can report the tags the replace
            removes; `plan --no-grants` turns both off.
        capture_prior: Read the current definition of each object a rendered artifact names,
            as `plan --capture-prior` asks: one GET_DDL each, or for an agent one SHOW
            VERSIONS IN AGENT; `app.prior` says what each kind's definition is.
    """

    grants: bool = True
    capture_prior: bool = False


# What a plan reads when no flag says otherwise: grants, and no definitions.
DEFAULT_OBSERVE_OPTIONS = ObserveOptions()


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
    options: ObserveOptions = DEFAULT_OBSERVE_OPTIONS,
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
        readers: Sessions to run the listings, then the reads of what they list, on
            concurrently; None reads on `port`, one at a time. Either way the observation and
            its diagnostics are the same.
        options: Which per-object reads to make, as `ObserveOptions` says.

    Diagnostics:
        SST-PLN001: listing an object type in a schema, or reading an object's grants, failed.
        SST-PLN037: under `capture_prior`, reading an object's definition failed; the
            observation is otherwise whole.
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
    readers = readers or Fanout(port)
    listings = readers.map(lambda session, unit: _list_scope(session, unit[1], unit[2]), units)
    listed = tuple(
        (unit, row)
        for unit, listing in zip(units, listings, strict=True)
        if not isinstance(listing, Diagnostic)
        for row in listing
    )
    reads = iter(
        readers.map(
            lambda session, item: _observe_row(session, item[0][0], item[0][1], item[1], desired, options), listed
        )
    )
    for listing in listings:
        if isinstance(listing, Diagnostic):
            diagnostics.append(listing)
            continue
        for artifact, row_diagnostics in itertools.islice(reads, len(listing)):
            diagnostics.extend(row_diagnostics)
            found[artifact.key] = artifact
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


def _list_scope(port: CatalogPort, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...] | Diagnostic:
    """List the objects of one type in one schema, in the order SHOW lists them; SST-PLN001 if it fails."""
    try:
        return port.show_objects(object_type, scope)
    except SnowflakePortError as exc:
        return D("SST-PLN001", value=f"{object_type} in {scope.sql}", detail=str(exc))


def _observe_row(
    port: CatalogPort,
    artifact_type: ArtifactType,
    object_type: str,
    row: ShowRow,
    desired: Mapping[_FoldedName, RenderedArtifact],
    options: ObserveOptions,
) -> tuple[ObservedArtifact, tuple[Diagnostic, ...]]:
    """Observe one listed object: its marker, its grants and definition, and an agent's versions."""
    desired_artifact = desired.get(row.qualified_name.folded)
    key = artifact_key(artifact_type.name, row.qualified_name.artifact_component)
    grants, diagnostics = (
        _replaceable_grants(port, artifact_type, object_type, row, desired_artifact) if options.grants else (None, ())
    )
    # One SHOW VERSIONS read serves both the update program and the agent's captured prior.
    versions = port.observe_agent_versions(row.qualified_name) if object_type == "AGENT" else AgentVersions()
    definition, definition_diagnostics = (
        prior_definition(port, object_type, row, desired_artifact, versions) if options.capture_prior else (None, ())
    )
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
        definition=definition,
        has_live_version=versions.has_live_version,
        aliases=versions.aliases,
    )
    return artifact, (*diagnostics, *definition_diagnostics)


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


def observe_replaced_tags(
    port: CatalogPort,
    observation: SnowflakeObservation,
    rendered: Mapping[str, RenderedArtifact],
    applied: Mapping[str, AppliedEntry],
    registry: Registry,
    *,
    readers: Fanout[CatalogPort] | None = None,
) -> tuple[SnowflakeObservation, DiagnosticBag]:
    """Read the tags on each semantic view the plan will replace, into the observation.

    A replace keeps only the tags its WITH TAG names, so they matter only for a view the plan
    updates: one whose decision, from the observation and the entry state recorded for it, is an
    update. Every other object keeps the tags it was observed with, None for unread. A failed
    read leaves the view's tags unread and the plan goes on.

    Args:
        applied: The entries state recorded, by artifact key.
        readers: Sessions to run the reads on concurrently; None reads on `port`, one at a time.

    Diagnostics:
        SST-PLN040: reading a replaced view's tags failed, so the tags its replace removes are
            not checked.
    """
    replaced = tuple(
        (key, observed)
        for key, artifact in rendered.items()
        if (observed := observation.artifacts.get(key)) is not None
        and observed.object_type.upper() in _TAGS_REPLACED
        and _decided_update(key, artifact, observed, applied.get(key), registry)
    )
    if not replaced:
        return observation, DiagnosticBag()
    reads = (readers or Fanout(port)).map(lambda session, item: _read_tags(session, item[1]), replaced)
    found = dict(observation.artifacts)
    diagnostics: list[Diagnostic] = []
    for (key, observed), read in zip(replaced, reads, strict=True):
        if isinstance(read, Diagnostic):
            diagnostics.append(read)
            continue
        found[key] = replace(observed, tags=read)
    return replace(observation, artifacts=MappingProxyType(found)), DiagnosticBag(diagnostics)


def _decided_update(
    key: str,
    artifact: RenderedArtifact,
    observed: ObservedArtifact,
    recorded: AppliedEntry | None,
    registry: Registry,
) -> bool:
    """Say whether the plan decides an update of the observed object, before validation blocks it."""
    decision = classify(
        key, artifact, registry, observed=observed, recorded=recorded, validation=DiagnosticBag(), composite=None
    )
    return decision.change.action is Action.UPDATE


def _read_tags(port: CatalogPort, observed: ObservedArtifact) -> tuple[str, ...] | Diagnostic:
    """Read the tags on one semantic view; SST-PLN040 if the read fails."""
    try:
        return port.tag_references(observed.object_type, observed.qualified_name)
    except SnowflakePortError as exc:
        return D("SST-PLN040", value=observed.qualified_name.sql, detail=str(exc))
