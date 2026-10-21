"""Plan rendered artifacts against a target: read what it holds, then decide each change.

`PlanArtifacts.read` observes what `app.observe` finds in Snowflake and, given a preflight
port, what `app.preflight` reads about the target. `plan_changes` decides the change set from
such a `TargetReading` without reading anything, so a plan can be decided from a recorded
reading as well as from a live one. `PlanArtifacts.run` does both.

A schema apply will create holds nothing yet, and Snowflake refuses to list one that does not
exist, so neither the observation nor a composite plan reads it: `absent_schemas` finds them
once, and both treat every object in them as absent.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Protocol, cast

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.observe import (
    DEFAULT_OBSERVE_OPTIONS,
    ObserveOptions,
    observe,
    observe_replaced_tags,
    read_attachments,
)
from snowflake_semantic_tools.app.preflight import read_preflight
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope, TargetIdentity
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
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort, ExtensionObservation
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort
from snowflake_semantic_tools.domain.ports.timing import NO_PHASES, PhaseTimer
from snowflake_semantic_tools.domain.state import DEACTIVATED, Manifest, State
from snowflake_semantic_tools.domain.state.lock import STANDARD_TABLES


class PlanReadPort(SnowflakePort, PreflightPort, Protocol):
    """A session a plan reads on: validation's checks, observation, and the preflight reads."""


class _AbsentSchemaReads:
    """A session whose existence reads find nothing in a schema apply will create.

    A composite plan reads its stages, extension, registry table and eval resources through
    `stage_type`, `observe_extension`, `table_columns` and `object_exists`, and reads further
    only what those find. Snowflake refuses each of them (002043) in a schema that does not
    exist, so for an object in one of `absent` they answer as for an object not there; every
    other read goes to the session as it is.

    Args:
        absent: The folded `(database, schema)` of the schemas apply will create.
    """

    def __init__(self, session: CatalogPort, absent: frozenset[tuple[str, str]]) -> None:
        self._session = session
        self._absent = absent

    def __getattr__(self, name: str) -> object:
        return getattr(self._session, name)

    def _in_absent(self, qualified_name: QualifiedName) -> bool:
        return qualified_name.folded[:2] in self._absent

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        """Return the stage's type; None in a schema apply will create."""
        return None if self._in_absent(qualified_name) else self._session.stage_type(qualified_name)

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        """Return the extension's observation; None in a schema apply will create."""
        return None if self._in_absent(qualified_name) else self._session.observe_extension(qualified_name)

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        """Return the table's columns; None in a schema apply will create."""
        return None if self._in_absent(qualified_name) else self._session.table_columns(qualified_name)

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        """Report whether the object exists; never in a schema apply will create."""
        return not self._in_absent(qualified_name) and self._session.object_exists(object_type, qualified_name)


@dataclass(frozen=True, slots=True)
class TargetReading:
    """What a plan read of the target before deciding.

    Attributes:
        observation: Every object observed, keyed by artifact key.
        preflight: The preflight facts; None when no preflight reads were made.
        failures: The reads Snowflake refused, as SST-PLN001, the tags of a replaced view it
            refused, as SST-PLN040, and the advisory reads the role may not make, as SST-VAL020.
        attachment: The account's CoWork object and what is attached to it, as
            `read_attachments` read them; None when nothing wants attaching or the read failed.
            Snowflake private preview (PrPr).
    """

    observation: SnowflakeObservation
    preflight: Preflight | None = None
    failures: DiagnosticBag = DiagnosticBag()
    attachment: AttachmentReading | None = None


class PlanArtifacts:
    """Plan the changes that bring a target to the rendered artifacts, writing nothing.

    Composite artifacts are planned by their lifecycle handlers, keyed by artifact type; every
    other artifact is planned from what `observe` finds in Snowflake.

    Args:
        readers: Sessions, opened from `port` and `preflight`, to run the composite plans, the
            observation's listings, and each phase of preflight reads on concurrently; None
            reads on `port` and `preflight`, one at a time. Either way the change set is the same.
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
        kind_targets: Mapping[str, tuple[QualifiedName, ...]] | None = None,
    ) -> ChangeSet:
        """Compute the change set for the rendered artifacts from what Snowflake shows now.

        Steps, in order:

        1. Composite plans and the reading of the target, as `read_target` makes them.
        2. The change set, as `plan_changes` decides it.

        Args:
            blocked: Diagnostics by artifact key that make its change BLOCKED.
            prune_types, prune_keys: Narrow the prunes to these artifact types and keys; None
                does not narrow.
            observation_targets: The objects whose schemas are observed; None observes the
                schemas of the rendered artifacts.
            kind_targets: The objects of each artifact type, as `targets_by_kind` returns
                them; None takes them from the rendered artifacts and state.

        Diagnostics:
            SST-PLN001: observing Snowflake, reading the CoWork object, or a preflight read, failed.
            SST-PLN040: reading the tags on a semantic view the plan will replace failed.
        """
        composite_plans, reading = self.read_target(
            rendered,
            manifest,
            state,
            target,
            fetched_at=fetched_at,
            include_prune=include_prune,
            prune_types=prune_types,
            observation_targets=observation_targets,
            kind_targets=kind_targets,
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

    def read_target(
        self,
        rendered: Mapping[str, RenderedArtifact],
        manifest: Manifest,
        state: State,
        target: TargetIdentity,
        *,
        fetched_at: str,
        phases: PhaseTimer = NO_PHASES,
        include_prune: bool = False,
        prune_types: frozenset[str] | None = None,
        observation_targets: tuple[QualifiedName, ...] | None = None,
        state_tables: tuple[QualifiedName, ...] = (),
        state_table_kind: str = STANDARD_TABLES,
        create_schemas: bool = False,
        kind_targets: Mapping[str, tuple[QualifiedName, ...]] | None = None,
    ) -> tuple[dict[str, CompositePlan], TargetReading]:
        """Plan the composite artifacts, then read the target as `read` does, each a phase of `phases`.

        With `create_schemas`, the schemas apply will create are found once, first, as
        `absent_schemas` finds them: neither the composite plans nor the observation reads one,
        so what is published there plans as a create.

        Diagnostics:
            SST-PLN001: observing Snowflake, reading the CoWork object, or a preflight read, failed.
            SST-PLN040: reading the tags on a semantic view the plan will replace failed.
        """
        with phases.phase("composites"):
            absent = (
                self.absent_schemas(rendered, state, observation_targets=observation_targets)
                if create_schemas
                else frozenset()
            )
            composite_plans = self.composite_plans(rendered, manifest, state, absent_schemas=absent)
        with phases.phase("observe"):
            reading = self.read(
                rendered,
                state,
                target,
                fetched_at=fetched_at,
                include_prune=include_prune,
                prune_types=prune_types,
                observation_targets=observation_targets,
                state_tables=state_tables,
                state_table_kind=state_table_kind,
                create_schemas=create_schemas,
                absent_schemas=absent,
                kind_targets=kind_targets,
            )
        return composite_plans, reading

    def composite_plans(
        self,
        rendered: Mapping[str, RenderedArtifact],
        manifest: Manifest,
        state: State,
        *,
        absent_schemas: frozenset[tuple[str, str]] = frozenset(),
    ) -> dict[str, CompositePlan]:
        """Ask each composite artifact's handler to plan it, keyed in rendered order.

        With `readers` of more than one worker, the artifacts are planned concurrently, each by
        its handler bound to a session leased for it alone; otherwise one at a time on the
        handlers' own session. A handler's plan never raises for a refused read, so either way
        the plans are the same.

        Args:
            absent_schemas: The folded `(database, schema)` of the schemas apply will create,
                as `absent_schemas` finds them. A handler finds nothing in them, so what it
                publishes there plans as a create; each is bound to `port`, or to its leased
                session, for the plan alone.
        """
        composites = tuple(
            (key, artifact) for key, artifact in rendered.items() if artifact.artifact_type in self._lifecycle_handlers
        )

        def planned(handler: CompositeLifecycleHandler, item: tuple[str, RenderedArtifact]) -> CompositePlan:
            key, artifact = item
            return handler.plan(artifact, state.applied.get(key), manifest)

        def reading(session: CatalogPort) -> CatalogPort:
            # The wrapper answers every read the session does, delegating all but its own four.
            return cast(CatalogPort, _AbsentSchemaReads(session, absent_schemas)) if absent_schemas else session

        def leased(session: PlanReadPort, item: tuple[str, RenderedArtifact]) -> CompositePlan:
            return planned(self._lifecycle_handlers[item[1].artifact_type].for_session(reading(session)), item)

        def own(item: tuple[str, RenderedArtifact]) -> CompositePlan:
            handler = self._lifecycle_handlers[item[1].artifact_type]
            return planned(handler.for_session(reading(self._port)) if absent_schemas else handler, item)

        if self._readers is None or self._readers.workers < 2:
            plans = tuple(own(item) for item in composites)
        else:
            plans = self._readers.map(leased, composites)
        return {key: plan for (key, _), plan in zip(composites, plans, strict=True)}

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
        state_tables: tuple[QualifiedName, ...] = (),
        state_table_kind: str = STANDARD_TABLES,
        create_schemas: bool = False,
        absent_schemas: frozenset[tuple[str, str]] | None = None,
        kind_targets: Mapping[str, tuple[QualifiedName, ...]] | None = None,
    ) -> TargetReading:
        """Read the target for a plan: observe it, then preflight it when given a preflight port.

        The observation covers every type rendered, every type they depend on, and, when
        pruning, every type a prune may remove, in the schemas of `observation_targets`, else of
        the rendered artifacts. Unless the options turn grants off, it then holds the tags of
        each semantic view the plan will replace, as `observe_replaced_tags` reads them. The
        account's CoWork object is read next, as `read_attachments` reads it. The preflight read
        is `read_preflight`'s, `state_tables` among what it checks, each that exists expected to
        be a `state_table_kind` table, and with `create_schemas` each absent schema one apply
        creates.

        Args:
            absent_schemas: The schemas apply will create, as `absent_schemas` found them for
                the composite plans; None finds them here, with `create_schemas`.
            kind_targets: The objects of each artifact type, as `targets_by_kind` returns
                them; None takes them from the rendered artifacts and state.

        Diagnostics:
            SST-PLN001: observing Snowflake, reading the CoWork object, or a preflight read, failed.
            SST-PLN040: reading the tags on a semantic view the plan will replace failed.
        """
        targets = observation_targets or tuple(artifact.target for artifact in rendered.values())
        observation, failures = observe(
            self._port,
            self._registry,
            targets,
            fetched_at=fetched_at,
            artifact_types=self._observed_artifact_types(rendered, include_prune, prune_types),
            observed_object_types=self._observed_object_types(rendered),
            desired_artifacts=rendered,
            readers=self._readers,
            options=self._observe_options,
            absent_schemas=(
                absent_schemas
                if absent_schemas is not None
                else self.absent_schemas(rendered, state, observation_targets=observation_targets)
                if create_schemas
                else frozenset()
            ),
            kind_targets=kind_targets if kind_targets is not None else targets_by_kind(rendered.values(), state),
        )
        if self._observe_options.grants:
            observation, unread = observe_replaced_tags(
                self._port, observation, rendered, state.applied, self._registry, readers=self._readers
            )
            failures = DiagnosticBag((*failures, *unread))
        # Snowflake private preview (PrPr): CoWork profiles
        attachment, unattached = read_attachments(self._port, rendered)
        failures = DiagnosticBag((*failures, *unattached))
        if self._preflight is None:
            return TargetReading(observation, None, failures, attachment)
        preflight, refused = read_preflight(
            self._preflight,
            rendered,
            observation,
            state,
            target,
            include_prune=include_prune,
            readers=self._readers,
            state_tables=state_tables,
            state_table_kind=state_table_kind,
            create_schemas=create_schemas,
        )
        return TargetReading(observation, preflight, DiagnosticBag((*failures, *refused)), attachment)

    def absent_schemas(
        self,
        rendered: Mapping[str, RenderedArtifact],
        state: State,
        *,
        observation_targets: tuple[QualifiedName, ...] | None = None,
    ) -> frozenset[tuple[str, str]]:
        """Return the folded schemas of the targets that the preflight port says do not exist.

        The targets are `observation_targets`, else the rendered artifacts'; without a preflight
        port, none is absent. A schema apply creates holds nothing yet, so reading it would only
        fail. A schema state records an object in is never one: the role may only be unable to
        see it, and observing it reports that. A read that fails counts the schema present, too.
        """
        targets = observation_targets or tuple(artifact.target for artifact in rendered.values())
        if self._preflight is None:
            return frozenset()
        recorded = {
            QualifiedName.parse(entry.qualified_name).folded[:2]
            for entry in state.applied.values()
            if entry.qualified_name
        }
        absent: set[tuple[str, str]] = set()
        for scope in dict.fromkeys(SchemaScope.from_qualified_name(target) for target in targets):
            if (scope.database.folded, scope.schema.folded) in recorded:
                continue
            try:
                found = self._preflight.schema_exists(scope)
            except SnowflakePortError:
                continue
            if not found:
                absent.add((scope.database.folded, scope.schema.folded))
        return frozenset(absent)

    def _observed_artifact_types(
        self,
        rendered: Mapping[str, RenderedArtifact],
        include_prune: bool,
        prune_types: frozenset[str] | None,
    ) -> frozenset[str]:
        """Return the types to observe: those rendered, those they depend on, and those a prune may remove.

        A dependency's type is observed even when `--select` leaves every artifact of it out, so
        a dependency that exists is seen to exist: only one Snowflake lacks blocks the write
        that depends on it (SST-PLN038).
        """
        prunable: set[str] = set()
        if include_prune:
            prunable = set(prune_types) if prune_types is not None else set(self._registry.artifacts)
        depended = {
            split_artifact_key(dependency)[0] for artifact in rendered.values() for dependency in artifact.depends_on
        }
        return frozenset(
            {artifact.artifact_type for artifact in rendered.values()}
            | (depended & set(self._registry.artifacts))
            | prunable
        )

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
    overwrite: bool = False,
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
        overwrite: `--overwrite`: update each object changed out of band instead of blocking it.
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
        overwrite=overwrite,
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


def targets_by_kind(rendered: Iterable[RenderedArtifact], state: State) -> Mapping[str, tuple[QualifiedName, ...]]:
    """Return the objects of each artifact type: each rendered target, then each state records.

    State's objects count too, so an object whose source was deleted is still observed under
    the kind that published it. An entry whose key names no kind is left out.
    """
    found: dict[str, dict[QualifiedName, None]] = {}
    for artifact in rendered:
        found.setdefault(artifact.artifact_type, {})[artifact.target] = None
    for key, entry in state.applied.items():
        kind, separator, _ = key.partition(":")
        if separator and entry.qualified_name:
            found.setdefault(kind, {})[QualifiedName.parse(entry.qualified_name)] = None
    return MappingProxyType({kind: tuple(targets) for kind, targets in found.items()})
