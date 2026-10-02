"""Observe live state and compute a deterministic, non-writing ChangeSet.

`PlanArtifacts` plans rendered artifacts against what `observe` finds in Snowflake.
`PreparePlan` is the plan and apply commands' use case around it: it decides what a plan
covers, validates it, reads authoritative state, renders what publishes, and builds the
composite artifacts' lifecycle handlers, returning `PlanReady` or `PlanRefused`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleConfig, EvalLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.profiles import ProfileLifecycleHandler, ProfilePublicationPort
from snowflake_semantic_tools.app.manifest import manifest_for, stale_manifest, target_mismatch
from snowflake_semantic_tools.app.partial import PartialSplit, partial_refusal, partial_split
from snowflake_semantic_tools.app.state import change_summary, read_state
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_text
from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
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
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, ArtifactType, Registry
from snowflake_semantic_tools.domain.plan import build_changeset
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import DEACTIVATED, Manifest, State

_FoldedName = tuple[str, str, str]


def observe(
    port: CatalogPort,
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


class PlanArtifacts:
    """Plan the changes that bring a target to the rendered artifacts, writing nothing.

    Composite artifacts are planned by their lifecycle handlers, keyed by artifact type; every
    other artifact is planned from what `observe` finds in Snowflake.
    """

    def __init__(
        self,
        port: CatalogPort,
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


@dataclass(frozen=True, slots=True)
class PlanScope:
    """Which artifacts a plan covers and which it may prune, as the selectors resolved.

    `prune_types` and `prune_keys` serve twice: an artifact either names is covered, and a
    prune acts only on what they name. When both are None, every artifact is covered.

    Attributes:
        selected: The selectors as given, which the saved plan records.
        prune_types, prune_keys: The artifact types and keys selected, less what `--exclude`
            names; None when the selectors name none.
        excluded_types, excluded_keys: What `--exclude` leaves out; None leaves nothing out.
    """

    selected: tuple[str, ...]
    prune_types: frozenset[str] | None
    prune_keys: frozenset[str] | None
    excluded_types: frozenset[str] | None
    excluded_keys: frozenset[str] | None
    include_prune: bool

    def covers(self, item: CompiledArtifact) -> bool:
        """Report whether the plan covers an artifact: selected by type or key, and not excluded."""
        selected = (
            (self.prune_types is None and self.prune_keys is None)
            or (self.prune_types is not None and item.artifact_type in self.prune_types)
            or (self.prune_keys is not None and item.artifact_key in self.prune_keys)
        )
        return (
            selected
            and (self.excluded_types is None or item.artifact_type not in self.excluded_types)
            and (self.excluded_keys is None or item.artifact_key not in self.excluded_keys)
        )


@dataclass(frozen=True, slots=True)
class PlanCandidates:
    """What a plan may publish, decided offline, before Snowflake is reached.

    Attributes:
        full: Everything that compiled; the lifecycle handlers and the observed schemas read
            all of it, whatever the selection.
        source: What may publish: with `--partial`, the compile split's healthy part, else `full`.
        selected: `source` narrowed to the scope, still carrying `source`'s diagnostics.
        split: The compile split, when `--partial` split the compile result.
        manifest: The manifest `source` publishes, which is the one `sst compile` wrote.
        strict, connected: How validation runs, with the flags applied over `validation:`.
    """

    full: CompileResult
    source: CompileResult
    selected: CompileResult
    split: PartialSplit | None
    manifest: Manifest
    scope: PlanScope
    partial: bool
    strict: bool
    connected: bool


@dataclass(frozen=True, slots=True)
class PlanReady:
    """A plan ready to save or apply, with what it was made from.

    Attributes:
        result: The selection as validated. With `--partial`, narrowed to what stayed healthy,
            and carrying validation's diagnostics followed by one SST-PLN032 per artifact
            left out; otherwise the selection with the compile's diagnostics.
        state: The authoritative state the plan was made from.
        changeset: The plan, with what reading state reported ahead of its own diagnostics.
        lifecycle_handlers: The composite artifact types' handlers, by type, which apply reuses.
    """

    result: CompileResult
    manifest: Manifest
    state: State
    changeset: ChangeSet
    lifecycle_handlers: Mapping[str, CompositeLifecycleHandler]


@dataclass(frozen=True, slots=True)
class PlanRefused:
    """Why nothing was planned: errors a run reports, or a project that cannot be planned at all.

    Attributes:
        diagnostics: The errors that stopped the plan, compile's or validation's.
        reason: Set, with no diagnostics, when the project cannot be planned at all -- the
            selectors matched nothing, or the compile is stale -- which the CLI reports as a
            configuration error rather than as findings.
    """

    diagnostics: DiagnosticBag = DiagnosticBag()
    reason: str | None = None


class PreparePlan:
    """Decide what a plan covers, then validate it, read state, and observe Snowflake for it.

    Two steps, because the caller opens the connection between them: `select` decides
    offline whether anything can be planned, and `run` plans over the open connection. The
    connection belongs to the caller, which closes it. Neither step writes to Snowflake.
    """

    def __init__(self, inputs: ProjectInputs, clock: ClockPort) -> None:
        self._inputs = inputs
        self._clock = clock

    def select(
        self,
        full: CompileResult,
        compiled_manifest: Manifest,
        scope: PlanScope,
        *,
        partial: bool,
        strict: bool | None,
        connected: bool | None,
        project: str,
    ) -> PlanCandidates | PlanRefused:
        """Decide what may publish and whether the compile it comes from is current.

        Steps, in order:

        1. With `--partial` and a failed compile, split off what can still publish.
        2. Narrow that to the scope; refuse when selectors matched nothing and nothing prunes.
        3. Refuse on a compile error the split did not set aside, adding SST-PLN033 with
           `--partial` when the error names no artifact.
        4. Read the `validation:` defaults, and build the manifest of what may publish.
        5. Refuse when it is not the manifest `sst compile` wrote, naming the targets when
           `sst compile` wrote it for another one.

        Args:
            compiled_manifest: The manifest `sst compile` wrote.
            strict, connected: The `--strict` and `--snowflake-syntax-check` flags; None
                defers to `validation:`.
            project: How the refusal for selectors that matched nothing names the project.

        Diagnostics:
            SST-PLN033: with `--partial`, a compile error names no artifact, so nothing can
                be split off.
            SST-MAN006: the compiled manifest was written for another target.
        """
        split = partial_split(full) if partial and not full.success else None
        source = split.healthy if split is not None else full
        selected = replace(source, compiled=tuple(item for item in source.compiled if scope.covers(item)))
        if scope.selected and not selected.compiled and not scope.include_prune:
            return PlanRefused(reason=f"selectors {scope.selected!r} matched no artifact in {project}")
        if not selected.success and split is None:
            refusal = partial_refusal(full) if partial else None
            return PlanRefused(DiagnosticBag((*selected.diagnostics, *((refusal,) if refusal else ()))))
        effective_strict, effective_connected = self._inputs.validation_defaults().resolve(strict, connected)
        manifest = manifest_for(source, self._inputs.manifest_sources())
        mismatch = target_mismatch(compiled_manifest, manifest)
        if mismatch is not None:
            return PlanRefused(DiagnosticBag((mismatch,)))
        stale = stale_manifest(compiled_manifest, manifest, before="plan or apply")
        if stale is not None:
            return PlanRefused(reason=stale)
        return PlanCandidates(
            full, source, selected, split, manifest, scope, partial, effective_strict, effective_connected
        )

    def run(
        self,
        candidates: PlanCandidates,
        port: SnowflakePort,
        state_store: StateStore,
        *,
        target: TargetIdentity,
        state_table: QualifiedName,
        temporary: bool = False,
    ) -> PlanReady | PlanRefused:
        """Validate the candidates, read authoritative state, and plan against what Snowflake shows now.

        Steps, in order:

        1. Validate the selection, with connected checks when the settings ask for them.
        2. With `--partial`, split again on what validation reported and narrow the selection
           to what is still healthy; otherwise refuse on an error.
        3. Read authoritative state, as `read_state` does.
        4. Render what publishes for the manifest, each agent staged under
           `apply.agent_spec_stage`.
        5. Build the lifecycle handlers of the composite artifact types.
        6. Observe and plan, as `PlanArtifacts` does, in the schemas of every compiled
           artifact and of every object state records.

        Args:
            target: The live target, with the account and role the connection reported.
            temporary: Render each agent as a session-scoped temporary agent, as
                `apply --temporary` publishes it.

        Diagnostics:
            SST-PLN032: with `--partial`, an artifact is left out of the plan.
            SST-PLN033: with `--partial`, a validation error names no artifact.
            Those of validation, of `read_state`, and of `PlanArtifacts`, then the plan's
            `change_summary`.
        """
        validation = ValidateArtifacts(port if candidates.connected else None).run(
            candidates.selected,
            strict=candidates.strict,
            connected=candidates.connected,
        )
        result = _validated(candidates, validation.diagnostics)
        if isinstance(result, PlanRefused):
            return result
        state, state_diagnostics = read_state(state_store, port, state_table=state_table, target=target)
        apply_config = config_block(self._inputs.config().tree.get("apply"))
        publish = self._publication(result, candidates.manifest, apply_config, target, temporary=temporary)
        handlers = _lifecycle_handlers(port, candidates.full, apply_config)
        observation_targets = _observation_targets(candidates.full, state)
        scope = candidates.scope
        changeset = PlanArtifacts(port, lifecycle_handlers=handlers).run(
            publish,
            candidates.manifest,
            state,
            target,
            fetched_at=self._clock.now_iso(),
            include_prune=scope.include_prune,
            prune_types=scope.prune_types,
            prune_keys=scope.prune_keys,
            observation_targets=observation_targets,
        )
        summary = change_summary(changeset)
        changeset = replace(
            changeset, diagnostics=DiagnosticBag((*state_diagnostics, *changeset.diagnostics, *summary))
        )
        return PlanReady(result, candidates.manifest, state, changeset, MappingProxyType(handlers))

    def _publication(
        self,
        result: CompileResult,
        manifest: Manifest,
        apply_config: Mapping[str, object],
        target: TargetIdentity,
        *,
        temporary: bool = False,
    ) -> dict[str, RenderedArtifact]:
        """Render each selected artifact as apply publishes it, by key, agents staged for publication.

        Each agent is staged under `apply.agent_spec_stage`, in the target's database and
        schema unless the block names others, beneath a folder for the project's commit.
        """
        stage_config = config_block(apply_config.get("agent_spec_stage"))
        database = target.database.folded
        schema = target.schema.folded
        stage = QualifiedName.from_parts(
            config_text(stage_config.get("database"), database) or database,
            config_text(stage_config.get("schema"), schema) or schema,
            str(stage_config.get("stage") or "AGENT_SPECS"),
        )
        compiled = tuple(
            (
                for_publication(item, stage=stage, git_sha=self._inputs.git_sha(), temporary=temporary)
                if isinstance(item, CompiledAgent)
                else item
            )
            for item in result.compiled
        )
        return {
            artifact.key: artifact
            for artifact in replace(result, compiled=compiled).rendered_for_publish(manifest.manifest_id)
        }


def _validated(candidates: PlanCandidates, diagnostics: DiagnosticBag) -> CompileResult | PlanRefused:
    """Apply validation's findings: narrow a partial plan to what stays healthy, else refuse on an error.

    The split walks the whole healthy set, so a selection cannot hide a dependency; its
    result is then narrowed back to what was selected.
    """
    selected = candidates.selected
    validated = partial_split(replace(candidates.source, diagnostics=diagnostics)) if candidates.partial else None
    if validated is not None:
        # Strict promotion or a connected check can exclude more; the notices name
        # everything left out, including what the compile split already excluded.
        compile_excluded = candidates.split.excluded if candidates.split is not None else ()
        left_out = dict.fromkeys((*compile_excluded, *validated.excluded))
        notices = tuple(D("SST-PLN032", subject=key, artifact=key) for key in left_out)
        still_healthy = {item.artifact_key for item in validated.healthy.compiled}
        return replace(
            selected,
            compiled=tuple(item for item in selected.compiled if item.artifact_key in still_healthy),
            diagnostics=DiagnosticBag((*diagnostics, *notices)),
        )
    if diagnostics.has_errors:
        refusal = partial_refusal(replace(selected, diagnostics=diagnostics)) if candidates.partial else None
        return PlanRefused(DiagnosticBag((*diagnostics, *((refusal,) if refusal else ()))))
    return selected


def _lifecycle_handlers(
    port: ProfilePublicationPort, full: CompileResult, apply_config: Mapping[str, object]
) -> dict[str, CompositeLifecycleHandler]:
    """Build each composite artifact type's handler over everything that compiled, by type."""
    eval_stage_config = config_block(apply_config.get("eval_config_stage"))
    releases = {item.artifact_key: item.release for item in full.compiled if isinstance(item, CompiledExtension)}
    profiles = {item.artifact_key: item for item in full.compiled if isinstance(item, CompiledProfile)}
    return {
        "eval": EvalLifecycleHandler(
            port, EvalLifecycleConfig(str(eval_stage_config.get("stage") or DEFAULT_EVAL_CONFIG_STAGE))
        ),
        "skill": ExtensionLifecycleHandler(port, releases, "skill"),
        "plugin": ExtensionLifecycleHandler(port, releases, "plugin"),
        "profile": ProfileLifecycleHandler(port, profiles),
    }


def _observation_targets(full: CompileResult, state: State) -> tuple[QualifiedName, ...]:
    """Return every object a plan observes the schema of: each compiled target, then each state records.

    State's objects are observed too, so an object whose source was deleted is still seen.
    """
    return tuple(
        dict.fromkeys(
            (
                *(artifact.target for artifact in full.rendered),
                *(
                    QualifiedName.parse(entry.qualified_name)
                    for entry in state.applied.values()
                    if entry.qualified_name
                ),
            )
        )
    )
