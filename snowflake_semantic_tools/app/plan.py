"""Observe live state and compute a deterministic, non-writing ChangeSet.

`PlanArtifacts` plans rendered artifacts against what `app.observe` finds in Snowflake and,
given a preflight port, what `app.preflight` reads about the target.
`PreparePlan` is the plan and apply commands' use case around it: it decides what a plan
covers, validates it, reads authoritative state, renders what publishes, and builds the
composite artifacts' lifecycle handlers, returning `PlanReady` or `PlanRefused`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.lifecycle.channels import channel_divergence
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleConfig, EvalLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.profiles import ProfileLifecycleHandler, ProfilePublicationPort
from snowflake_semantic_tools.app.manifest import manifest_for, stale_manifest, target_mismatch
from snowflake_semantic_tools.app.observe import observe
from snowflake_semantic_tools.app.partial import PartialSplit, partial_refusal, partial_split
from snowflake_semantic_tools.app.preflight import read_preflight
from snowflake_semantic_tools.app.state import change_summary, read_state
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_text
from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Change,
    ChangeSet,
    CompositePlan,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, Registry
from snowflake_semantic_tools.domain.plan import build_changeset
from snowflake_semantic_tools.domain.plan.impact import impact_scope
from snowflake_semantic_tools.domain.plan.selectors import Selection, SelectionScope
from snowflake_semantic_tools.domain.plan.summary import plan_notices
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import DEACTIVATED, Manifest, State

# How long an observation stays current. A plan that takes longer to observe and decide
# reports SST-PLN018, because what it observed first may already have changed.
OBSERVATION_TTL_MS = 15 * 60 * 1000


class PlanReadPort(SnowflakePort, PreflightPort, Protocol):
    """A session a plan reads on: validation's checks, observation, and the preflight reads."""


class PlanArtifacts:
    """Plan the changes that bring a target to the rendered artifacts, writing nothing.

    Composite artifacts are planned by their lifecycle handlers, keyed by artifact type; every
    other artifact is planned from what `observe` finds in Snowflake.

    Args:
        readers: Sessions, opened from `port` and `preflight`, to run the observation's
            listings and each phase of preflight reads on concurrently; None reads on `port`
            and `preflight`, one at a time. Either way the change set is the same.
    """

    def __init__(
        self,
        port: CatalogPort,
        *,
        registry: Registry = SEMANTIC_REGISTRY,
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
        preflight: PreflightPort | None = None,
        readers: Fanout[PlanReadPort] | None = None,
    ) -> None:
        self._port = port
        self._registry = registry
        self._lifecycle_handlers = dict(lifecycle_handlers or {})
        self._preflight = preflight
        self._readers = readers

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
        3. The preflight read, when the use case was given a preflight port, as
           `read_preflight` does.
        4. The change set, from the observation, the preflight, the manifest and state.
        5. Composite prunes, when pruning: a composite artifact state records, that nothing
           rendered or planned names, is reported by its handler as a prune.
        6. The observation's and the preflight's failures, ahead of every other diagnostic.

        Args:
            blocked: Diagnostics by artifact key that make its change BLOCKED.
            prune_types, prune_keys: Narrow the prunes to these artifact types and keys; None
                does not narrow.
            observation_targets: The objects whose schemas are observed; None observes the
                schemas of the rendered artifacts.

        Diagnostics:
            SST-PLN001: observing Snowflake, or a preflight read, failed.
        """
        composite_plans = self._composite_plans(rendered, manifest, state)
        observation, diagnostics = self._observe(rendered, fetched_at, include_prune, prune_types, observation_targets)
        preflight = None
        if self._preflight is not None:
            preflight, failures = read_preflight(
                self._preflight,
                rendered,
                observation,
                state,
                target,
                include_prune=include_prune,
                readers=self._readers,
            )
            diagnostics = DiagnosticBag((*diagnostics, *failures))
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
            preflight=preflight,
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
            readers=self._readers,
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
        impact: Whether `state:modified` narrows the plan to what changed since the previous
            manifest.
    """

    selected: tuple[str, ...]
    prune_types: frozenset[str] | None
    prune_keys: frozenset[str] | None
    excluded_types: frozenset[str] | None
    excluded_keys: frozenset[str] | None
    include_prune: bool
    impact: bool = False

    def covers(self, item: CompiledArtifact) -> bool:
        """Report whether the plan covers an artifact: selected by type or key, and not excluded."""
        named = self.prune_types is not None or self.prune_keys is not None
        scope = SelectionScope(
            Selection(self.prune_types, self.prune_keys) if named else None,
            Selection(self.excluded_types, self.excluded_keys),
        )
        return scope.covers(item.artifact_type, item.artifact_key)


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
        notices: What deciding the scope reported, which the plan reports first.
        covers_all: False when `state:modified` narrowed the plan to what changed.
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
    notices: tuple[Diagnostic, ...] = ()
    covers_all: bool = True


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

    @property
    def restamps_state(self) -> bool:
        """Report whether applying the plan would change state though it executes nothing.

        A report-only prune executes nothing, but apply records it under this plan's
        manifest; until it has, applying the plan changes the state table.
        """
        return any(
            (entry := self.state.applied.get(change.key)) is None or entry.manifest_id != self.manifest.manifest_id
            for change in self.changeset.report_only
        )


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
        previous_manifest: Manifest | None = None,
    ) -> PlanCandidates | PlanRefused:
        """Decide what may publish and whether the compile it comes from is current.

        Steps, in order:

        1. With `--partial` and a failed compile, split off what can still publish.
        2. Narrow that to the scope, and with `state:modified` to what changed since
           `previous_manifest`; refuse when selectors matched nothing and nothing prunes.
        3. Refuse on a compile error the split did not set aside, adding SST-PLN033 with
           `--partial` when the error names no artifact.
        4. Read the `validation:` defaults, and build the manifest of what may publish;
           `state:modified` builds it first, to compare it with `previous_manifest`.
        5. Refuse when it is not the manifest `sst compile` wrote, naming the targets when
           `sst compile` wrote it for another one.

        Args:
            compiled_manifest: The manifest `sst compile` wrote.
            strict, connected: The `--strict` and `--snowflake-syntax-check` flags; None
                defers to `validation:`.
            project: How the refusal for selectors that matched nothing names the project.
            previous_manifest: The manifest `state:modified` compares with; None when there
                is none.

        Diagnostics:
            SST-PLN006: `state:modified` was asked for without a previous manifest, so the
                plan covers everything.
            SST-PLN033: with `--partial`, a compile error names no artifact, so nothing can
                be split off.
            SST-MAN006: the compiled manifest was written for another target.
        """
        split = partial_split(full) if partial and not full.success else None
        source = split.healthy if split is not None else full
        # Only `state:modified` needs the manifest before the refusals; otherwise it is
        # built once something can be planned.
        manifest = manifest_for(source, self._inputs.manifest_sources()) if scope.impact else None
        impact, notice = impact_scope(previous_manifest, manifest) if manifest is not None else (None, None)
        selected = replace(
            source,
            compiled=tuple(
                item
                for item in source.compiled
                if scope.covers(item) and (impact is None or item.artifact_key in impact)
            ),
        )
        if scope.selected and not scope.impact and not selected.compiled and not scope.include_prune:
            return PlanRefused(reason=f"selectors {scope.selected!r} matched no artifact in {project}")
        if not selected.success and split is None:
            refusal = partial_refusal(full) if partial else None
            return PlanRefused(DiagnosticBag((*selected.diagnostics, *((refusal,) if refusal else ()))))
        effective_strict, effective_connected = self._inputs.validation_defaults().resolve(strict, connected)
        manifest = manifest or manifest_for(source, self._inputs.manifest_sources())
        mismatch = target_mismatch(compiled_manifest, manifest)
        if mismatch is not None:
            return PlanRefused(DiagnosticBag((mismatch,)))
        stale = stale_manifest(compiled_manifest, manifest, before="plan or apply")
        if stale is not None:
            return PlanRefused(reason=stale)
        return PlanCandidates(
            full,
            source,
            selected,
            split,
            manifest,
            scope,
            partial,
            effective_strict,
            effective_connected,
            notices=(notice,) if notice is not None else (),
            covers_all=impact is None,
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
        preflight: PreflightPort | None = None,
        readers: Fanout[PlanReadPort] | None = None,
    ) -> PlanReady | PlanRefused:
        """Validate the candidates, read authoritative state, and plan against what Snowflake shows now.

        Steps, in order:

        1. Validate the selection, with connected checks when the settings ask for them.
        2. With `--partial`, split again on what validation reported, narrow the selection
           to what is still healthy, and build the manifest of what is; otherwise refuse on
           an error.
        3. Read authoritative state, as `read_state` does.
        4. Render what publishes for the manifest, each agent staged under
           `apply.agent_spec_stage`.
        5. Build the lifecycle handlers of the composite artifact types.
        6. Observe, preflight when given `preflight`, and plan, as `PlanArtifacts` does, in
           the schemas of every compiled artifact and of every object state records.
        7. Report what selecting reported first, and the plan's notices last.

        Args:
            target: The live target, with the account and role the connection reported.
            temporary: Render each agent as a session-scoped temporary agent, as
                `apply --temporary` publishes it.
            preflight: The port the preflight checks read the target through; None skips them.
            readers: Sessions, opened from `port`, to run the connected validation checks,
                the observation, and the preflight reads on concurrently, as
                `ValidateArtifacts` and `PlanArtifacts` do; None runs them on `port`, one at a
                time. Either way the plan and its diagnostics are the same.

        Diagnostics:
            SST-PLN018: observing and planning took longer than the observation stays current.
            SST-PLN032: with `--partial`, an artifact is left out of the plan.
            SST-PLN033: with `--partial`, a validation error names no artifact.
            Those of validation, of `read_state`, and of `PlanArtifacts`, then
            `channel_divergence`'s, then the plan's `change_summary` and `plan_notices`.
        """
        live = port if candidates.connected else None
        validation = ValidateArtifacts(
            live, catalog=live, target=target.name, readers=readers if candidates.connected else None
        ).run(
            candidates.selected,
            strict=candidates.strict,
            connected=candidates.connected,
        )
        validated = _validated(candidates, validation.diagnostics)
        if isinstance(validated, PlanRefused):
            return validated
        result, healthy = validated
        manifest = candidates.manifest if healthy is None else manifest_for(healthy, self._inputs.manifest_sources())
        state, state_diagnostics = read_state(state_store, port, state_table=state_table, target=target)
        apply_config = config_block(self._inputs.config().tree.get("apply"))
        handlers = _lifecycle_handlers(port, candidates.full, apply_config)
        scope = candidates.scope
        fetched_at = self._clock.now_iso()
        started = self._clock.monotonic_ms()
        changeset = PlanArtifacts(port, lifecycle_handlers=handlers, preflight=preflight, readers=readers).run(
            self._publication(result, manifest, apply_config, target, temporary=temporary),
            manifest,
            state,
            target,
            fetched_at=fetched_at,
            include_prune=scope.include_prune,
            full=candidates.covers_all,
            prune_types=scope.prune_types,
            prune_keys=scope.prune_keys,
            observation_targets=_observation_targets(candidates.full, state),
        )
        stale = stale_observation(target, self._clock.monotonic_ms() - started)
        leading = (*candidates.notices, *state_diagnostics, *((stale,) if stale else ()))
        trailing = (*channel_divergence(port, result), *change_summary(changeset), *plan_notices(changeset))
        changeset = replace(changeset, diagnostics=DiagnosticBag((*leading, *changeset.diagnostics, *trailing)))
        return PlanReady(result, manifest, state, changeset, MappingProxyType(handlers))

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
        schema unless the block names others, beneath a folder for the project's commit; each
        eval's dataset version records that commit.
        """
        stage_config = config_block(apply_config.get("agent_spec_stage"))
        database = target.database.folded
        schema = target.schema.folded
        stage = QualifiedName.from_parts(
            config_text(stage_config.get("database"), database) or database,
            config_text(stage_config.get("schema"), schema) or schema,
            str(stage_config.get("stage") or "AGENT_SPECS"),
        )
        git_sha = self._inputs.git_sha()
        compiled = tuple(
            (
                for_publication(item, stage=stage, git_sha=git_sha, temporary=temporary)
                if isinstance(item, CompiledAgent)
                # A dataset version records the commit in its METADATA.
                else replace(item, git_sha=git_sha)
                if isinstance(item, CompiledEval)
                else item
            )
            for item in result.compiled
        )
        return {
            artifact.key: artifact
            for artifact in replace(result, compiled=compiled).rendered_for_publish(manifest.manifest_id)
        }


def _validated(
    candidates: PlanCandidates, diagnostics: DiagnosticBag
) -> tuple[CompileResult, CompileResult | None] | PlanRefused:
    """Apply validation's findings: narrow a partial plan to what stays healthy, else refuse on an error.

    The split walks the whole healthy set, so a selection cannot hide a dependency; its
    result is then narrowed back to what was selected.

    Returns:
        The selection as validated, and with `--partial` the healthy part of the source
        that the plan's manifest is built from when validation left out more than compile
        did; otherwise None, and the candidates' manifest stands.
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
        narrowed = replace(
            selected,
            compiled=tuple(item for item in selected.compiled if item.artifact_key in still_healthy),
            diagnostics=DiagnosticBag((*diagnostics, *notices)),
        )
        source = candidates.source
        healthy_source = replace(
            source, compiled=tuple(item for item in source.compiled if item.artifact_key in still_healthy)
        )
        return narrowed, (healthy_source if len(healthy_source.compiled) != len(source.compiled) else None)
    if diagnostics.has_errors:
        refusal = partial_refusal(replace(selected, diagnostics=diagnostics)) if candidates.partial else None
        return PlanRefused(DiagnosticBag((*diagnostics, *((refusal,) if refusal else ()))))
    return selected, None


def stale_observation(target: TargetIdentity, elapsed_ms: int) -> Diagnostic | None:
    """Report an observation that went stale while plan used it; None while it is current.

    Diagnostics:
        SST-PLN018: observing and planning took longer than `OBSERVATION_TTL_MS`.
    """
    if elapsed_ms <= OBSERVATION_TTL_MS:
        return None
    return D(
        "SST-PLN018",
        value=f"target '{target.name}'",
        detail=f"{elapsed_ms // 1000}s old, past the {OBSERVATION_TTL_MS // 1000}s it stays current",
    )


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
