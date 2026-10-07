"""Observe live state and compute a deterministic, non-writing ChangeSet.

`PreparePlan` is the plan and apply commands' use case around `app.plan_artifacts`: it
decides what a plan covers, validates it, reads authoritative state, renders what publishes,
and builds the composite artifacts' lifecycle handlers, returning `PlanReady` or
`PlanRefused`. `PreparePlan.run` reads the live target; `PreparePlan.run_recorded` plans
from what an earlier plan recorded of it instead, and reads nothing.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.baseline import blocking, held_by_baseline
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
from snowflake_semantic_tools.app.observation_age import duration, seconds_between
from snowflake_semantic_tools.app.observe import DEFAULT_OBSERVE_OPTIONS, ObserveOptions
from snowflake_semantic_tools.app.partial import PartialSplit, partial_refusal, partial_split
from snowflake_semantic_tools.app.plan_artifacts import (
    PlanArtifacts,
    PlanReadPort,
    TargetReading,
    plan_changes,
    unrecorded_composites,
)
from snowflake_semantic_tools.app.policy import strict_reach
from snowflake_semantic_tools.app.state import change_summary, read_state
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline
from snowflake_semantic_tools.domain.diagnostics.policy import apply_overrides
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_text
from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import ChangeSet, CompositePlan, RenderedArtifact
from snowflake_semantic_tools.domain.plan.impact import impact_scope
from snowflake_semantic_tools.domain.plan.recorded import RecordedObservation
from snowflake_semantic_tools.domain.plan.selectors import Selection, SelectionScope
from snowflake_semantic_tools.domain.plan.summary import plan_notices
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import Manifest, State
from snowflake_semantic_tools.domain.validate.config import severity_overrides

# How long an observation stays current. A plan that takes longer to observe and decide
# reports SST-PLN018, because what it observed first may already have changed.
OBSERVATION_TTL_MS = 15 * 60 * 1000


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
        return self.selection.covers(item.artifact_type, item.artifact_key)

    @property
    def selection(self) -> SelectionScope:
        """Return what `--select` and `--exclude` choose, as every command applies it."""
        named = self.prune_types is not None or self.prune_keys is not None
        return SelectionScope(
            Selection(self.prune_types, self.prune_keys) if named else None,
            Selection(self.excluded_types, self.excluded_keys),
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
class CachedObservation:
    """The recorded observation a plan was made from, and how old it was when the plan used it.

    Attributes:
        fetched_at: When the observation was taken.
        age_seconds: How long before the plan that was; None when the time does not parse.
    """

    fetched_at: str
    age_seconds: float | None

    @property
    def age(self) -> str:
        """The age as hours and minutes, such as `2h 05m`, or `unknown`."""
        return duration(self.age_seconds) if self.age_seconds is not None else "unknown"


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
        recorded: What the plan read of the live target, for a later plan to reuse; None when
            a read failed, or the plan was made from a record.
        cached: The record the plan was made from instead of reading the target; None when
            it read the live target.
    """

    result: CompileResult
    manifest: Manifest
    state: State
    changeset: ChangeSet
    lifecycle_handlers: Mapping[str, CompositeLifecycleHandler]
    recorded: RecordedObservation | None = None
    cached: CachedObservation | None = None

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

    Args:
        baseline: The run's baseline. A validation error it holds -- a warning `--strict`
            promoted -- does not refuse the plan, as it does not fail `sst validate`.
    """

    def __init__(self, inputs: ProjectInputs, clock: ClockPort, *, baseline: Baseline | None = None) -> None:
        self._inputs = inputs
        self._clock = clock
        self._baseline = baseline

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
        observe_options: ObserveOptions = DEFAULT_OBSERVE_OPTIONS,
        validate: bool = True,
    ) -> PlanReady | PlanRefused:
        """Validate the candidates, read authoritative state, and plan against what Snowflake shows now.

        Steps, in order:

        1. Validate the selection, with connected checks when the settings ask for them;
           without `validate`, take the compile's diagnostics as they are.
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
            observe_options: Which per-object reads the observation makes, as `ObserveOptions` says.
            validate: False skips validation, as `--no-validate` asks once `sst validate` ran on
                the same tree: no cycle check, no connected check, and no strict promotion. The
                project's severity overrides still apply, and the compile's own errors still
                refuse the plan, or with `--partial` split it.

        Diagnostics:
            SST-PLN018: observing and planning took longer than the observation stays current.
            SST-PLN032: with `--partial`, an artifact is left out of the plan.
            SST-PLN033: with `--partial`, a validation error names no artifact.
            Those of validation, of `read_state`, and of `PlanArtifacts`, then
            `channel_divergence`'s, then the plan's `change_summary` and `plan_notices`.
        """
        validated = self._validated_selection(candidates, port, target, readers, validate)
        if isinstance(validated, PlanRefused):
            return validated
        result, manifest = validated
        state, state_diagnostics = read_state(state_store, port, state_table=state_table, target=target)
        apply_config = config_block(self._inputs.config().tree.get("apply"))
        handlers = _lifecycle_handlers(port, candidates.full, apply_config)
        scope = candidates.scope
        rendered = self._publication(result, manifest, apply_config, target, temporary=temporary)
        fetched_at = self._clock.now_iso()
        started = self._clock.monotonic_ms()
        planner = PlanArtifacts(
            port, lifecycle_handlers=handlers, preflight=preflight, readers=readers, observe_options=observe_options
        )
        composite_plans = planner.composite_plans(rendered, manifest, state)
        reading = planner.read(
            rendered,
            state,
            target,
            fetched_at=fetched_at,
            include_prune=scope.include_prune,
            prune_types=scope.prune_types,
            observation_targets=_observation_targets(candidates.full, state),
        )
        changeset = _decided(candidates, rendered, reading, manifest, state, target, handlers, composite_plans)
        stale = stale_observation(target, self._clock.monotonic_ms() - started)
        # A reading with a refused read is incomplete, so no later plan may reuse it. An advisory
        # read the role may not make (SST-VAL020) is as complete as the role can ever read it.
        recorded = RecordedObservation(target, reading.observation, state, reading.preflight)
        ready = _ready(
            candidates,
            result,
            manifest,
            state,
            changeset,
            handlers,
            leading=(*state_diagnostics, *((stale,) if stale else ())),
            trailing=channel_divergence(port, result),
        )
        return replace(ready, recorded=None if reading.failures.has_errors else recorded)

    def run_recorded(
        self,
        candidates: PlanCandidates,
        recorded: RecordedObservation,
        *,
        temporary: bool = False,
        validate: bool = True,
    ) -> PlanReady | PlanRefused:
        """Plan the candidates from what an earlier plan recorded of the target, reading nothing.

        The steps are `run`'s, with the record in place of every read: validation runs without
        its connected checks, the record's state stands for the authoritative state, and its
        observation and preflight facts for the target's. Composite artifacts, whose handlers
        observe them live and so are never recorded, are blocked; nothing reports their prunes.
        The plan says what the target held when the record was taken, so its age is reported
        first, however old it is.

        Diagnostics:
            SST-PLN016: when the recorded observation was taken, and how old it is.
            SST-PLN001: a composite artifact, which the record cannot decide.
            Those of validation, then the plan's `change_summary` and `plan_notices`.
        """
        offline = replace(candidates, connected=False)
        validated = self._validated_selection(offline, None, recorded.target, None, validate)
        if isinstance(validated, PlanRefused):
            return validated
        result, manifest = validated
        target, state = recorded.target, recorded.state
        apply_config = config_block(self._inputs.config().tree.get("apply"))
        rendered = self._publication(result, manifest, apply_config, target, temporary=temporary)
        reading = TargetReading(recorded.observation, recorded.preflight)
        composites = unrecorded_composites(rendered)
        changeset = _decided(candidates, rendered, reading, manifest, state, target, {}, composites)
        cached = CachedObservation(recorded.fetched_at, seconds_between(recorded.fetched_at, self._clock.now_iso()))
        age = D("SST-PLN016", value=f"planned from the observation recorded at {cached.fetched_at}, {cached.age} old")
        ready = _ready(candidates, result, manifest, state, changeset, {}, leading=(age,), trailing=())
        return replace(ready, cached=cached)

    def _validated_selection(
        self,
        candidates: PlanCandidates,
        port: SnowflakePort | None,
        target: TargetIdentity,
        readers: Fanout[PlanReadPort] | None,
        validate: bool,
    ) -> tuple[CompileResult, Manifest] | PlanRefused:
        """Validate the selection as `run` says, and return it with the manifest of what publishes."""
        overrides = severity_overrides(self._inputs.config().tree)
        diagnostics = (
            _validation(candidates, port, target, readers, overrides)
            if validate
            else apply_overrides(candidates.selected.diagnostics, overrides)
        )
        held = held_by_baseline(diagnostics, self._baseline, clock=self._clock)
        validated = _validated(candidates, diagnostics, held)
        if isinstance(validated, PlanRefused):
            return validated
        result, healthy = validated
        manifest = candidates.manifest if healthy is None else manifest_for(healthy, self._inputs.manifest_sources())
        return result, manifest

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


def _decided(
    candidates: PlanCandidates,
    rendered: Mapping[str, RenderedArtifact],
    reading: TargetReading,
    manifest: Manifest,
    state: State,
    target: TargetIdentity,
    handlers: Mapping[str, CompositeLifecycleHandler],
    composite_plans: Mapping[str, CompositePlan],
) -> ChangeSet:
    """Decide the candidates' change set from a reading, within their scope."""
    scope = candidates.scope
    return plan_changes(
        rendered,
        reading,
        manifest,
        state,
        target,
        lifecycle_handlers=handlers,
        composite_plans=composite_plans,
        include_prune=scope.include_prune,
        full=candidates.covers_all,
        prune_types=scope.prune_types,
        prune_keys=scope.prune_keys,
    )


def _ready(
    candidates: PlanCandidates,
    result: CompileResult,
    manifest: Manifest,
    state: State,
    changeset: ChangeSet,
    handlers: Mapping[str, CompositeLifecycleHandler],
    *,
    leading: tuple[Diagnostic, ...],
    trailing: tuple[Diagnostic, ...],
) -> PlanReady:
    """Return the ready plan: what selecting reported and `leading` first, the plan's notices last."""
    first = (*candidates.notices, *leading)
    last = (*trailing, *change_summary(changeset), *plan_notices(changeset))
    changeset = replace(changeset, diagnostics=DiagnosticBag((*first, *changeset.diagnostics, *last)))
    return PlanReady(result, manifest, state, changeset, MappingProxyType(dict(handlers)))


def _validation(
    candidates: PlanCandidates,
    port: SnowflakePort | None,
    target: TargetIdentity,
    readers: Fanout[PlanReadPort] | None,
    overrides: Mapping[str, Severity],
) -> DiagnosticBag:
    """Validate the selection, connected when the settings ask for it; return what validation reported."""
    live = port if candidates.connected else None
    return (
        ValidateArtifacts(live, catalog=live, target=target.name, readers=readers if candidates.connected else None)
        .run(
            candidates.selected,
            strict=candidates.strict,
            connected=candidates.connected,
            overrides=overrides,
            reaches=strict_reach(candidates.scope.selection),
        )
        .diagnostics
    )


def _validated(
    candidates: PlanCandidates, diagnostics: DiagnosticBag, held: frozenset[str] = frozenset()
) -> tuple[CompileResult, CompileResult | None] | PlanRefused:
    """Apply validation's findings: narrow a partial plan to what stays healthy, else refuse on an error.

    An error whose stable fingerprint `held` names is one the run's baseline holds, which
    refuses nothing; a partial plan still splits on every error.

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
    if blocking(diagnostics, held):
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
