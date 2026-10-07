"""Wire `sst plan` and `sst apply` to the plan use case: resolve the scope, compile, connect, and plan.

Both commands describe what to plan as a `PlanRequest`. `plan_runtime` decides offline what
can be planned, then connects and plans, and returns a `PlanSession` holding the open
connection, or a `PlanRefused`. `recorded_plan`, for `plan --use-cached-state`, plans from
the observation `--state` records instead and never connects.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import NoReturn

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.dbt.profiles import ProfileTarget
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import (
    ManifestFileStore,
    ObservationFileStore,
    StateFileStore,
    observation_file,
)
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool, SnowflakeConnector
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.observe import ObserveOptions
from snowflake_semantic_tools.app.plan import PlanCandidates, PlanReady, PlanRefused, PlanScope, PreparePlan
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.compile import manifest_universe, selection
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest
from snowflake_semantic_tools.cli.wiring.project import (
    closed_on_error,
    connect,
    open_connector,
    project_inputs,
    state_store,
    target_dir,
)
from snowflake_semantic_tools.cli.wiring.selectors import selector_report
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.impact import STATE_MODIFIED
from snowflake_semantic_tools.domain.plan.recorded import RecordedObservation
from snowflake_semantic_tools.domain.state import Manifest, SavedPlan


@dataclasses.dataclass(frozen=True)
class PlanRequest:
    """What `sst plan` or `sst apply` was asked to plan, as the command line gave it.

    Attributes:
        strict, connected: `--strict` and `--snowflake-syntax-check`; None defers to `validation:`.
        temporary: `apply --temporary`: agents publish as session-scoped temporary agents.
        state_dir: `--state`, the previous run's build directory `state:` selectors compare with,
            and whose `manifest.json` `state:modified` scopes the plan by.
        threads: How many sessions planning reads on at once, as `--threads` resolved.
        observe_options: `plan --grants/--no-grants` and `--capture-prior`: which per-object
            reads the observation makes.
        validate: False under `--no-validate`, which skips validation.
        use_cached_state: `plan --use-cached-state`: plan from the observation recorded in
            `state_dir` instead of reading the target.
        baseline: The run's baseline; a validation error it holds does not refuse the plan.
    """

    paths: ProjectPaths
    target_name: str | None
    manifest_path: Path | None
    selected: tuple[str, ...]
    excluded: tuple[str, ...]
    prune: bool
    partial: bool
    strict: bool | None
    connected: bool | None
    temporary: bool = False
    state_dir: Path | None = None
    threads: int = 1
    observe_options: ObserveOptions = ObserveOptions()
    validate: bool = True
    use_cached_state: bool = False
    baseline: Baseline | None = None

    @property
    def project_dir(self) -> Path:
        """Return the project directory the request plans."""
        return self.paths.project_dir

    def following(self, saved: SavedPlan | None) -> PlanRequest:
        """Return the request with a saved plan's selection in place of the flags'; itself without one."""
        if saved is None:
            return self
        return dataclasses.replace(self, selected=saved.selected, excluded=saved.excluded, prune=saved.include_prune)


@dataclasses.dataclass(frozen=True)
class PlanSession:
    """A ready plan and what the CLI wired for it: the live target, the open connection, the state file.

    Whoever holds the session closes `port`.
    """

    ready: PlanReady
    profile: ProfileTarget
    port: SnowflakeConnector
    state_store: StateFileStore


def plan_scope(request: PlanRequest) -> PlanScope:
    """Resolve `--select`, `--exclude`, and `--prune` into what a plan covers and may prune.

    An excluded type leaves the selected types, or every type when none is selected. With
    `--prune`, an excluded key leaves the selected keys, which must then be given.

    Names, paths, and states are resolved against the manifest `sst compile` wrote. With
    `--state`, `state:modified` selects nothing itself: it narrows the plan to what changed since
    the previous manifest, and falls back to a full plan when there is none (SST-PLN006).

    Raises:
        SstUsageError: a selector is refused, such as a `state:` selector without `--state`
            (SST-PRT103), or `--prune` excludes keys without `--select`.
        ProjectError: no compiled manifest exists, or `--state` holds none for a `state:`
            selector other than `state:modified` (SST-MAN001).
    """
    impact = request.state_dir is not None and STATE_MODIFIED in request.selected
    named = _named(request)
    universe = manifest_universe(compiled_manifest(request.project_dir)) if named or request.excluded else ()
    stateful = any(_is_state(value) for value in (*named, *request.excluded))
    previous = _previous_fingerprints(request.state_dir) if stateful else None
    named_selection = selection(named, universe, previous)
    excluded_selection = selection(request.excluded, universe, previous)
    prune_types, prune_keys = named_selection.types, named_selection.keys
    excluded_types, excluded_keys = excluded_selection.types, excluded_selection.keys
    if excluded_types is not None:
        prune_types = (
            frozenset(SEMANTIC_REGISTRY.artifacts) - excluded_types
            if prune_types is None
            else frozenset(prune_types - excluded_types)
        )
    if request.prune and excluded_keys is not None:
        if prune_keys is None:
            raise SstUsageError("--prune with --exclude requires --select so the prune scope is explicit")
        prune_keys = frozenset(prune_keys - excluded_keys)
    return PlanScope(request.selected, prune_types, prune_keys, excluded_types, excluded_keys, request.prune, impact)


def _named(request: PlanRequest) -> tuple[str, ...]:
    """The selectors that name artifacts: all of them, less `state:modified` when it scopes by impact."""
    if request.state_dir is None:
        return request.selected
    return tuple(value for value in request.selected if value != STATE_MODIFIED)


def _is_state(selector: str) -> bool:
    return selector.casefold().startswith("state:")


def previous_manifest(request: PlanRequest) -> Manifest | None:
    """Read the manifest in `--state`, which `state:modified` compares with; None when there is none.

    Raises:
        ProjectError: the file is there but cannot be used.
    """
    if request.state_dir is None:
        return None
    return ManifestFileStore(request.state_dir / "manifest.json").read()


def _previous_fingerprints(state_dir: Path | None) -> dict[str, str] | None:
    """Return each artifact key's fingerprint in the `--state` manifest; None without `--state`.

    Raises:
        ProjectError: the directory holds no SST manifest (SST-MAN001).
    """
    if state_dir is None:
        return None
    path = state_dir / "manifest.json"
    manifest = ManifestFileStore(path).read()
    if manifest is None:
        diagnostic = D("SST-MAN001", path=str(path))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return {key: entry.fingerprint for key, entry in manifest.artifacts.items()}


def plan_runtime(request: PlanRequest) -> PlanSession | PlanRefused:
    """Compile and decide what to plan offline, then connect and plan; a refusal when nothing can be.

    A ready plan comes with its open connection, which the caller closes. Once connected, the
    connection is closed here whenever planning raises or is refused. With more than one
    thread, planning reads on up to `threads` sessions at once, the connection and siblings
    opened from the same settings; every sibling is closed before this returns or raises.

    The plan's diagnostics start with what `selector_report` says of the selectors.

    Raises:
        SstUsageError: the selectors cannot be resolved, as `plan_scope` says.
        ProjectError: the selectors matched nothing, carrying SST-DIS010 for each, or the
            compiled manifest is stale.
    """
    prepared = _prepared(request)
    if isinstance(prepared, PlanRefused):
        return prepared
    prepare, candidates, selectors = prepared
    profile, port = connect(request.paths, request.target_name, generating=True)
    params = profile.connection_params
    with closed_on_error(port), ConnectorPool(request.threads, lambda: open_connector(params)) as pool:
        store = state_store(request.paths, profile.target_name)
        outcome = prepare.run(
            candidates,
            port,
            store,
            target=profile.identity,
            state_table=profile.state_table,
            temporary=request.temporary,
            preflight=port,
            readers=Fanout(port, pool, request.threads),
            observe_options=request.observe_options,
            validate=request.validate,
        )
    if isinstance(outcome, PlanRefused):
        port.close()
        return outcome
    return PlanSession(_with_selectors(outcome, selectors), profile, port, store)


def recorded_plan(request: PlanRequest) -> PlanReady | PlanRefused:
    """Compile and decide what to plan, then plan from the observation `--state` records; never connect.

    The plan's diagnostics start with what `selector_report` says of the selectors.

    Raises:
        SstUsageError: the selectors cannot be resolved, as `plan_scope` says.
        ProjectError: the selectors matched nothing, or the compiled manifest is stale; or no
            usable observation of the target is recorded, as `recorded_observation` says.
    """
    prepared = _prepared(request)
    if isinstance(prepared, PlanRefused):
        return prepared
    prepare, candidates, selectors = prepared
    outcome = prepare.run_recorded(candidates, recorded_observation(request), validate=request.validate)
    if isinstance(outcome, PlanRefused):
        return outcome
    return _with_selectors(outcome, selectors)


def recorded_observation(request: PlanRequest) -> RecordedObservation:
    """Read the observation `--state` records of the request's target, as `sst plan` recorded it.

    The target is resolved without connecting, and the record must have been taken of it: the
    same target name, account as the profile declares it, database, and schema.

    Raises:
        ProjectError: no observation of the target is recorded in `--state` (SST-PRT009), the
            file cannot be used, or it records another account, database, or schema (SST-MAN025).

    Diagnostics:
        SST-PRT009: `--state` holds no observation of the target; raised.
        SST-MAN025: the file under the target's name records another account, database, or
            schema; raised.
    """
    target = _declared_target(request)
    path = cached_observation_path(request)
    recorded = ObservationFileStore(path, root=path.parent).read()
    if recorded is None:
        detail = "no observation is recorded there; run sst plan without --use-cached-state to record one"
        _refuse(D("SST-PRT009", path=str(path), detail=detail))
    foreign = recorded.foreign_to(target)
    if foreign is not None:
        _refuse(D("SST-MAN025", path=str(path), value=foreign))
    return recorded


def cached_observation_path(request: PlanRequest) -> Path:
    """Return the file in `--state` that `--use-cached-state` reads the target's observation from.

    Raises:
        SstUsageError: carrying SST-PRT100, when the request names no `--state`.
    """
    if request.state_dir is None:
        raise SstUsageError("--use-cached-state requires --state, the directory holding the recorded observation")
    return observation_file(request.state_dir, _declared_target(request).name)


def record_observation(request: PlanRequest, ready: PlanReady) -> Path | None:
    """Record what a live plan read of its target in the build directory; None when it recorded nothing.

    The record keeps the account the profile declares beside the one the session reported,
    so a later `--use-cached-state`, which does not connect, can tell the target is the same.
    A plan made from a record, or one whose reading had a refused read, records nothing.
    """
    if ready.recorded is None:
        return None
    recorded = dataclasses.replace(ready.recorded, declared_account=_declared_target(request).account_locator)
    path = observation_file(target_dir(request.project_dir), recorded.target.name)
    ObservationFileStore(path, root=request.project_dir).write(recorded)
    return path


def _declared_target(request: PlanRequest) -> TargetIdentity:
    """The request's target as the profile declares it, resolved without connecting."""
    return project_inputs(request.paths, request.target_name, request.manifest_path).target().identity


def _refuse(diagnostic: Diagnostic) -> NoReturn:
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _prepared(request: PlanRequest) -> tuple[PreparePlan, PlanCandidates, DiagnosticBag] | PlanRefused:
    """Resolve the scope, compile, and decide offline what may be planned, with the selectors' report.

    Raises:
        SstUsageError: the selectors cannot be resolved, as `plan_scope` says.
        ProjectError: the selectors matched nothing, carrying SST-DIS010 for each, or the
            compiled manifest is stale.
    """
    scope = plan_scope(request)
    project_dir = request.project_dir
    full_result = compiling.compile_result(request.paths, request.target_name, request.manifest_path)
    selectors = selector_report(_named(request), request.excluded, full_result.compiled)
    inputs = project_inputs(request.paths, request.target_name, request.manifest_path)
    prepare = PreparePlan(inputs, SystemClock(), baseline=request.baseline)
    candidates = prepare.select(
        full_result,
        compiled_manifest(project_dir),
        scope,
        partial=request.partial,
        strict=request.strict,
        connected=request.connected,
        project=str(project_dir),
        previous_manifest=previous_manifest(request) if scope.impact else None,
    )
    if isinstance(candidates, PlanRefused):
        if candidates.reason is not None:
            raise ProjectError(candidates.reason, diagnostics=tuple(selectors))
        return candidates
    return prepare, candidates, DiagnosticBag(tuple(selectors))


def _with_selectors(ready: PlanReady, selectors: DiagnosticBag) -> PlanReady:
    """Report what the selectors' report says ahead of the plan's own diagnostics."""
    if not selectors:
        return ready
    changeset = dataclasses.replace(
        ready.changeset, diagnostics=DiagnosticBag((*selectors, *ready.changeset.diagnostics))
    )
    return dataclasses.replace(ready, changeset=changeset)


def refuse_partial_prune(partial: bool, prune: bool) -> None:
    """Refuse `--partial` with `--prune`, for `sst plan` and `sst apply` alike.

    An artifact left out for errors looks orphaned, so pruning could remove a live
    object whose source is only broken; the combination is refused outright.

    Raises:
        SstUsageError: both flags were given (SST-PRT104).
    """
    if partial and prune:
        refuse_together("--partial", "--prune")


def refuse_unvalidated(no_validate: bool, snowflake_syntax_check: bool | None) -> None:
    """Refuse `--no-validate` with `--snowflake-syntax-check`, for `sst plan` and `sst apply` alike.

    The syntax check is part of validation, so asking for it while skipping validation
    asks for two contradictory things.

    Raises:
        SstUsageError: both flags were given (SST-PRT104).
    """
    if no_validate and snowflake_syntax_check:
        refuse_together("--no-validate", "--snowflake-syntax-check")


def refuse_together(first: str, second: str) -> NoReturn:
    """Refuse two flags that exclude each other.

    Raises:
        SstUsageError: always, carrying SST-PRT104.

    Diagnostics:
        SST-PRT104: both flags were given; raised.
    """
    diagnostic = D("SST-PRT104", subject="cli", a=first, b=second)
    raise SstUsageError(diagnostic.message, diagnostic=diagnostic)


def partial_excluded(diagnostics: DiagnosticBag) -> list[str]:
    """Return what a partial run left out: the subject of each SST-PLN032 diagnostic, in order."""
    return [str(item.subject) for item in diagnostics if item.code == "SST-PLN032"]
