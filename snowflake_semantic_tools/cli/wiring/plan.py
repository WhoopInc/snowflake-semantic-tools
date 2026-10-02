"""Wire `sst plan` and `sst apply` to the plan use case: resolve the scope, compile, connect, and plan.

Both commands describe what to plan as a `PlanRequest`. `plan_runtime` decides offline what
can be planned, then connects and plans, and returns a `PlanSession` holding the open
connection, or a `PlanRefused`.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.dbt.profiles import ProfileTarget
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.app.plan import PlanReady, PlanRefused, PlanScope, PreparePlan
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.compile import selection
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest
from snowflake_semantic_tools.cli.wiring.project import closed_on_error, connect, project_inputs, state_store
from snowflake_semantic_tools.cli.wiring.selectors import selector_report
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.state import SavedPlan


@dataclasses.dataclass(frozen=True)
class PlanRequest:
    """What `sst plan` or `sst apply` was asked to plan, as the command line gave it.

    Attributes:
        strict, connected: `--strict` and `--snowflake-syntax-check`; None defers to `validation:`.
    """

    project_dir: Path
    target_name: str | None
    manifest_path: Path | None
    selected: tuple[str, ...]
    excluded: tuple[str, ...]
    prune: bool
    partial: bool
    strict: bool | None
    connected: bool | None

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

    Raises:
        SstUsageError: a selector does not parse, or `--prune` excludes keys without `--select`.
    """
    prune_types, prune_keys = selection(request.selected)
    excluded_types, excluded_keys = selection(request.excluded)
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
    return PlanScope(request.selected, prune_types, prune_keys, excluded_types, excluded_keys, request.prune)


def plan_runtime(request: PlanRequest) -> PlanSession | PlanRefused:
    """Compile and decide what to plan offline, then connect and plan; a refusal when nothing can be.

    A ready plan comes with its open connection, which the caller closes. Once connected, the
    connection is closed here whenever planning raises or is refused.

    The plan's diagnostics start with what `selector_report` says of the selectors.

    Raises:
        SstUsageError: the selectors cannot be resolved, as `plan_scope` says.
        ProjectError: the selectors matched nothing, carrying SST-DIS010 for each, or the
            compiled manifest is stale.
    """
    scope = plan_scope(request)
    project_dir = request.project_dir
    full_result = compiling.compile_result(project_dir, request.target_name, request.manifest_path)
    selectors = selector_report(request.selected, request.excluded, full_result.compiled)
    prepare = PreparePlan(project_inputs(project_dir, request.target_name, request.manifest_path), SystemClock())
    candidates = prepare.select(
        full_result,
        compiled_manifest(project_dir),
        scope,
        partial=request.partial,
        strict=request.strict,
        connected=request.connected,
        project=str(project_dir),
    )
    if isinstance(candidates, PlanRefused):
        if candidates.reason is not None:
            raise ProjectError(candidates.reason, diagnostics=tuple(selectors))
        return candidates
    profile, port = connect(project_dir, request.target_name)
    with closed_on_error(port):
        store = state_store(project_dir, profile.target_name)
        outcome = prepare.run(candidates, port, store, target=profile.identity, state_table=profile.state_table)
    if isinstance(outcome, PlanRefused):
        port.close()
        return outcome
    if selectors:
        changeset = dataclasses.replace(
            outcome.changeset, diagnostics=DiagnosticBag((*selectors, *outcome.changeset.diagnostics))
        )
        outcome = dataclasses.replace(outcome, changeset=changeset)
    return PlanSession(outcome, profile, port, store)


def refuse_partial_prune(partial: bool, prune: bool) -> None:
    """Refuse `--partial` with `--prune`, for `sst plan` and `sst apply` alike.

    An artifact left out for errors looks orphaned, so pruning could remove a live
    object whose source is only broken; the combination is refused outright.

    Raises:
        SstUsageError: both flags were given.
    """
    if partial and prune:
        raise SstUsageError("--partial cannot be combined with --prune")


def partial_excluded(diagnostics: DiagnosticBag) -> list[str]:
    """Return what a partial run left out: the subject of each SST-PLN032 diagnostic, in order."""
    return [str(item.subject) for item in diagnostics if item.code == "SST-PLN032"]
