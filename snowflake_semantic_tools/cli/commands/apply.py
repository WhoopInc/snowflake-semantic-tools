"""`sst apply`: apply a current, reviewed plan to Snowflake; smoke probes never run here.

Without `--plan`, apply plans afresh; with it, the saved plan's selection is used and the
plan must still match what the project compiles and what the target holds.
"""

from __future__ import annotations

import contextlib
import dataclasses
import socket
from pathlib import Path
from typing import NoReturn

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import PlanFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.apply.observation import stale_observation
from snowflake_semantic_tools.app.plan import PlanRefused
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    defer_target_option,
    fail_fast_pair,
    partial_option,
    prune_option,
    selection_options,
    sql_out_option,
    state_option,
    target_option,
    threads_option,
    validation_options,
)
from snowflake_semantic_tools.cli.plan_output import outcome_json, print_plan, write_plan_sql
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import (
    apply_fail_fast,
    apply_parallelism,
    strict_disagreement,
    threads_setting,
)
from snowflake_semantic_tools.cli.wiring import project
from snowflake_semantic_tools.cli.wiring.plan import (
    PlanRequest,
    PlanSession,
    partial_excluded,
    plan_runtime,
    refuse_partial_prune,
)
from snowflake_semantic_tools.cli.wiring.project import closed_on_error
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ApplyOutcome, FailurePolicy
from snowflake_semantic_tools.domain.state import SavedPlan


def _refuse_invocation(options: GlobalOptions, confirmed: bool, prune: bool, partial: bool) -> None:
    """Refuse a command line apply cannot run: JSON or a prune without `--yes`, or a partial prune.

    Raises:
        SstUsageError: as `_require_yes` and `refuse_partial_prune` say.
    """
    if options.output == "json" and not confirmed:
        _require_yes("sst apply --output json")
    if prune and not confirmed:
        _require_yes("sst apply --prune")
    refuse_partial_prune(partial, prune)


@click.command()
@target_option()
@selection_options()
@state_option()
@defer_target_option()
@click.option("--plan", "plan_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@prune_option()
@partial_option()
@click.option("--yes", "-y", "confirmed", is_flag=True)
@fail_fast_pair()
@threads_option()
@click.option("--break-stale-lock", is_flag=True)
@click.option("--temporary", is_flag=True)
@sql_out_option()
@validation_options()
@command_body("apply", refusals=_refuse_invocation)
def apply(
    paths: ProjectPaths,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    state_dir: Path | None,
    plan_path: Path | None,
    prune: bool,
    partial: bool,
    confirmed: bool,
    fail_fast: bool | None,
    threads: int | None,
    break_stale_lock: bool,
    temporary: bool,
    sql_out: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    options: GlobalOptions,
) -> CommandResult:
    """Apply a current reviewed plan; smoke probes never run here.

    `--fail-fast` and `--no-fail-fast` override `apply.fail_fast` for this run.
    """
    request = PlanRequest(
        paths,
        target_name,
        manifest_path,
        selected,
        excluded,
        prune,
        partial,
        strict,
        snowflake_syntax_check,
        temporary=temporary,
        state_dir=state_dir,
        threads=threads_setting(paths, threads),
    )
    saved = _saved_plan(plan_path, request)
    planned = request.following(saved)
    session = plan_runtime(planned)
    if isinstance(session, PlanRefused):
        return CommandResult(ERROR, session.diagnostics)
    with closed_on_error(session.port):
        apply_options, notes = _confirmed_options(
            planned,
            session,
            saved,
            plan_path,
            sql_out=sql_out,
            confirmed=confirmed,
            fail_fast=apply_fail_fast(paths, fail_fast),
            threads=threads,
            break_stale_lock=break_stale_lock,
        )
    result = _apply_plan(planned, session, apply_options, notes)
    disagreement = strict_disagreement(paths, strict)
    return dataclasses.replace(result, diagnostics=DiagnosticBag((*disagreement, *result.diagnostics)))


def _require_yes(command: str) -> NoReturn:
    """Refuse a run whose confirmation is mandatory and was not given.

    Raises:
        SstUsageError: always, carrying SST-PRT109.

    Diagnostics:
        SST-PRT109: the run needs --yes and was not given it; raised.
    """
    diagnostic = D("SST-PRT109", subject="cli", command=command)
    raise SstUsageError(diagnostic.message, diagnostic=diagnostic)


def _saved_plan(plan_path: Path | None, request: PlanRequest) -> SavedPlan | None:
    """Read the plan `--plan` names, refusing selection flags that disagree with it; None without one.

    Raises:
        ProjectError: no saved plan is at the path.
        SstUsageError: `--select`, `--exclude`, `--prune`, or `--partial` disagrees with the saved plan.
    """
    if plan_path is None:
        return None
    saved = PlanFileStore(plan_path).read()
    if saved is None:
        raise ProjectError(f"no saved plan at {plan_path}")
    if request.selected and request.selected != saved.selected:
        raise SstUsageError("--select conflicts with the saved plan selection")
    if request.excluded and request.excluded != saved.excluded:
        raise SstUsageError("--exclude conflicts with the saved plan selection")
    if request.prune and not saved.include_prune:
        raise SstUsageError("--prune conflicts with a non-pruning saved plan")
    # Publishing a partial result is always explicit, in both directions.
    if saved.partial and not request.partial:
        raise SstUsageError("the saved plan is partial; apply it with --partial")
    if request.partial and not saved.partial:
        raise SstUsageError("--partial conflicts with a saved plan that is not partial")
    return saved


def _confirmed_options(
    request: PlanRequest,
    session: PlanSession,
    saved: SavedPlan | None,
    plan_path: Path | None,
    *,
    sql_out: Path | None,
    confirmed: bool,
    fail_fast: bool,
    threads: int | None,
    break_stale_lock: bool,
) -> tuple[ApplyOptions, DiagnosticBag]:
    """Check a saved plan still applies, write the statements, and confirm; return how to apply.

    Asks before applying a plan that writes, unless `--yes` was given. Returns the options,
    and what checking the saved plan reported without refusing it.

    Raises:
        ProjectError: the saved plan cannot be applied, as `SavedPlan.check_applicable` says.
        click.exceptions.Abort: the user declined to apply.
    """
    changeset = session.ready.changeset
    current = SavedPlan.from_changeset(
        changeset,
        selected=request.selected,
        excluded=request.excluded,
        include_prune=request.prune,
        partial=request.partial,
    )
    notes = DiagnosticBag()
    if saved is not None:
        mismatch = saved.check_applicable(current, source=str(plan_path))
        if mismatch is not None:
            raise ProjectError(mismatch.message, diagnostics=mismatch.diagnostics)
        stale = stale_observation(saved, current, source=str(plan_path))
        notes = DiagnosticBag((stale,) if stale is not None else ())
    write_plan_sql(request.project_dir, changeset, sql_out)
    if changeset.writes and not confirmed:
        print_plan(changeset)
        click.confirm("Apply this plan?", abort=True)
    options = ApplyOptions(
        parallelism=apply_parallelism(request.paths, threads),
        on_failure=(FailurePolicy.STOP_ALL if fail_fast else FailurePolicy.STOP_DEPENDENTS),
        allow_prune=request.prune,
        break_stale_lock=break_stale_lock,
        temporary=request.temporary,
    )
    return options, notes


def _apply_plan(
    request: PlanRequest, session: PlanSession, options: ApplyOptions, notes: DiagnosticBag
) -> CommandResult:
    """Apply the plan, close the connection, and report each outcome; exit 1 unless everything applied.

    A partial apply publishes the healthy changes and still exits 1 while errors remain.
    `notes` are reported first, then what the plan left out, then the run's own diagnostics.
    """
    ready = session.ready
    params = session.profile.connection_params
    # Changes run on connections leased from the pool, never on `port`, which keeps the plan
    # reads, the run lock, and the state write; the heartbeat has a connection of its own.
    try:
        with (
            ConnectorPool(options.parallelism, lambda: project.open_connector(params)) as pool,
            contextlib.closing(project.open_connector(params)) as heartbeat,
        ):
            apply_result = ApplyArtifacts(
                session.port,
                session.state_store,
                SystemClock(),
                state_table=session.profile.state_table,
                git_sha=project.git_sha(request.project_dir),
                actor=session.profile.identity.role or "",
                host=socket.gethostname(),
                lifecycle_handlers=ready.lifecycle_handlers,
                sessions=pool,
                heartbeat=heartbeat,
            ).run(ready.changeset, ready.state, options)
    finally:
        session.port.close()
    left_out = ready.result.diagnostics if request.partial else DiagnosticBag()
    shown = DiagnosticBag((*notes, *left_out, *apply_result.diagnostics))
    exit_code = OK if apply_result.success and not left_out.has_errors else ERROR
    data: dict[str, object] = {
        "run_id": apply_result.run_id,
        "state_written": apply_result.state_written,
        "outcomes": [outcome_json(outcome) for outcome in apply_result.outcomes],
    }
    if request.partial:
        data["partial"] = {"excluded": partial_excluded(ready.result.diagnostics)}
    return CommandResult(exit_code, shown, data, human=lambda: _print_outcomes(apply_result.outcomes))


def _print_outcomes(outcomes: tuple[ApplyOutcome, ...]) -> None:
    for outcome in outcomes:
        click.echo(f"{outcome.status.value}: {outcome.key} ({outcome.action.value})")
