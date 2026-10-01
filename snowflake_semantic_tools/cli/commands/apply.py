"""`sst apply`: apply a current, reviewed plan to Snowflake; smoke probes never run here.

Without `--plan`, apply plans afresh; with it, the saved plan's selection is used and the
plan must still match what the project compiles and what the target holds.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import PlanFileStore
from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.plan import PlanRefused
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    fail_fast_option,
    output_option,
    partial_option,
    project_options,
    prune_option,
    selection_options,
    sql_out_option,
    validation_options,
)
from snowflake_semantic_tools.cli.plan_output import outcome_json, print_plan, write_plan_sql
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import apply_parallelism
from snowflake_semantic_tools.cli.wiring import project
from snowflake_semantic_tools.cli.wiring.plan import (
    PlanRequest,
    PlanSession,
    partial_excluded,
    plan_runtime,
    refuse_partial_prune,
)
from snowflake_semantic_tools.cli.wiring.project import closed_on_error
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ApplyOutcome, FailurePolicy
from snowflake_semantic_tools.domain.state import SavedPlan


@click.command()
@project_options()
@selection_options()
@click.option("--plan", "plan_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@prune_option()
@partial_option()
@click.option("--yes", "confirmed", is_flag=True)
@fail_fast_option()
@click.option("--break-stale-lock", is_flag=True)
@sql_out_option()
@validation_options()
@output_option()
@command_body("apply")
def apply(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    plan_path: Path | None,
    prune: bool,
    partial: bool,
    confirmed: bool,
    fail_fast: bool,
    break_stale_lock: bool,
    sql_out: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    output: str,
) -> CommandResult:
    """Apply a current reviewed plan; smoke probes never run here."""
    if output == "json" and not confirmed:
        raise SstUsageError("--output json apply requires --yes")
    if prune and not confirmed:
        raise SstUsageError("--prune requires --yes")
    refuse_partial_prune(partial, prune)
    request = PlanRequest(
        project_dir, target_name, manifest_path, selected, excluded, prune, partial, strict, snowflake_syntax_check
    )
    saved = _saved_plan(plan_path, request)
    planned = request.following(saved)
    session = plan_runtime(planned)
    if isinstance(session, PlanRefused):
        return CommandResult(ERROR, session.diagnostics)
    with closed_on_error(session.port):
        options = _confirmed_options(
            planned,
            session,
            saved,
            plan_path,
            sql_out=sql_out,
            confirmed=confirmed,
            fail_fast=fail_fast,
            break_stale_lock=break_stale_lock,
        )
    return _apply_plan(planned, session, options)


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
    break_stale_lock: bool,
) -> ApplyOptions:
    """Check a saved plan still applies, write the statements, and confirm; return how to apply.

    Asks before applying a plan that writes, unless `--yes` was given.

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
    if saved is not None:
        mismatch = saved.check_applicable(current, source=str(plan_path))
        if mismatch is not None:
            raise ProjectError(mismatch.message, diagnostics=mismatch.diagnostics)
    write_plan_sql(request.project_dir, changeset, sql_out)
    if changeset.writes and not confirmed:
        print_plan(changeset)
        click.confirm("Apply this plan?", abort=True)
    return ApplyOptions(
        parallelism=apply_parallelism(request.project_dir),
        on_failure=(FailurePolicy.STOP_ALL if fail_fast else FailurePolicy.STOP_DEPENDENTS),
        allow_prune=request.prune,
        break_stale_lock=break_stale_lock,
    )


def _apply_plan(request: PlanRequest, session: PlanSession, options: ApplyOptions) -> CommandResult:
    """Apply the plan, close the connection, and report each outcome; exit 1 unless everything applied.

    A partial apply publishes the healthy changes and still exits 1 while errors remain.
    """
    ready = session.ready
    try:
        apply_result = ApplyArtifacts(
            session.port,
            session.state_store,
            SystemClock(),
            state_table=session.profile.state_table,
            git_sha=project.git_sha(request.project_dir),
            actor=session.profile.identity.role or "",
            lifecycle_handlers=ready.lifecycle_handlers,
        ).run(ready.changeset, ready.state, options)
    finally:
        session.port.close()
    left_out = ready.result.diagnostics if request.partial else DiagnosticBag()
    shown = DiagnosticBag((*left_out, *apply_result.diagnostics))
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
