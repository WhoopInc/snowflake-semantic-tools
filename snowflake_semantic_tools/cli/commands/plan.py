"""`sst plan`: observe live Snowflake state and compute the changes, without writing to Snowflake."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.fs.local import PlanFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.plan import PlanReady, PlanRefused
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.options import (
    defer_target_option,
    no_detailed_exitcode_option,
    partial_option,
    prune_option,
    selection_options,
    sql_out_option,
    state_option,
    target_option,
    validation_options,
)
from snowflake_semantic_tools.cli.plan_output import change_json, print_plan, write_plan_sql
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import strict_disagreement
from snowflake_semantic_tools.cli.wiring.plan import (
    PlanRequest,
    PlanSession,
    partial_excluded,
    plan_runtime,
    refuse_partial_prune,
    refuse_together,
)
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.state import SavedPlan


def _refuse_invocation(plan_out: Path | None, no_plan_out: bool, partial: bool, prune: bool) -> None:
    """Refuse flags that exclude each other: a plan path with `--no-plan-out`, or a partial prune.

    Raises:
        SstUsageError: carrying SST-PRT104.
    """
    if plan_out is not None and no_plan_out:
        refuse_together("--plan-out", "--no-plan-out")
    refuse_partial_prune(partial, prune)


@click.command()
@target_option()
@selection_options()
@state_option()
@defer_target_option()
@prune_option()
@partial_option()
@click.option("--plan-out", type=click.Path(dir_okay=False, path_type=Path))
@click.option("--no-plan-out", is_flag=True)
@sql_out_option()
@no_detailed_exitcode_option()
@validation_options()
@command_body("plan", refusals=_refuse_invocation)
def plan(
    paths: ProjectPaths,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    state_dir: Path | None,
    prune: bool,
    partial: bool,
    plan_out: Path | None,
    no_plan_out: bool,
    sql_out: Path | None,
    no_detailed_exitcode: bool,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
) -> CommandResult:
    """Observe live Snowflake state and compute a non-writing plan.

    Exit 0 with nothing to change, 2 with changes pending, and 1 on an error or a blocked change.
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
        state_dir,
    )
    session = plan_runtime(request)
    if isinstance(session, PlanRefused):
        return CommandResult(ERROR, session.diagnostics)
    saved, destination, sql_path = _save_plan(request, session, plan_out, no_plan_out, sql_out)
    plan_path = None if no_plan_out else destination
    report = _plan_report(request, session.ready, saved, plan_path, sql_path, no_detailed_exitcode)
    disagreement = strict_disagreement(paths, strict)
    return dataclasses.replace(report, diagnostics=DiagnosticBag((*disagreement, *report.diagnostics)))


def _save_plan(
    request: PlanRequest, session: PlanSession, plan_out: Path | None, no_plan_out: bool, sql_out: Path | None
) -> tuple[SavedPlan, Path, Path]:
    """Save the plan unless `--no-plan-out`, write each change's statements, then close the connection.

    Returns:
        The saved plan, the path it is saved at or would have been, and the statements' directory.
    """
    changeset = session.ready.changeset
    try:
        saved = SavedPlan.from_changeset(
            changeset,
            selected=request.selected,
            excluded=request.excluded,
            include_prune=request.prune,
            partial=request.partial,
        )
        destination = plan_out or target_dir(request.project_dir) / "plan.json"
        if not no_plan_out:
            PlanFileStore(destination).write(saved)
        sql_path = write_plan_sql(request.project_dir, changeset, sql_out)
    finally:
        session.port.close()
    return saved, destination, sql_path


def _plan_report(
    request: PlanRequest,
    ready: PlanReady,
    saved: SavedPlan,
    plan_path: Path | None,
    sql_path: Path,
    no_detailed_exitcode: bool,
) -> CommandResult:
    """Report a plan, which exits 1 on an error or a blocked change, 2 with writes pending, else 0.

    With `--no-detailed-exitcode`, pending writes exit 0. With `--partial`, what was left out
    is reported ahead of the plan's own diagnostics.
    """
    changeset = ready.changeset
    shown = (
        DiagnosticBag((*ready.result.diagnostics, *changeset.diagnostics)) if request.partial else changeset.diagnostics
    )
    if changeset.blocked or shown.has_errors:
        exit_code = ERROR
    elif changeset.writes:
        exit_code = OK if no_detailed_exitcode else CHANGES
    else:
        exit_code = OK
    data: dict[str, object] = {
        "manifest_id": ready.manifest.manifest_id,
        "plan_id": saved.plan_id,
        "plan_path": None if plan_path is None else str(plan_path),
        "sql_path": str(sql_path),
        "changes": [change_json(change) for change in changeset.changes],
        "report_only": [change.key for change in changeset.report_only],
    }
    if request.partial:
        data["partial"] = {"excluded": partial_excluded(ready.result.diagnostics)}
    return CommandResult(exit_code, shown, data, human=lambda: print_plan(changeset))
