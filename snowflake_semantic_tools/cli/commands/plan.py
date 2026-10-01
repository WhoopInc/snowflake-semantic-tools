"""`sst plan`: observe live Snowflake state and compute the changes, without writing to Snowflake."""

from __future__ import annotations

from pathlib import Path

import click

from ...adapters.fs.local import PlanFileStore
from ...app.plan import PlanReady, PlanRefused
from ...domain.model.diagnostic import DiagnosticBag
from ...domain.state import SavedPlan
from ..exit_codes import CHANGES, ERROR, OK
from ..group import SstUsageError
from ..options import (
    output_option,
    partial_option,
    project_options,
    prune_option,
    selection_options,
    sql_out_option,
    validation_options,
)
from ..plan_output import change_json, print_plan, write_plan_sql
from ..runner import CommandResult, command_body
from ..wiring.plan import PlanRequest, PlanSession, partial_excluded, plan_runtime, refuse_partial_prune
from ..wiring.project import target_dir


@click.command()
@project_options()
@selection_options()
@prune_option()
@partial_option()
@click.option("--plan-out", type=click.Path(dir_okay=False, path_type=Path))
@click.option("--no-plan-out", is_flag=True)
@sql_out_option()
@click.option("--no-detailed-exitcode", is_flag=True)
@validation_options()
@output_option()
@command_body("plan")
def plan(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    prune: bool,
    partial: bool,
    plan_out: Path | None,
    no_plan_out: bool,
    sql_out: Path | None,
    no_detailed_exitcode: bool,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    output: str,
) -> CommandResult:
    """Observe live Snowflake state and compute a non-writing plan."""
    if plan_out is not None and no_plan_out:
        raise SstUsageError("--plan-out and --no-plan-out are mutually exclusive")
    refuse_partial_prune(partial, prune)
    request = PlanRequest(
        project_dir, target_name, manifest_path, selected, excluded, prune, partial, strict, snowflake_syntax_check
    )
    session = plan_runtime(request)
    if isinstance(session, PlanRefused):
        return CommandResult(ERROR, session.diagnostics)
    saved, destination, sql_path = _save_plan(request, session, plan_out, no_plan_out, sql_out)
    plan_path = None if no_plan_out else destination
    return _plan_report(request, session.ready, saved, plan_path, sql_path, no_detailed_exitcode)


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
