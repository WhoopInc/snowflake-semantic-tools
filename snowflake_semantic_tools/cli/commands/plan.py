"""`sst plan`: observe live Snowflake state and compute the changes, without writing to Snowflake."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.fs.local import PlanFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import output_root
from snowflake_semantic_tools.app.baseline import held_by_baseline
from snowflake_semantic_tools.app.observe import ObserveOptions
from snowflake_semantic_tools.app.plan import PlanReady, PlanRefused
from snowflake_semantic_tools.cli.exit_codes import ERROR
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    defer_target_option,
    no_detailed_exitcode_option,
    no_validate_option,
    partial_option,
    prune_option,
    selection_options,
    sql_out_option,
    state_option,
    target_option,
    threads_option,
    validation_options,
)
from snowflake_semantic_tools.cli.plan_output import (
    change_counts,
    change_json,
    plan_diff_path,
    plan_diff_text,
    plan_exit_code,
    print_changed_names,
    print_plan,
    write_plan_diff,
    write_plan_sql,
)
from snowflake_semantic_tools.cli.render.report import plan_headline
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, run_baseline
from snowflake_semantic_tools.cli.settings import drift_check_setting, strict_disagreement, threads_setting
from snowflake_semantic_tools.cli.wiring.plan import (
    PlanRequest,
    cached_observation_path,
    partial_excluded,
    plan_runtime,
    record_observation,
    recorded_plan,
    refuse_partial_prune,
    refuse_together,
    refuse_unvalidated,
)
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.state import SavedPlan


def _refuse_invocation(
    plan_out: Path | None,
    no_plan_out: bool,
    partial: bool,
    prune: bool,
    no_validate: bool,
    snowflake_syntax_check: bool | None,
    use_cached_state: bool,
    state_dir: Path | None,
    capture_prior: bool,
    no_diff: bool,
    diff_out: Path | None,
) -> None:
    """Refuse flags that exclude each other, or one missing what it needs.

    A plan path with `--no-plan-out`, a diff path with `--no-diff`, a partial prune, or a syntax
    check with `--no-validate`; `--use-cached-state` without `--state`, or with a flag that reads
    the live target.

    Raises:
        SstUsageError: carrying SST-PRT104, or SST-PRT100 for `--use-cached-state` without `--state`.
    """
    if plan_out is not None and no_plan_out:
        refuse_together("--plan-out", "--no-plan-out")
    if diff_out is not None and no_diff:
        refuse_together("--diff-out", "--no-diff")
    refuse_partial_prune(partial, prune)
    refuse_unvalidated(no_validate, snowflake_syntax_check)
    if not use_cached_state:
        return
    if state_dir is None:
        raise SstUsageError("--use-cached-state requires --state, the directory holding the recorded observation")
    if snowflake_syntax_check:
        refuse_together("--use-cached-state", "--snowflake-syntax-check")
    if capture_prior:
        refuse_together("--use-cached-state", "--capture-prior")


@click.command(cls=SstCommand)
@target_option()
@selection_options()
@state_option()
@defer_target_option()
@prune_option()
@partial_option()
@click.option("--plan-out", type=click.Path(dir_okay=False, path_type=Path))
@click.option("--no-plan-out", is_flag=True)
@click.option("--grants/--no-grants", default=True)
@click.option("--use-cached-state", is_flag=True)
@click.option("--capture-prior", is_flag=True)
@click.option("--no-diff", is_flag=True)
@click.option("--diff-out", type=click.Path(dir_okay=False, path_type=Path))
@sql_out_option()
@no_validate_option()
@threads_option()
@click.option("--full", is_flag=True)
@click.option("--names-only", is_flag=True)
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
    grants: bool,
    use_cached_state: bool,
    capture_prior: bool,
    no_diff: bool,
    diff_out: Path | None,
    sql_out: Path | None,
    no_validate: bool,
    threads: int | None,
    full: bool,
    names_only: bool,
    no_detailed_exitcode: bool,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    options: GlobalOptions,
) -> CommandResult:
    """Observe live Snowflake state and compute a non-writing plan.

    Exit 0 with nothing to change, 2 with changes pending, and 1 on an error or a blocked change.
    Nothing to change means each live object carries the ownership marker state records, and
    the properties `plan.drift_check` compares are as apply read them back; an object edited
    by hand in either way is blocked (SST-PLN014).
    The plan is saved as `sst_target/plan.json` unless --plan-out or --no-plan-out says
    otherwise, and each create's and update's statements go to `sst_target/sql`, or --sql-out.
    `--names-only` prints only the name of each changed artifact, and wins over `--full`.
    Each update is followed by what it changes, member by member, the live object against what
    SST renders, cut short where long; all of it is written as a unified diff to
    `sst_target/plan.<target>.diff`, or --diff-out, on every plan. `--no-diff` skips reading
    the live definitions. What the plan read of the target is recorded as
    `sst_target/observation.<target>.json`; `--use-cached-state` plans from the one `--state`
    holds instead, without connecting, and reports how old it is.
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
        state_dir=state_dir,
        threads=threads_setting(paths, threads),
        observe_options=ObserveOptions(
            grants=grants, capture_prior=capture_prior, drift_check=drift_check_setting(paths), diff=not no_diff
        ),
        validate=not no_validate,
        use_cached_state=use_cached_state,
        baseline=run_baseline(options),
    )
    ready, close = _planned(request)
    if isinstance(ready, PlanRefused):
        return CommandResult(ERROR, ready.diagnostics, promoted=ready.promoted)
    saved, destination, sql_path, observed_path, diff_path = _save_plan(
        request, ready, close, _Outputs(plan_out, no_plan_out, sql_out, diff_out)
    )
    plan_path = None if no_plan_out else destination
    paths_written = _Written(plan_path, sql_path, observed_path, diff_path)
    report = _plan_report(request, ready, saved, paths_written, no_detailed_exitcode, full=full, names_only=names_only)
    disagreement = strict_disagreement(paths, strict)
    return dataclasses.replace(report, diagnostics=DiagnosticBag((*disagreement, *report.diagnostics)))


def _planned(request: PlanRequest) -> tuple[PlanReady | PlanRefused, Callable[[], None]]:
    """Plan from the recorded observation, or else against the live target; with what closes it."""
    if request.use_cached_state:
        return recorded_plan(request), lambda: None
    session = plan_runtime(request)
    if isinstance(session, PlanRefused):
        return session, lambda: None
    return session.ready, session.port.close


@dataclasses.dataclass(frozen=True, slots=True)
class _Written:
    """Where the plan went: the saved plan, unless `--no-plan-out`; the statements; the observation.

    Attributes:
        observation: Where the observation the plan read was recorded, or the cached one read
            from; None when a live plan recorded nothing.
    """

    plan: Path | None
    sql: Path
    observation: Path | None
    diff: Path


@dataclasses.dataclass(frozen=True, slots=True)
class _Outputs:
    """The plan's file options: `--plan-out`, `--no-plan-out`, `--sql-out` and `--diff-out`."""

    plan_out: Path | None
    no_plan_out: bool
    sql_out: Path | None
    diff_out: Path | None


def _save_plan(
    request: PlanRequest,
    ready: PlanReady,
    close: Callable[[], None],
    outputs: _Outputs,
) -> tuple[SavedPlan, Path, Path, Path | None, Path]:
    """Save the plan unless `--no-plan-out`, write its statements, diff and what it read, then close.

    The member diff file is written on every plan, so it never describes an earlier one.

    Returns:
        The saved plan, the path it is saved at or would have been, the statements' directory,
        where the observation was recorded or read from, and the member diff file.
    """
    changeset = ready.changeset
    try:
        saved = SavedPlan.from_changeset(
            changeset,
            selected=request.selected,
            excluded=request.excluded,
            include_prune=request.prune,
            partial=request.partial,
        )
        destination = outputs.plan_out or target_dir(request.project_dir) / "plan.json"
        if not outputs.no_plan_out:
            PlanFileStore(destination, root=output_root(request.project_dir, destination.parent)).write(saved)
        sql_path = write_plan_sql(request.project_dir, changeset, outputs.sql_out)
        diff_path = plan_diff_path(request.project_dir, changeset, outputs.diff_out)
        diff_text = plan_diff_text(changeset, plan_id=saved.plan_id, skipped=_diffs_skipped(request))
        write_plan_diff(request.project_dir, diff_path, diff_text)
        observed = record_observation(request, ready)
    finally:
        close()
    if ready.cached is not None:
        observed = cached_observation_path(request)
    return saved, destination, sql_path, observed, diff_path


def _diffs_skipped(request: PlanRequest) -> str | None:
    """Say why a plan read no live definitions to compare; None when it read them."""
    if request.use_cached_state:
        return "planned from a recorded observation (--use-cached-state), which reads nothing live"
    if not request.observe_options.diff:
        return "--no-diff"
    return None


def _plan_report(
    request: PlanRequest,
    ready: PlanReady,
    saved: SavedPlan,
    written: _Written,
    no_detailed_exitcode: bool,
    *,
    full: bool,
    names_only: bool,
) -> CommandResult:
    """Report a plan, exiting as `plan_exit_code` says.

    With `--partial`, what was left out is reported ahead of the plan's own diagnostics. A plan
    made from a recorded observation says so, and how old the observation is, before the plan.
    """
    changeset = ready.changeset
    shown = (
        DiagnosticBag((*ready.result.diagnostics, *changeset.diagnostics)) if request.partial else changeset.diagnostics
    )
    held = held_by_baseline(shown, request.baseline, clock=SystemClock())
    exit_code = plan_exit_code(ready, shown, detailed=not no_detailed_exitcode, held=held)
    unread = frozenset(str(item.subject) for item in changeset.diagnostics if item.code == "SST-PLN041")
    data: dict[str, object] = {
        "manifest_id": ready.manifest.manifest_id,
        "plan_id": saved.plan_id,
        "plan_path": None if written.plan is None else str(written.plan),
        "sql_path": str(written.sql),
        "diff_path": str(written.diff),
        "observation": _observation_data(ready, written.observation),
        "changes": [change_json(change, changeset.manifest_id, unread=unread) for change in changeset.changes],
        "counts": change_counts(changeset),
        "report_only": [change.key for change in changeset.report_only],
        "schemas": [scope.sql for scope in changeset.schemas],
    }
    if request.partial:
        data["partial"] = {"excluded": partial_excluded(ready.result.diagnostics)}
    if names_only:
        return CommandResult(
            exit_code,
            shown,
            data,
            human=lambda: print_changed_names(changeset),
            promoted=ready.promoted,
            headline=plan_headline(changeset),
        )

    def human() -> None:
        if ready.cached is not None:
            click.echo(f"planned from the observation recorded at {ready.cached.fetched_at} ({ready.cached.age} old)")
        print_plan(changeset, full=full, more=f"all of it in {written.diff}", unchecked=ready.cached is not None)

    return CommandResult(
        exit_code, shown, data, human=human, promoted=ready.promoted, headline=plan_headline(changeset)
    )


def _observation_data(ready: PlanReady, path: Path | None) -> dict[str, object]:
    """Describe the observation the plan was made from: when, whether cached and how old, and where."""
    cached = ready.cached
    return {
        "fetched_at": ready.changeset.observation_at,
        "cached": cached is not None,
        "age_seconds": int(cached.age_seconds) if cached is not None and cached.age_seconds is not None else None,
        "path": None if path is None else str(path),
    }
