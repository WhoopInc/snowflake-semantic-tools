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
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.apply.observation import stale_observation
from snowflake_semantic_tools.app.baseline import held_by_baseline
from snowflake_semantic_tools.app.observe import ObserveOptions
from snowflake_semantic_tools.app.plan import PlanRefused
from snowflake_semantic_tools.cli.confirm import confirm, require_answerable
from snowflake_semantic_tools.cli.exit_codes import ERROR, INTERRUPTED, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    break_stale_lock_option,
    defer_target_option,
    fail_fast_pair,
    no_validate_option,
    overwrite_option,
    partial_option,
    prune_option,
    selection_options,
    sql_out_option,
    state_option,
    target_option,
    threads_option,
    validation_options,
)
from snowflake_semantic_tools.cli.output import announce_interrupt, render_policy
from snowflake_semantic_tools.cli.plan_output import outcome_json, print_plan, write_plan_sql
from snowflake_semantic_tools.cli.render.progress import active_display
from snowflake_semantic_tools.cli.render.report import apply_headline, blocked_count, outcome_lines
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, run_baseline
from snowflake_semantic_tools.cli.settings import (
    apply_fail_fast,
    apply_parallelism,
    drift_check_setting,
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
    refuse_unvalidated,
)
from snowflake_semantic_tools.cli.wiring.project import closed_on_error
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    FailurePolicy,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.ports.progress import NO_PROGRESS
from snowflake_semantic_tools.domain.state import SavedPlan


def _refuse_invocation(
    options: GlobalOptions,
    confirmed: bool,
    prune: bool,
    partial: bool,
    no_validate: bool,
    snowflake_syntax_check: bool | None,
) -> None:
    """Refuse a command line apply cannot run.

    JSON or agent output, or a prune, without `--yes`, a run without `--yes` that could not ask for it, a partial
    prune, or a syntax check with `--no-validate`. Each is refused before anything is planned.

    Raises:
        SstUsageError: as `_require_yes`, `require_answerable`, `refuse_partial_prune` and
            `refuse_unvalidated` say.
    """
    if options.output in ("json", "agent") and not confirmed:
        _require_yes(f"sst apply --output {options.output}")
    if prune and not confirmed:
        _require_yes("sst apply --prune")
    if not confirmed:
        require_answerable("sst apply")
    refuse_partial_prune(partial, prune)
    refuse_unvalidated(no_validate, snowflake_syntax_check)


@click.command(cls=SstCommand)
@target_option()
@selection_options()
@state_option()
@defer_target_option()
@click.option("--plan", "plan_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@prune_option()
@partial_option()
@overwrite_option()
@click.option("--yes", "-y", "confirmed", is_flag=True)
@fail_fast_pair()
@threads_option()
@break_stale_lock_option()
@click.option("--temporary", is_flag=True)
@no_validate_option()
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
    overwrite: bool,
    confirmed: bool,
    fail_fast: bool | None,
    threads: int | None,
    break_stale_lock: bool,
    temporary: bool,
    no_validate: bool,
    sql_out: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    options: GlobalOptions,
) -> CommandResult:
    """Apply a current reviewed plan; smoke probes never run here.

    Without --plan, apply plans afresh as `sst plan` does, prints the plan, and asks before it
    writes; --yes skips the question, and is required with --prune, with --output json, and
    whenever stdin or stderr is not a terminal, which apply refuses before it plans. The plan it
    asks about shows what each update changes, member by member, as `sst plan` does; with --yes
    nothing is shown, so those definitions are not read.
    With --plan, the saved plan's selection and --prune are used: --select, --exclude, --prune
    and --partial must agree with it, and it must have been made for this target, from the
    manifest the project compiles now, observing what Snowflake holds now. The statements are
    written to `sst_target/sql`, or --sql-out, before anything runs, and the changes run under
    the target's run lock. A failed change skips the changes that depend on it; with
    --fail-fast, every change not yet run. `--fail-fast` and `--no-fail-fast` override
    `apply.fail_fast`, false by default, for this run. With --partial, the changes no error
    touches are applied, those left out are reported, and the run exits 1 while errors remain.
    Interrupted by Ctrl-C, SIGTERM or SIGHUP, apply starts no further change, waits for those
    running, records what they wrote in state, and releases the lock; a second Ctrl-C stops
    the running changes at their next statement.

    Exit 0 when every change applied; 1 when a change failed, the plan has errors, or another
    run holds the lock; 3 when apply refuses the command line; 4 when the project, its
    configuration, or the saved plan cannot be used; 5 when Snowflake cannot be reached; and
    130 when the question is declined or the run is interrupted.

    Diagnostics:
        SST-PRT109: --prune or --output json without --yes, or no terminal to ask on; raised.
        SST-PRT107: the run was interrupted while its changes ran.
        SST-PRT104: --partial with --prune, or --no-validate with --snowflake-syntax-check; raised.
        SST-APL005: the saved plan was made for another target.
        SST-MAN030: the saved plan's observation is past its time to live; apply observed afresh.
        SST-APL010: the run took over an expired lock, with --break-stale-lock.
        SST-APL011: another run holds the lock.
        SST-APL013: --temporary would publish to a production-like target.
        SST-APL103: Snowflake ended a session the run used, after it took the lock.
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
        observe_options=ObserveOptions(drift_check=drift_check_setting(paths), diff=not confirmed),
        validate=not no_validate,
        baseline=run_baseline(options),
        overwrite=overwrite,
    )
    saved = _saved_plan(plan_path, request)
    planned = request.following(saved)
    session = plan_runtime(planned)
    if isinstance(session, PlanRefused):
        return CommandResult(ERROR, session.diagnostics, promoted=session.promoted)
    with closed_on_error(session.port):
        apply_options, notes, sql_path = _confirmed_options(
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
    result = _apply_plan(planned, session, apply_options, notes, sql_path)
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
        ProjectError: no saved plan is at the path, or it cannot be read (SST-PRT009).
        SstUsageError: `--select`, `--exclude`, `--prune`, `--partial`, or `--overwrite` disagrees
            with the saved plan.

    Diagnostics:
        SST-PRT009: no saved plan is at the path, or the one there cannot be read; raised.
    """
    if plan_path is None:
        return None
    saved = PlanFileStore(plan_path).read()
    if saved is None:
        # `--plan` must name a file, so it is absent only when it went between the check and the read.
        absent = D("SST-PRT009", subject="cli", path=str(plan_path), detail="there is no saved plan")
        raise ProjectError(diagnostics=(absent,))
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
    # So is overwriting a hand edit.
    if saved.overwrite and not request.overwrite:
        raise SstUsageError("the saved plan overwrites changes made out of band; apply it with --overwrite")
    if request.overwrite and not saved.overwrite:
        raise SstUsageError("--overwrite conflicts with a saved plan made without it")
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
) -> tuple[ApplyOptions, DiagnosticBag, Path]:
    """Check a saved plan still applies, write the statements, and confirm; return how to apply.

    Asks before applying a plan that writes, unless `--yes` was given. Returns the options,
    what checking the saved plan reported without refusing it, and where the statements are.

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
        overwrite=request.overwrite,
    )
    notes = DiagnosticBag()
    if saved is not None:
        mismatch = saved.check_applicable(current, source=str(plan_path))
        if mismatch is not None:
            raise ProjectError(mismatch.message, diagnostics=mismatch.diagnostics)
        stale = stale_observation(saved, current, source=str(plan_path))
        notes = DiagnosticBag((stale,) if stale is not None else ())
    sql_path = write_plan_sql(request.project_dir, changeset, sql_out)
    if changeset.writes and not confirmed:
        print_plan(changeset, more="sst plan --full shows all")
        confirm("Apply this plan?")
    options = ApplyOptions(
        parallelism=apply_parallelism(request.paths, threads),
        on_failure=(FailurePolicy.STOP_ALL if fail_fast else FailurePolicy.STOP_DEPENDENTS),
        allow_prune=request.prune,
        break_stale_lock=break_stale_lock,
        temporary=request.temporary,
        held=held_by_baseline(changeset.diagnostics, request.baseline, clock=SystemClock()),
    )
    return options, notes, sql_path


def _apply_plan(
    request: PlanRequest, session: PlanSession, options: ApplyOptions, notes: DiagnosticBag, sql_path: Path
) -> CommandResult:
    """Apply the plan, close the connection, and report each outcome; exit 1 unless everything applied.

    A partial apply publishes the healthy changes and still exits 1 while errors remain; an
    interrupted one exits 130 (INTERRUPTED) with SST-PRT107, reporting what did run.
    `notes` are reported first, then what the plan left out, then the run's own diagnostics.
    """
    ready = session.ready
    params = session.profile.connection_params
    # Changes run on connections leased from the pool, never on `port`, which keeps the plan
    # reads, the run lock, and the state write; the heartbeat has a connection of its own.
    # Should Snowflake end `port`'s session mid-run, a fresh connection writes state and
    # releases the lock.
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
                phases=RUN_TIMINGS,
                progress=active_display() or NO_PROGRESS,
                reconnect=lambda: contextlib.closing(project.open_connector(params)),
            ).run(ready.changeset, ready.state, options)
    finally:
        session.port.close()
    left_out = ready.result.diagnostics if request.partial else DiagnosticBag()
    notice = _interrupt_notice(apply_result.state_written) if apply_result.interrupted else None
    shown = DiagnosticBag((*notes, *left_out, *apply_result.diagnostics, *((notice,) if notice else ())))
    exit_code = OK if apply_result.success and not left_out.has_errors else ERROR
    outcomes = apply_result.outcomes
    data: dict[str, object] = {
        "run_id": apply_result.run_id,
        "state_written": apply_result.state_written,
        "interrupted": apply_result.interrupted,
        "applied": _keys(outcomes, OutcomeStatus.APPLIED, prune=False),
        "failed": [outcome.key for outcome in outcomes if outcome.status is OutcomeStatus.FAILED],
        "pruned": _keys(outcomes, OutcomeStatus.APPLIED, prune=True),
        "sql_path": str(sql_path),
        "outcomes": [outcome_json(outcome) for outcome in outcomes],
    }
    if request.partial:
        data["partial"] = {"excluded": partial_excluded(ready.result.diagnostics)}
    if notice is not None:
        interrupted = notice

        def report() -> None:
            _print_outcomes(outcomes)
            announce_interrupt(interrupted)

        return CommandResult(INTERRUPTED, shown, data, human=report, promoted=ready.promoted)
    return CommandResult(
        exit_code,
        shown,
        data,
        human=lambda: _print_outcomes(apply_result.outcomes),
        promoted=ready.promoted,
        headline=apply_headline(apply_result.outcomes),
        failure=blocked_count(ready.changeset),
    )


def _interrupt_notice(state_written: bool) -> Diagnostic:
    """Report an apply an interrupt stopped while its changes ran, and what state then holds.

    Diagnostics:
        SST-PRT107: the run was interrupted; its changes that finished are recorded in state,
            unless the run lost its lock.
    """
    value = (
        "the changes that finished are recorded in state and the rest were skipped; run sst plan to see what remains"
        if state_written
        else "partial work may have been applied; run sst plan to see what remains"
    )
    return D("SST-PRT107", subject="cli", detail="sst apply started", value=value)


def _keys(outcomes: tuple[ApplyOutcome, ...], status: OutcomeStatus, *, prune: bool) -> list[str]:
    """Return the keys of the outcomes with `status`: the prunes' when `prune`, else every other's."""
    return [
        outcome.key for outcome in outcomes if outcome.status is status and (outcome.action is Action.PRUNE) == prune
    ]


def _print_outcomes(outcomes: tuple[ApplyOutcome, ...]) -> None:
    terminal = render_policy().terminal
    for line in outcome_lines(outcomes, Style(terminal, stdout=True)):
        click.echo(line, color=True if terminal.color_out else None)
