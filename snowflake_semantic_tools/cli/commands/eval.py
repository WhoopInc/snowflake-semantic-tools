"""`sst eval`: run each agent's evals in Snowflake, as published or as it compiles now.

A published run first compiles the project and stops on its errors, then refuses to run unless
`sst compile` wrote the manifest the project compiles to now; an unpublished run needs neither.
"""

from __future__ import annotations

import dataclasses
import secrets
import socket
from collections.abc import Mapping
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.adapters.snowflake.eval_state import SnowflakeEvalStateStore
from snowflake_semantic_tools.app.ask import agent_scope, compiled_agents
from snowflake_semantic_tools.app.ask_scratch import ScratchPort, ScratchSession, edited_dependencies
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.inline import (
    InlineEvalOutcome,
    InlineEvalSetup,
    RunInlineEvals,
    unpublished_refusals,
)
from snowflake_semantic_tools.app.evals.progress import (
    AttemptEnded,
    AttemptStarted,
    EvalProgressEvent,
    StatusChanged,
    StatusReadFailed,
    StatusReadTimedOut,
)
from snowflake_semantic_tools.app.evals.run import suite_concurrency
from snowflake_semantic_tools.app.evals.suite import (
    EvalGateOutcome,
    EvalGateRefused,
    EvalGateRequest,
    RunEvalGate,
    compiled_evals,
)
from snowflake_semantic_tools.cli.compare import CompareFlags, refresh_comparison, run_comparison
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    break_stale_lock_option,
    defer_target_option,
    fail_fast_option,
    selection_options,
    target_option,
    threads_option,
)
from snowflake_semantic_tools.cli.output import print_progress
from snowflake_semantic_tools.cli.plan_output import print_eval_results
from snowflake_semantic_tools.cli.render.diagnostics import plural
from snowflake_semantic_tools.cli.render.evals import EvalMarks
from snowflake_semantic_tools.cli.render.next_steps import capture_baseline_step
from snowflake_semantic_tools.cli.render.progress import active_display
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import threads_setting
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.manifest import current_manifest, refuse_stale_inputs
from snowflake_semantic_tools.cli.wiring.project import (
    closed_on_error,
    connect,
    open_connector,
    project_inputs,
    published_entries,
    state_store,
)
from snowflake_semantic_tools.cli.wiring.selection import narrowed, noted
from snowflake_semantic_tools.domain.ask.spec import rewrite_spec
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval.model import EVAL_PASS_STATUSES
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.state import AppliedEntry


def _refuse_invocation(
    capture_baseline_requested: bool,
    reason: str | None,
    unpublished: bool,
    compare_models: str | None,
    compare_runs: int | None,
    compare_report: Path | None,
    refresh_cost: Path | None,
) -> None:
    """Refuse the flags that never run together, before anything is resolved.

    Raises:
        SstUsageError: `--capture-baseline` and `--reason` are not given together; or
            `--unpublished` is given with `--capture-baseline`; or `--compare-runs` or
            `--compare-report` is given without `--compare-models`; or a comparison flag is
            given as `_refuse_comparison` refuses it.
    """
    _refuse_comparison(capture_baseline_requested, unpublished, compare_models, refresh_cost)
    if compare_models is None and (compare_runs is not None or compare_report is not None):
        flag = "--compare-runs" if compare_runs is not None else "--compare-report"
        raise SstUsageError(f"{flag} sets how --compare-models compares; add --compare-models")
    if unpublished and capture_baseline_requested:
        raise SstUsageError("--unpublished never captures a baseline: an unpublished run is not comparable")
    if capture_baseline_requested and not reason:
        raise SstUsageError("--capture-baseline requires --reason")
    if reason and not capture_baseline_requested:
        raise SstUsageError("--reason requires --capture-baseline")


def _refuse_comparison(
    capture_baseline_requested: bool,
    unpublished: bool,
    compare_models: str | None,
    refresh_cost: Path | None,
) -> None:
    """Refuse a comparison, or a refresh of one, beside what it never does.

    Raises:
        SstUsageError: `--compare-models` or `--refresh-cost` is given with `--capture-baseline` or
            `--unpublished`; or both are given.
    """
    flag = "--compare-models" if compare_models is not None else "--refresh-cost" if refresh_cost else None
    if flag is None:
        return
    if compare_models is not None and refresh_cost is not None:
        raise SstUsageError("--refresh-cost rereads a comparison's cost and runs nothing; drop --compare-models")
    if capture_baseline_requested:
        raise SstUsageError(f"{flag} never captures a baseline: a comparison gates nothing")
    if unpublished:
        raise SstUsageError(f"{flag} compares published versions; drop --unpublished")


@click.command(cls=SstCommand, name="eval")
@selection_options()
@target_option()
@defer_target_option()
@threads_option()
@fail_fast_option()
@click.option("--capture-baseline", "capture_baseline_requested", is_flag=True)
@click.option("--reason")
@click.option("--unpublished", is_flag=True)
@click.option("--compare-models", "compare_models", is_flag=False, flag_value="", default=None, metavar="[LIST]")
@click.option("--compare-runs", type=click.IntRange(min=1))
@click.option("--compare-report", type=click.Path(file_okay=False, path_type=Path))
@click.option("--refresh-cost", type=click.Path(dir_okay=False, path_type=Path))
@break_stale_lock_option()
@command_body("eval", refusals=_refuse_invocation)
def eval_command(
    paths: ProjectPaths,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    target_name: str | None,
    manifest_path: Path | None,
    threads: int | None,
    fail_fast: bool,
    capture_baseline_requested: bool,
    reason: str | None,
    unpublished: bool,
    compare_models: str | None,
    compare_runs: int | None,
    compare_report: Path | None,
    refresh_cost: Path | None,
    break_stale_lock: bool,
) -> CommandResult:
    """Run agent evals in Snowflake and gate them against their baselines; billed.

    Without a mode flag, each selected eval runs against the agent `sst apply` published, once
    `sst compile` has written the manifest, and the gate compares its scores with the eval's
    baseline. `--select` and `--exclude` narrow the evals run. Exit 1 when the project does
    not compile, the gate fails, or another run holds the target's run lock; 3 when the
    selection compiles no eval; 4 when the compiled manifest is missing or stale; and 5 when
    Snowflake cannot be reached. The run holds the target's run lock while it runs, and
    `--break-stale-lock` takes it over only from a run that has expired; progress on its runs
    goes to stderr, except under --quiet or --output json. `--capture-baseline` records the
    scores as the baselines later gates compare with.

    `--unpublished` runs each eval against its agent as it compiles now, through temporary
    agents: it needs no `sst compile` or `sst apply` first, takes no lock, and reads and writes
    no baseline or eval state. Only `tool_selection_accuracy`, which it scores exactly, can fail
    it; the metrics it judges are approximate and reported only.

    `--compare-models` runs the evals as a model comparison (`cli.compare`): it gates nothing
    and exits 1 only when a precheck refuses or a version could not be committed or restored.
    `--refresh-cost` rereads an earlier comparison's cost and runs nothing, exiting 4 for a file
    SST did not write. Both exit 3 with `--capture-baseline` or `--unpublished`.
    """
    if refresh_cost is not None:
        return refresh_comparison(paths, target_name, refresh_cost)
    if not unpublished:
        refuse_stale_inputs(paths, target_name, manifest_path, before="eval")
    compiled = compiling.compile_result(paths, target_name, manifest_path)
    if not compiled.success:
        return CommandResult(ERROR, compiled.diagnostics)
    result, notes = narrowed(paths, compiled, selected, excluded, manifest_path, target_name)
    inputs = project_inputs(paths, target_name, manifest_path)
    workers = threads_setting(paths, threads, default=1)
    results = (compiled, result)
    if compare_models is not None:
        comparing = CompareFlags(compare_models, compare_runs, compare_report, break_stale_lock)
        evals = _selected_evals(result, selected)
        report = run_comparison(paths, target_name, results, inputs, evals, comparing, _print_eval_progress)
    elif unpublished:
        report = _run_unpublished_evals(paths, target_name, results, inputs, selected, workers, fail_fast)
    else:
        request = EvalGateRequest(
            fail_fast, capture_baseline_requested, reason, threads=workers, break_stale_lock=break_stale_lock
        )
        report = _run_evals(paths, target_name, results, inputs, selected, request)
    return noted(report, notes)


def _run_evals(
    paths: ProjectPaths,
    target_name: str | None,
    results: tuple[CompileResult, CompileResult],
    inputs: YamlProjectInputs,
    selected: tuple[str, ...],
    request: EvalGateRequest,
) -> CommandResult:
    """Run the selected evals against their published agents and gate them; exit 1 unless the run passes.

    `results` is the whole compile, which the compiled manifest must match, and the selection.
    SIGTERM and SIGHUP interrupt the run as Ctrl-C does (`cli.signals`, which every command
    runs under), so the run lock is released either way. While
    the runs go on, each attempt's start, status changes and end, every status read retried,
    and a line naming the runs still in flight at most once a minute, go to stderr.

    Another operation holding the target's lock refuses the run with exit 1, as it refuses
    `sst apply`, carrying what taking the lock reported and the reason under `data.error`.

    Raises:
        ProjectError: the selection compiles no eval, or the compiled manifest is stale.
    """
    full, result = results
    evals = _selected_evals(result, selected)
    manifest = current_manifest(paths.project_dir, full, inputs, before="eval")
    profile, port = connect(paths, target_name)
    params = profile.connection_params
    workers = suite_concurrency(evals, inputs.eval_catalog().defaults, request.threads)
    with (
        closed_on_error(port),
        ConnectorPool(workers, lambda: open_connector(params)) as pool,
    ):
        store = state_store(paths, profile.target_name)
        eval_store = SnowflakeEvalStateStore(port, _eval_state_table(profile.state_table))
        outcome = RunEvalGate(
            port,
            inputs,
            store,
            eval_store,
            SystemClock(),
            actor=profile.identity.role or "",
            host=socket.gethostname(),
            sessions=pool,
            progress=_print_eval_progress,
        ).run(evals, manifest, request, target=profile.identity, state_table=profile.state_table)
    port.close()
    if isinstance(outcome, EvalGateRefused):
        return CommandResult(ERROR, outcome.diagnostics, {"error": outcome.reason})
    return _eval_report(outcome)


def _selected_evals(result: CompileResult, selected: tuple[str, ...]) -> tuple[CompiledEval, ...]:
    """Return the evals the selection compiles, refusing a selection that compiles none.

    Raises:
        ProjectError: the selection compiles no eval.

    Diagnostics:
        SST-DIS010: the selection compiles no eval, one per selector; raised.
    """
    evals = compiled_evals(result)
    if not evals:
        # The run selects the evals within `selected`; with no selection, its own `type:eval`.
        named = selected or ("type:eval",)
        raise ProjectError(
            "no eval artifacts matched the project",
            diagnostics=tuple(D("SST-DIS010", selector=value) for value in named),
        )
    return evals


def _run_unpublished_evals(
    paths: ProjectPaths,
    target_name: str | None,
    results: tuple[CompileResult, CompileResult],
    inputs: YamlProjectInputs,
    selected: tuple[str, ...],
    threads: int,
    fail_fast: bool,
) -> CommandResult:
    """Run the selected evals against their agents as they compile now; exit 1 unless the run passes.

    `results` is the whole compile, which holds every eval's agent and every artifact an
    agent depends on, and the selection. No lock is taken, and no manifest, state, baseline
    or eval state is read or written beyond one read of the state table, which checks what
    each agent calls that is published differently (SST-ASK001). Each question's end goes to
    stderr, except under --quiet or --output json.

    Raises:
        ProjectError: the selection compiles no eval, or an agent's scratch schema renders
            unusable; it carries the diagnostic.

    Diagnostics:
        SST-ASK013: the selection holds a CoWork profile's eval; nothing runs, and nothing
            connects.
    """
    full, result = results
    evals = _selected_evals(result, selected)
    # Snowflake private preview (PrPr): CoWork profiles. A profile's eval runs published only.
    refused = unpublished_refusals(evals)
    if refused:
        return CommandResult(ERROR, DiagnosticBag(refused))
    agents = {agent.artifact_key: agent for agent in compiled_agents(full)}
    defaults = inputs.eval_catalog().defaults
    profile, port = connect(paths, target_name)
    params = profile.connection_params
    workers = suite_concurrency(evals, defaults, threads)
    sessions: dict[str, ScratchSession] = {}
    with closed_on_error(port), ConnectorPool(workers, lambda: open_connector(params)) as pool:
        applied = published_entries(port, profile, paths.config_name)
        setups: list[InlineEvalSetup] = []
        notes: list[Diagnostic] = []
        try:
            for item in evals:
                agent = agents[item.resolved.evaluated.key]
                scope = agent_scope(agent, profile.identity)
                if isinstance(scope, Diagnostic):
                    raise ProjectError(scope.message, diagnostics=(scope,))
                copied = _scratch_agent(port, agent, full, applied, scope, sessions)
                if isinstance(copied, Diagnostic):
                    return CommandResult(ERROR, DiagnosticBag((copied,)))
                setups.append(InlineEvalSetup(item, copied, scope))
            outcome = RunInlineEvals(port, pool, progress=print_progress).run(
                setups, defaults=defaults, threads=threads, fail_fast=fail_fast
            )
        finally:
            for session in sessions.values():
                notes.extend(session.drop())
    port.close()
    return CommandResult(
        OK if outcome.passed else ERROR,
        DiagnosticBag((*outcome.diagnostics, *notes)),
        dict(outcome.data),
        human=lambda: _print_unpublished(outcome),
    )


def _print_unpublished(outcome: InlineEvalOutcome) -> None:
    """Print every eval as a published run prints it, then the run's verdict.

    Its judged metrics are marked approximate, and those only the evaluation service scores
    unscored.
    """
    marks = EvalMarks(
        approximate=frozenset(_names(outcome.data.get("approximated_metrics"))),
        unscored=frozenset(_names(outcome.data.get("unscored_metrics"))),
    )
    print_eval_results(outcome.suite, marks)
    click.echo(f"unpublished eval run {outcome.data.get('gate_verdict')}")


def _names(value: object) -> list[str]:
    return [str(name) for name in value] if isinstance(value, list) else []


# The eval key each run in flight is shown under on a terminal, by run name.
_EVAL_LABELS: dict[str, str] = {}


def _scratch_agent(
    port: ScratchPort,
    agent: CompiledAgent,
    full: CompileResult,
    applied: Mapping[str, AppliedEntry] | None,
    scope: SchemaScope,
    sessions: dict[str, ScratchSession],
) -> CompiledAgent | Diagnostic:
    """Return the agent pointed at scratch copies of what you edited that it reaches; made once per agent.

    Diagnostics:
        SST-ASK008: a copy was refused.
    """
    session = sessions.get(agent.artifact_key)
    if session is None:
        session = ScratchSession(port, scope, secrets.token_hex(4), print_progress)
        sessions[agent.artifact_key] = session
        failed = session.build(edited_dependencies(agent, full, applied))
        if failed is not None:
            return failed
    payload, _ = rewrite_spec(agent.payload, session.renames)
    return dataclasses.replace(agent, payload=payload)


def _print_eval_progress(event: EvalProgressEvent) -> None:
    """Show one eval progress event: as a row of the live block on a terminal, else as a line.

    On a terminal each run is a unit, labeled with its eval's key, whose status the events
    change, and which leaves one finished line when it ends; the heartbeat says nothing the rows
    do not. A status read that failed or timed out is also printed as a warning, so it outlives
    the row. Elsewhere each event prints as its line, a failed or timed-out read as a warning.
    """
    display = active_display()
    if display is None or not display.live:
        print_progress(event.line, warning=isinstance(event, (StatusReadFailed, StatusReadTimedOut)))
        return
    if isinstance(event, AttemptStarted):
        _EVAL_LABELS[event.run_name] = event.label or event.run_name
        attempt = f"attempt {event.attempt} of {event.attempt_limit}" if event.attempt_limit > 1 else "starting"
        display.unit_started("evals", _EVAL_LABELS[event.run_name], attempt)
    elif isinstance(event, StatusChanged):
        display.unit_status("evals", _EVAL_LABELS.get(event.run_name, event.run_name), event.status)
    elif isinstance(event, AttemptEnded):
        label = _EVAL_LABELS.pop(event.run_name, event.run_name)
        display.unit_finished("evals", label, ok=event.status in EVAL_PASS_STATUSES, status=event.status)
    elif isinstance(event, (StatusReadFailed, StatusReadTimedOut)):
        label = _EVAL_LABELS.get(event.run_name, event.run_name)
        display.unit_status("evals", label, "status read retrying")
        print_progress(event.line, warning=True)


def _eval_state_table(state_table: QualifiedName) -> QualifiedName:
    """Name the table eval baselines and gates are kept in: beside the state table, suffixed `_EVALS`."""
    return QualifiedName(state_table.database, state_table.schema, Identifier.shown(f"{state_table.name.folded}_EVALS"))


def _eval_report(outcome: EvalGateOutcome) -> CommandResult:
    """Report an eval run, its diagnostics and then every attempt; exit 1 unless it passed.

    When a gate compared nothing for want of a usable baseline, the next step is the capture
    that replaces it, since running the evals again cannot pass. A run that passed closes with
    what the gate decided, as `_eval_headline` says it.
    """
    suite = outcome.suite
    return CommandResult(
        OK if outcome.passed else ERROR,
        outcome.diagnostics,
        dict(outcome.data),
        human=None if suite is None else lambda: print_eval_results(suite, _gate_marks(outcome.data)),
        headline=_eval_headline(outcome.data),
        next_steps=(capture_baseline_step(outcome.recapture),) if outcome.recapture else None,
    )


def _eval_headline(data: Mapping[str, object]) -> str | None:
    """Say what the gate decided: `eval gate passed: 2 evals, no regressions`, or `captured 1 baseline`.

    A report-tier regression does not fail the gate, so it is counted in the passing line. None
    for a verdict that is no pass, which the failed verdict reports in its own words.
    """
    verdict = data.get("gate_verdict")
    if verdict == "captured":
        captured = data.get("captured_baselines")
        return f"captured {plural(len(captured) if isinstance(captured, list) else 0, 'baseline')}"
    if verdict not in ("passed", "regressed"):
        return None
    evals = data.get("evals")
    regressions = data.get("regression_count")
    count = regressions if isinstance(regressions, int) else 0
    found = plural(count, "report-tier regression") if count else "no regressions"
    return f"eval gate passed: {plural(len(evals) if isinstance(evals, list) else 0, 'eval')}, {found}"


def _gate_marks(data: Mapping[str, object]) -> EvalMarks:
    """Mark the questions and metrics the gate found regressed."""
    regressions = data.get("regressions")
    pairs = (
        (str(item.get("question_key")), str(item.get("metric_name")))
        for item in (regressions if isinstance(regressions, list) else ())
        if isinstance(item, dict)
    )
    return EvalMarks(regressions=frozenset(pairs))
