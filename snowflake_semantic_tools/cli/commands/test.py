"""`sst test`: the smoke and eval suites, run against what `sst apply` published.

Every suite first compiles the project and stops on its errors, then refuses to run unless
`sst compile` wrote the manifest the project compiles to now.
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
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.app.ask import agent_scope, compiled_agents
from snowflake_semantic_tools.app.ask_scratch import ScratchPort, ScratchSession, edited_dependencies
from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.inline import InlineEvalOutcome, InlineEvalSetup, RunInlineEvals
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
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.smoke import SmokePublished
from snowflake_semantic_tools.cli.exit_codes import CONFIG, CONNECTION, ERROR, OK
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
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest, current_manifest, lineage_of
from snowflake_semantic_tools.cli.wiring.project import (
    closed_on_error,
    connect,
    open_connector,
    project_inputs,
    published_entries,
    state_store,
    target_dir,
)
from snowflake_semantic_tools.domain.ask.spec import rewrite_spec
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval.model import EVAL_PASS_STATUSES
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.progress import NO_PROGRESS
from snowflake_semantic_tools.domain.state import AppliedEntry

SUITES = ("smoke", "evals")
# Which exit code wins when suites disagree: Snowflake unreachable, then a setup failure, then 1.
_EXIT_RANK = {OK: 0, ERROR: 1, CONFIG: 2, CONNECTION: 3}


def _refuse_invocation(
    suites: tuple[str, ...], capture_baseline_requested: bool, reason: str | None, unpublished: bool
) -> None:
    """Refuse `--capture-baseline` and `--reason` apart, or where the command line runs no eval suite.

    Raises:
        SstUsageError: `--capture-baseline` and `--reason` are not given together, or are given
            with a `--suite` that does not name `evals`; or `--unpublished` is given with
            `--capture-baseline`, or with a `--suite` that does not name `evals`.
    """
    if unpublished and capture_baseline_requested:
        raise SstUsageError("--unpublished never captures a baseline: an unpublished run is not comparable")
    if unpublished and suites and "evals" not in suites:
        raise SstUsageError("--unpublished runs the eval suite; add --suite evals")
    if capture_baseline_requested and not reason:
        raise SstUsageError("--capture-baseline requires --reason")
    if reason and not capture_baseline_requested:
        raise SstUsageError("--reason requires --capture-baseline")
    if not capture_baseline_requested:
        return
    if suites and "evals" not in suites:
        raise SstUsageError("--capture-baseline captures an eval baseline; add --suite evals")


@click.command(cls=SstCommand, name="test")
@click.option("--suite", "suites", type=click.Choice(SUITES), multiple=True)
@selection_options()
@target_option()
@defer_target_option()
@threads_option()
@fail_fast_option()
@click.option("--capture-baseline", "capture_baseline_requested", is_flag=True)
@click.option("--reason")
@click.option("--unpublished", is_flag=True)
@break_stale_lock_option()
@command_body("test", refusals=_refuse_invocation)
def test_command(
    paths: ProjectPaths,
    suites: tuple[str, ...],
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    target_name: str | None,
    manifest_path: Path | None,
    threads: int | None,
    fail_fast: bool,
    capture_baseline_requested: bool,
    reason: str | None,
    unpublished: bool,
    break_stale_lock: bool,
) -> CommandResult:
    """Run the smoke and eval suites: those --suite names, else every one that applies.

    Both suites test what `sst apply` published, so they apply once `sst compile` has written
    the manifest: the smoke suite when a semantic view compiles, the eval suite when an eval
    does. `--select` and `--exclude` narrow every suite to those artifacts. Exit 1 when the
    project does not compile, a suite fails, or another run holds the target's run lock; 4
    when, with no --suite, no suite applies, or a suite finds the compiled manifest missing
    or stale; and 5 when a suite cannot reach Snowflake. Unless one --suite is given, the worst exit wins and
    the summary counts the suites that passed, failed, and were skipped: not named, not
    applying, or not reached once --fail-fast stopped. The eval suite holds the target's run
    lock while it runs, and `--break-stale-lock` takes it over only from a run that has
    expired; progress on its runs goes to stderr, except under --quiet or --output json.
    `--capture-baseline` exits 3 unless the eval suite runs.

    `--unpublished` runs the eval suite, alone unless --suite names smoke too, against each
    agent as it compiles now, through temporary agents: it needs no `sst compile` or `sst
    apply` first, takes no lock, and reads and writes no baseline or eval state. Only
    `tool_selection_accuracy`, which it scores exactly, can fail it; the metrics it judges are
    approximate and reported only. It exits 3 with `--capture-baseline`, or with a --suite
    that does not name evals.
    """
    compiled = compiling.compile_result(paths, target_name, manifest_path)
    if not compiled.success:
        return CommandResult(ERROR, compiled.diagnostics)
    graph = lineage_of(paths, compiled, manifest_path, target_name)
    narrowed = (
        compiling.selected_within(paths.project_dir, compiled, selected, excluded, graph=graph)
        if selected or excluded
        else None
    )
    result = compiled if narrowed is None else narrowed.result
    notes = DiagnosticBag() if narrowed is None else narrowed.report
    inputs = project_inputs(paths, target_name, manifest_path)
    workers = threads_setting(paths, threads, default=1)
    if capture_baseline_requested and not suites and not _evals_apply(paths, result):
        raise SstUsageError(
            "--capture-baseline captures an eval baseline, and the eval suite does not apply: "
            "run sst compile first, or select an eval"
        )
    chosen = suites or (("evals",) if unpublished else _applying(paths, result))
    reports: list[tuple[str, CommandResult]] = []
    for name in dict.fromkeys(chosen):
        if name == "evals" and unpublished:
            report = _run_unpublished_evals(
                paths, target_name, (compiled, result), inputs, selected, workers, fail_fast
            )
        elif name == "evals":
            report = _run_evals(
                paths,
                target_name,
                (compiled, result),
                inputs,
                selected,
                EvalGateRequest(
                    fail_fast, capture_baseline_requested, reason, threads=workers, break_stale_lock=break_stale_lock
                ),
            )
        else:
            report = _run_smoke(paths, target_name, (compiled, result), inputs, fail_fast, workers)
        reports.append((name, report))
        if fail_fast and report.exit_code:
            break
    if len(suites) == 1:
        return _noted(reports[0][1], notes)
    # A suite --fail-fast stopped before is skipped, as one that does not apply is.
    ran = {name for name, _ in reports}
    return _noted(_combined(reports, skipped=tuple(name for name in SUITES if name not in ran)), notes)


def _noted(result: CommandResult, notes: DiagnosticBag) -> CommandResult:
    """Report what the selectors' report says ahead of the suites' own diagnostics."""
    if not notes:
        return result
    return dataclasses.replace(result, diagnostics=DiagnosticBag((*notes, *result.diagnostics)))


def _applying(paths: ProjectPaths, result: CompileResult) -> tuple[str, ...]:
    """Name the suites that apply when --suite names none, refusing a run that would test nothing.

    Raises:
        ProjectError: `sst compile` has written no manifest (SST-MAN001), or the selection
            compiles no semantic view and no eval (SST-DIS012).
    """
    compiled_manifest(paths.project_dir)
    applying = tuple(
        name
        for name, applies in (
            ("smoke", any(isinstance(item, CompiledView) for item in result.compiled)),
            ("evals", bool(compiled_evals(result))),
        )
        if applies
    )
    if not applying:
        diagnostic = D("SST-DIS012")
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return applying


def _evals_apply(paths: ProjectPaths, result: CompileResult) -> bool:
    """Report whether the eval suite applies with no --suite: `sst compile` has run, and an eval compiles."""
    return (target_dir(paths.project_dir) / "manifest.json").is_file() and bool(compiled_evals(result))


def _combined(reports: list[tuple[str, CommandResult]], *, skipped: tuple[str, ...]) -> CommandResult:
    """Report several suites as one run: every diagnostic, each suite's data, the worst exit code."""
    failed = [name for name, report in reports if report.exit_code]
    worst = max((report.exit_code for _, report in reports), default=OK, key=lambda code: _EXIT_RANK.get(code, 0))
    data = {
        "suites": [name for name, _ in reports],
        "results": [report.data for _, report in reports],
        "passed": len(reports) - len(failed),
        "failed": len(failed),
        "skipped": len(skipped),
        "skipped_suites": list(skipped),
    }

    summary = f"{plural(len(reports) - len(failed), 'suite')} passed, {len(failed)} failed, {len(skipped)} skipped"

    def human() -> None:
        for _, report in reports:
            if report.human is not None:
                report.human()
        # A run that passed says this in its verdict; one that failed says which suites did.
        if failed:
            click.echo(f"{summary}: {', '.join(failed)} failed")

    diagnostics = DiagnosticBag(tuple(item for _, report in reports for item in report.diagnostics))
    return CommandResult(worst, diagnostics, data, human=human, headline=summary)


def _run_smoke(
    paths: ProjectPaths,
    target_name: str | None,
    results: tuple[CompileResult, CompileResult],
    inputs: YamlProjectInputs,
    fail_fast: bool,
    threads: int,
) -> CommandResult:
    """Probe the published objects once SST is proven to own them; exit 1 when a check or probe fails.

    `results` is the whole compile, which the compiled manifest must match, and the selection
    the probes run on. The markers are read, and the probes run, on up to `threads` sessions
    at once; every session but the first is closed before the connection is.
    """
    full, result = results
    manifest = current_manifest(paths.project_dir, full, inputs, before="smoke")
    profile, port = connect(paths, target_name)
    params = profile.connection_params
    try:
        with ConnectorPool(threads, lambda: open_connector(params)) as pool:
            readers = Fanout(port, pool, threads)
            smoke = SmokePublished(
                port,
                state_store(paths, profile.target_name),
                readers,
                phases=RUN_TIMINGS,
                progress=active_display() or NO_PROGRESS,
            ).run(
                result,
                manifest,
                target=profile.identity,
                state_table=profile.state_table,
                fail_fast=fail_fast,
            )
    finally:
        port.close()
    if smoke.unpublished:
        print_progress(f"- smoke probes skipped: {plural(smoke.unpublished, 'artifact')} not published as compiled")
    exit_code = OK if smoke.success else ERROR
    attempted = len(smoke.attempted)
    return CommandResult(
        exit_code,
        smoke.diagnostics,
        data={"suite": "smoke", "attempted": attempted},
        headline=f"smoke suite passed: {plural(attempted, 'probe')}",
    )


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
    manifest = current_manifest(paths.project_dir, full, inputs, before="evals")
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
        return CommandResult(ERROR, outcome.diagnostics, {"suite": "evals", "error": outcome.reason})
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
        # The suite selects the evals within `selected`; with no selection, its own `type:eval`.
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
    """
    full, result = results
    evals = _selected_evals(result, selected)
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
                agent = agents[item.resolved.agent.key]
                scope = agent_scope(agent, profile.identity)
                if isinstance(scope, Diagnostic):
                    raise ProjectError(scope.message, diagnostics=(scope,))
                copied = _scratch_agent(port, agent, full, applied, scope, sessions)
                if isinstance(copied, Diagnostic):
                    return CommandResult(ERROR, DiagnosticBag((copied,)), {"suite": "evals"})
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
    """Print every eval as `sst test` prints a published run, then the run's verdict.

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

    On a terminal each run is a unit, labelled with its eval's key, whose status the events
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
    that replaces it, since running the suite again cannot pass. A run that passed closes with
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
