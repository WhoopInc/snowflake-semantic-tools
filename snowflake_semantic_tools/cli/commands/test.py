"""`sst test`: the offline golden suite, and the connected smoke and eval suites.

Every suite first compiles the project and stops on its errors. The connected suites
then refuse to run unless `sst compile` wrote the manifest the project compiles to now.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.golden import GoldenFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import output_root
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs, YamlProjectSource
from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.adapters.snowflake.eval_state import SnowflakeEvalStateStore
from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.evals.run import suite_concurrency
from snowflake_semantic_tools.app.evals.suite import (
    EvalGateOutcome,
    EvalGateRefused,
    EvalGateRequest,
    RunEvalGate,
    compiled_evals,
)
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.golden import CompareGoldens, GoldenReport, UpdateGoldens
from snowflake_semantic_tools.app.smoke import SmokePublished
from snowflake_semantic_tools.cli.exit_codes import CONFIG, CONNECTION, ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    break_stale_lock_option,
    fail_fast_option,
    selection_options,
    target_option,
    threads_option,
)
from snowflake_semantic_tools.cli.plan_output import print_eval_results
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, terminated_as_interrupt
from snowflake_semantic_tools.cli.settings import threads_setting
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.compile import selected_result
from snowflake_semantic_tools.cli.wiring.manifest import current_manifest
from snowflake_semantic_tools.cli.wiring.project import (
    closed_on_error,
    connect,
    open_connector,
    project_inputs,
    state_store,
    target_dir,
)
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, unstable_fingerprints
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName

SUITES = ("golden", "smoke", "evals")
# Which exit code wins when suites disagree: Snowflake unreachable, then a setup failure, then 1.
_EXIT_RANK = {OK: 0, ERROR: 1, CONFIG: 2, CONNECTION: 3}
# Values of `$CI` that do not mean a CI run.
_NOT_CI = frozenset({"", "0", "false", "no"})


def _refuse_invocation(suites: tuple[str, ...], update_golden: bool) -> None:
    """Refuse `--update-golden` with a suite that has no goldens, or in a CI run.

    A pipeline that rewrites its own expectations asserts nothing, so `$CI` set to anything
    but empty, `0`, `false` or `no` refuses the flag.

    Raises:
        SstUsageError: `--suite` names `smoke` or `evals` with `--update-golden`, or `$CI` is set.
    """
    if not update_golden:
        return
    others = [name for name in suites if name != "golden"]
    if others:
        raise SstUsageError(f"--update-golden rewrites golden files only; it cannot run with --suite {others[0]}")
    if os.environ.get("CI", "").strip().casefold() not in _NOT_CI:
        raise SstUsageError("--update-golden is never valid in CI; regenerate golden files locally and commit them")


@click.command(cls=SstCommand, name="test")
@click.option("--suite", "suites", type=click.Choice(SUITES), multiple=True)
@selection_options()
@target_option()
@click.option(
    "--golden-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("expected/ddl"),
)
@click.option("--update-golden", is_flag=True)
@threads_option()
@fail_fast_option()
@click.option("--capture-baseline", "capture_baseline_requested", is_flag=True)
@click.option("--reason")
@break_stale_lock_option()
@command_body("test", refusals=_refuse_invocation)
def test_command(
    paths: ProjectPaths,
    suites: tuple[str, ...],
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    target_name: str | None,
    manifest_path: Path | None,
    golden_dir: Path,
    update_golden: bool,
    threads: int | None,
    fail_fast: bool,
    capture_baseline_requested: bool,
    reason: str | None,
    break_stale_lock: bool,
) -> CommandResult:
    """Run the golden, smoke, and eval suites: those --suite names, else every one that applies.

    The golden suite always applies; the connected suites apply once `sst compile` has written
    the manifest, the smoke suite when a semantic view compiles, the eval suite when an eval
    does. `--select` and `--exclude` narrow every suite to those artifacts. Exit 1 when any
    suite fails, 4 when a selected artifact has no golden file, and 5 when a connected suite
    cannot reach Snowflake. `--threads` runs the smoke probes, and the evals no setting paces,
    that many at once. `--update-golden` runs the golden suite only, rewriting each golden the
    current output no longer equals. The eval suite holds the target's run lock while it runs;
    `--break-stale-lock` takes it over only from a run that has expired.
    """
    compiled = compiling.compile_result(paths, target_name, manifest_path)
    if not compiled.success:
        return CommandResult(ERROR, compiled.diagnostics)
    result = selected_result(paths.project_dir, compiled, selected, excluded) if selected or excluded else compiled
    inputs = project_inputs(paths, target_name, manifest_path)
    workers = threads_setting(paths, threads)
    if update_golden:
        return _update_golden(paths.project_dir, golden_dir, result, inputs)
    chosen = suites or tuple(name for name in SUITES if _applies(name, paths, result))
    reports: list[tuple[str, CommandResult]] = []
    for name in dict.fromkeys(chosen):
        if name == "golden":
            report = _golden(paths, target_name, manifest_path, golden_dir, result, inputs)
        elif name == "evals":
            report = _run_evals(
                paths,
                target_name,
                (compiled, result),
                inputs,
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
        return reports[0][1]
    return _combined(reports, skipped=tuple(name for name in SUITES if name not in chosen))


def _applies(suite: str, paths: ProjectPaths, result: CompileResult) -> bool:
    """Report whether a suite applies to the project when --suite does not name it."""
    if suite == "golden":
        return True
    if not (target_dir(paths.project_dir) / "manifest.json").is_file():
        return False
    if suite == "evals":
        return bool(compiled_evals(result))
    return any(isinstance(item, CompiledView) for item in result.compiled)


def _golden(
    paths: ProjectPaths,
    target_name: str | None,
    manifest_path: Path | None,
    golden_dir: Path,
    result: CompileResult,
    inputs: YamlProjectInputs,
) -> CommandResult:
    # The same project compiled again from the manifest just read, so dbt is not run twice.
    manifest = manifest_path
    if manifest is None and (paths.project_dir / "dbt_project.yml").is_file():
        manifest = YamlProjectSource(paths).manifest_file()
    again = compiling.compile_result(paths, target_name, manifest)
    return _run_golden(
        paths.project_dir, golden_dir, result, inputs, unstable_fingerprints(result.diagnostics, again.diagnostics)
    )


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

    def human() -> None:
        for _, report in reports:
            if report.human is not None:
                report.human()
        click.echo(f"{len(reports) - len(failed)} suite(s) passed, {len(failed)} failed, {len(skipped)} skipped")

    diagnostics = DiagnosticBag(tuple(item for _, report in reports for item in report.diagnostics))
    return CommandResult(worst, diagnostics, data, human=human)


def _run_golden(
    project_dir: Path,
    golden_dir: Path,
    result: CompileResult,
    inputs: YamlProjectInputs,
    unstable: DiagnosticBag,
) -> CommandResult:
    """Compare every compiled payload with its committed golden, offline.

    Exit 1 on a mismatch, and 4 when a golden does not exist, which `--update-golden` creates.
    A relative `--golden-dir` is taken from the project directory. `unstable` holds the
    SST-INT006 a second compile of the project found, each of which fails the suite too.
    """
    report = CompareGoldens(GoldenFileStore(_golden_root(project_dir, golden_dir)), inputs.git_sha).run(result)
    exit_code = CONFIG if report.missing else OK if report.passed and not unstable else ERROR
    return CommandResult(
        exit_code,
        unstable,
        data={"suite": "golden", "failures": list(report.failures), "missing": list(report.missing)},
        human=lambda: _print_golden(report, len(result.compiled)),
    )


def _golden_root(project_dir: Path, golden_dir: Path) -> Path:
    """Return `--golden-dir`, a relative one taken from the project directory."""
    return golden_dir if golden_dir.is_absolute() else project_dir / golden_dir


def _update_golden(
    project_dir: Path, golden_dir: Path, result: CompileResult, inputs: YamlProjectInputs
) -> CommandResult:
    """Rewrite each golden the selected artifacts' output no longer equals; exit 0 once written.

    Every golden is written inside the project, or inside the golden directory's parent when
    `--golden-dir` lies outside it.
    """
    golden_root = _golden_root(project_dir, golden_dir)
    store = GoldenFileStore(golden_root, root=output_root(project_dir, golden_root.parent))
    written = UpdateGoldens(store, inputs.git_sha).run(result)

    def human() -> None:
        for name in written:
            click.echo(f"wrote {name}")
        click.echo(f"{len(written)} golden file(s) written for {len(result.compiled)} artifact(s)")

    return CommandResult(OK, data={"suite": "golden", "written": list(written)}, human=human)


def _print_golden(report: GoldenReport, artifact_count: int) -> None:
    if not report.passed:
        click.echo("golden suite failed:\n" + "\n\n".join(report.failures), err=True)
        return
    click.echo(f"golden suite passed for {artifact_count} artifact(s)")


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
            smoke = SmokePublished(port, state_store(paths, profile.target_name), Fanout(port, pool, threads)).run(
                result,
                manifest,
                target=profile.identity,
                state_table=profile.state_table,
                fail_fast=fail_fast,
            )
    finally:
        port.close()
    exit_code = OK if smoke.success else ERROR
    attempted = len(smoke.attempted)
    return CommandResult(
        exit_code,
        smoke.diagnostics,
        data={"suite": "smoke", "attempted": attempted},
        human=None if exit_code else lambda: click.echo(f"smoke suite passed: {attempted} probe(s)"),
    )


def _run_evals(
    paths: ProjectPaths,
    target_name: str | None,
    results: tuple[CompileResult, CompileResult],
    inputs: YamlProjectInputs,
    request: EvalGateRequest,
) -> CommandResult:
    """Run the selected evals against their published agents and gate them; exit 1 unless the run passes.

    `results` is the whole compile, which the compiled manifest must match, and the selection.
    SIGTERM interrupts the run as Ctrl-C does, so the run lock is released either way.

    Raises:
        SstUsageError: `--capture-baseline` and `--reason` are not given together.
        ProjectError: the selection compiles no eval, the compiled manifest is stale, or another
            operation holds the target's lock; it carries what taking the lock reported.
    """
    if request.capture_baseline and not request.reason:
        raise SstUsageError("--capture-baseline requires --reason")
    if request.reason and not request.capture_baseline:
        raise SstUsageError("--reason requires --capture-baseline")
    full, result = results
    evals = compiled_evals(result)
    if not evals:
        raise ProjectError("no eval artifacts matched the project")
    manifest = current_manifest(paths.project_dir, full, inputs, before="evals")
    profile, port = connect(paths, target_name)
    params = profile.connection_params
    workers = suite_concurrency(evals, inputs.eval_catalog().defaults, request.threads)
    with (
        terminated_as_interrupt(),
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
        ).run(evals, manifest, request, target=profile.identity, state_table=profile.state_table)
    port.close()
    if isinstance(outcome, EvalGateRefused):
        raise ProjectError(outcome.reason, diagnostics=tuple(outcome.diagnostics))
    return _eval_report(outcome)


def _eval_state_table(state_table: QualifiedName) -> QualifiedName:
    """Name the table eval baselines and gates are kept in: beside the state table, suffixed `_EVALS`."""
    return QualifiedName(state_table.database, state_table.schema, Identifier.parse(f"{state_table.name.folded}_EVALS"))


def _eval_report(outcome: EvalGateOutcome) -> CommandResult:
    """Report an eval run, its diagnostics and then every attempt; exit 1 unless it passed."""
    suite = outcome.suite
    return CommandResult(
        OK if outcome.passed else ERROR,
        outcome.diagnostics,
        dict(outcome.data),
        human=None if suite is None else lambda: print_eval_results(suite),
    )
