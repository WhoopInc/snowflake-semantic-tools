"""`sst test`: the offline golden suite, and the connected smoke and eval suites.

Every suite first compiles the project and stops on its errors. The connected suites
then refuse to run unless `sst compile` wrote the manifest the project compiles to now.
"""

from __future__ import annotations

import socket
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.golden import GoldenFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
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
from snowflake_semantic_tools.app.golden import CompareGoldens, GoldenReport
from snowflake_semantic_tools.app.smoke import SmokePublished
from snowflake_semantic_tools.cli.exit_codes import CONFIG, CONNECTION, ERROR, OK
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import fail_fast_option, target_option, threads_option
from snowflake_semantic_tools.cli.plan_output import print_eval_results
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import threads_setting
from snowflake_semantic_tools.cli.wiring import compile as compiling
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


@click.command(name="test")
@click.option("--suite", "suites", type=click.Choice(SUITES), multiple=True)
@target_option()
@click.option(
    "--golden-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("expected/ddl"),
)
@threads_option()
@fail_fast_option()
@click.option("--capture-baseline", "capture_baseline_requested", is_flag=True)
@click.option("--reason")
@command_body("test")
def test_command(
    paths: ProjectPaths,
    suites: tuple[str, ...],
    target_name: str | None,
    manifest_path: Path | None,
    golden_dir: Path,
    threads: int | None,
    fail_fast: bool,
    capture_baseline_requested: bool,
    reason: str | None,
) -> CommandResult:
    """Run the golden, smoke, and eval suites: those --suite names, else every one that applies.

    The golden suite always applies; the connected suites apply once `sst compile` has written
    the manifest, the smoke suite when a semantic view compiles, the eval suite when an eval
    does. Exit 1 when any suite fails, and 5 when a connected suite cannot reach Snowflake.
    `--threads` runs the smoke probes, and the evals no setting paces, that many at once.
    """
    result = compiling.compile_result(paths, target_name, manifest_path)
    if not result.success:
        return CommandResult(ERROR, result.diagnostics)
    inputs = project_inputs(paths, target_name, manifest_path)
    workers = threads_setting(paths, threads)
    chosen = suites or tuple(name for name in SUITES if _applies(name, paths, result))
    reports: list[tuple[str, CommandResult]] = []
    for name in dict.fromkeys(chosen):
        if name == "golden":
            report = _golden(paths, target_name, manifest_path, golden_dir, result, inputs)
        elif name == "evals":
            report = _run_evals(
                paths,
                target_name,
                result,
                inputs,
                EvalGateRequest(fail_fast, capture_baseline_requested, reason, threads=workers),
            )
        else:
            report = _run_smoke(paths, target_name, result, inputs, fail_fast, workers)
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
    """Compare every compiled payload with its committed golden, offline; exit 1 on any failure.

    A relative `--golden-dir` is taken from the project directory. `unstable` holds the
    SST-INT006 a second compile of the project found, each of which fails the suite too.
    """
    resolved = golden_dir if golden_dir.is_absolute() else project_dir / golden_dir
    report = CompareGoldens(GoldenFileStore(resolved), inputs.git_sha).run(result)
    return CommandResult(
        OK if report.passed and not unstable else ERROR,
        unstable,
        data={"suite": "golden", "failures": list(report.failures)},
        human=lambda: _print_golden(report, len(result.compiled)),
    )


def _print_golden(report: GoldenReport, artifact_count: int) -> None:
    if not report.passed:
        click.echo("golden suite failed:\n" + "\n\n".join(report.failures), err=True)
        return
    click.echo(f"golden suite passed for {artifact_count} artifact(s)")


def _run_smoke(
    paths: ProjectPaths,
    target_name: str | None,
    result: CompileResult,
    inputs: YamlProjectInputs,
    fail_fast: bool,
    threads: int,
) -> CommandResult:
    """Probe the published objects once SST is proven to own them; exit 1 when a check or probe fails.

    The markers are read, and the probes run, on up to `threads` sessions at once; every
    session but the first is closed before the connection is.
    """
    manifest = current_manifest(paths.project_dir, result, inputs, before="smoke")
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
    result: CompileResult,
    inputs: YamlProjectInputs,
    request: EvalGateRequest,
) -> CommandResult:
    """Run the evals against their published agents and gate them; exit 1 unless the run passes.

    Raises:
        SstUsageError: `--capture-baseline` and `--reason` are not given together.
        ProjectError: the project compiles no eval, its compiled manifest is stale, or another
            operation holds the target's lock; it carries what taking the lock reported.
    """
    if request.capture_baseline and not request.reason:
        raise SstUsageError("--capture-baseline requires --reason")
    if request.reason and not request.capture_baseline:
        raise SstUsageError("--reason requires --capture-baseline")
    evals = compiled_evals(result)
    if not evals:
        raise ProjectError("no eval artifacts matched the project")
    manifest = current_manifest(paths.project_dir, result, inputs, before="evals")
    profile, port = connect(paths, target_name)
    params = profile.connection_params
    workers = suite_concurrency(evals, inputs.eval_catalog().defaults, request.threads)
    with closed_on_error(port), ConnectorPool(workers, lambda: open_connector(params)) as pool:
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
