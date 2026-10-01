"""`sst test`: the offline golden suite, and the connected smoke and eval suites.

Every suite first compiles the project and stops on its errors. The connected suites
then refuse to run unless `sst compile` wrote the manifest the project compiles to now.
"""

from __future__ import annotations

from pathlib import Path

import click

from ...adapters.clock import SystemClock
from ...adapters.errors import ProjectError
from ...adapters.fs.golden import GoldenFileStore
from ...adapters.project_source import YamlProjectInputs
from ...adapters.snowflake.eval_state import SnowflakeEvalStateStore
from ...app.compile import CompileResult
from ...app.evals.suite import EvalGateOutcome, EvalGateRefused, EvalGateRequest, RunEvalGate, compiled_evals
from ...app.golden import CompareGoldens, GoldenReport
from ...app.smoke import SmokePublished
from ...domain.model.identifier import Identifier, QualifiedName
from ..exit_codes import ERROR, OK
from ..group import SstUsageError
from ..options import fail_fast_option, manifest_option, output_option, project_dir_option, target_option
from ..plan_output import print_eval_results
from ..runner import CommandResult, command_body
from ..wiring import compile as compiling
from ..wiring.manifest import current_manifest
from ..wiring.project import closed_on_error, connect, project_inputs, state_store


@click.command(name="test")
@project_dir_option()
@click.option("--suite", type=click.Choice(["golden", "smoke", "evals"]), required=True)
@target_option()
@manifest_option()
@click.option(
    "--golden-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("expected/ddl"),
)
@fail_fast_option()
@click.option("--capture-baseline", "capture_baseline_requested", is_flag=True)
@click.option("--reason")
@output_option()
@command_body("test")
def test_command(
    project_dir: Path,
    suite: str,
    target_name: str | None,
    manifest_path: Path | None,
    golden_dir: Path,
    fail_fast: bool,
    capture_baseline_requested: bool,
    reason: str | None,
    output: str,
) -> CommandResult:
    """Run exact offline goldens or separate connected smoke probes."""
    result = compiling.compile_result(project_dir, target_name, manifest_path)
    if not result.success:
        return CommandResult(ERROR, result.diagnostics)
    inputs = project_inputs(project_dir, target_name, manifest_path)
    if suite == "golden":
        return _run_golden(project_dir, golden_dir, result, inputs)
    if suite == "evals":
        request = EvalGateRequest(fail_fast, capture_baseline_requested, reason)
        return _run_evals(project_dir, target_name, result, inputs, request)
    return _run_smoke(project_dir, target_name, result, inputs, fail_fast)


def _run_golden(project_dir: Path, golden_dir: Path, result: CompileResult, inputs: YamlProjectInputs) -> CommandResult:
    """Compare every compiled payload with its committed golden, offline; exit 1 on any failure.

    A relative `--golden-dir` is taken from the project directory.
    """
    resolved = golden_dir if golden_dir.is_absolute() else project_dir / golden_dir
    report = CompareGoldens(GoldenFileStore(resolved), inputs.git_sha).run(result)
    return CommandResult(
        OK if report.passed else ERROR,
        data={"suite": "golden", "failures": list(report.failures)},
        human=lambda: _print_golden(report, len(result.compiled)),
    )


def _print_golden(report: GoldenReport, artifact_count: int) -> None:
    if not report.passed:
        click.echo("golden suite failed:\n" + "\n\n".join(report.failures), err=True)
        return
    click.echo(f"golden suite passed for {artifact_count} artifact(s)")


def _run_smoke(
    project_dir: Path,
    target_name: str | None,
    result: CompileResult,
    inputs: YamlProjectInputs,
    fail_fast: bool,
) -> CommandResult:
    """Probe the published objects once SST is proven to own them; exit 1 when a check or probe fails."""
    manifest = current_manifest(project_dir, result, inputs, before="smoke")
    profile, port = connect(project_dir, target_name)
    try:
        smoke = SmokePublished(port, state_store(project_dir, profile.target_name)).run(
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
    project_dir: Path,
    target_name: str | None,
    result: CompileResult,
    inputs: YamlProjectInputs,
    request: EvalGateRequest,
) -> CommandResult:
    """Run the evals against their published agents and gate them; exit 1 unless the run passes.

    Raises:
        SstUsageError: `--capture-baseline` and `--reason` are not given together.
        ProjectError: the project compiles no eval, its compiled manifest is stale, or another
            operation holds the target's state lock.
    """
    if request.capture_baseline and not request.reason:
        raise SstUsageError("--capture-baseline requires --reason")
    if request.reason and not request.capture_baseline:
        raise SstUsageError("--reason requires --capture-baseline")
    evals = compiled_evals(result)
    if not evals:
        raise ProjectError("no eval artifacts matched the project")
    manifest = current_manifest(project_dir, result, inputs, before="evals")
    profile, port = connect(project_dir, target_name)
    with closed_on_error(port):
        store = state_store(project_dir, profile.target_name)
        eval_store = SnowflakeEvalStateStore(port, _eval_state_table(profile.state_table))
        outcome = RunEvalGate(port, inputs, store, eval_store, SystemClock()).run(
            evals, manifest, request, target=profile.identity, state_table=profile.state_table
        )
    port.close()
    if isinstance(outcome, EvalGateRefused):
        raise ProjectError(outcome.reason)
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
