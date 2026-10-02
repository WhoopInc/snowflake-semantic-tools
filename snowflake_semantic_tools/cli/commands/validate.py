"""`sst validate`: every offline rule, and the connected checks when they are asked for."""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.fs.baseline import read_baseline
from snowflake_semantic_tools.adapters.yaml.ownership import ownership_report
from snowflake_semantic_tools.app.validate import ValidateArtifacts, ValidationResult
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.options import output_option, project_options, validation_options
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import validation_settings
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.project import connect
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity


@click.command()
@project_options()
@validation_options()
@click.option("--show-info", is_flag=True)
@output_option()
@command_body("validate")
def validate(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    show_info: bool,
    output: str,
) -> CommandResult:
    """Validate every offline rule, with optional connected checks."""
    compiled = compiling.compile_result(project_dir, target_name, manifest_path)
    effective_strict, effective_connected = validation_settings(
        project_dir,
        strict=strict,
        connected=snowflake_syntax_check,
    )
    port = None
    target = target_name or ""
    if effective_connected:
        profile, port = connect(project_dir, target_name)
        target = profile.target_name
    try:
        result = ValidateArtifacts(port, catalog=port, target=target, clock=SystemClock()).run(
            compiled,
            strict=effective_strict,
            connected=effective_connected,
            baseline=read_baseline(project_dir),
        )
    finally:
        if port is not None:
            port.close()
    exit_code = OK if result.success else ERROR
    diagnostics = (
        DiagnosticBag((*result.diagnostics, *ownership_report(project_dir))) if show_info else result.diagnostics
    )
    return CommandResult(
        exit_code,
        diagnostics,
        human=None if exit_code else lambda: _print_counts(result),
        promoted=result.promoted,
    )


def _print_counts(result: ValidationResult) -> None:
    click.echo(
        f"validated {len(result.rendered)} artifact(s): "
        f"{result.diagnostics.count(Severity.ERROR)} errors, "
        f"{result.diagnostics.count(Severity.WARNING)} warnings"
    )
