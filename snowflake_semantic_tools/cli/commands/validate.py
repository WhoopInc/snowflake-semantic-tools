"""`sst validate`: every offline rule, and the connected checks when they are asked for."""

from __future__ import annotations

from pathlib import Path

import click

from ...app.validate import ValidateArtifacts, ValidationResult
from ...domain.model.diagnostic import Severity
from ..exit_codes import ERROR, OK
from ..options import output_option, project_options, validation_options
from ..runner import CommandResult, command_body
from ..settings import validation_settings
from ..wiring import compile as compiling
from ..wiring.project import connect


@click.command()
@project_options()
@validation_options()
@output_option()
@command_body("validate")
def validate(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
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
    if effective_connected:
        _, port = connect(project_dir, target_name)
    try:
        result = ValidateArtifacts(port).run(
            compiled,
            strict=effective_strict,
            connected=effective_connected,
        )
    finally:
        if port is not None:
            port.close()
    exit_code = OK if result.success else ERROR
    return CommandResult(
        exit_code,
        result.diagnostics,
        human=None if exit_code else lambda: _print_counts(result),
        promoted=result.promoted,
    )


def _print_counts(result: ValidationResult) -> None:
    click.echo(
        f"validated {len(result.rendered)} artifact(s): "
        f"{result.diagnostics.count(Severity.ERROR)} errors, "
        f"{result.diagnostics.count(Severity.WARNING)} warnings"
    )
