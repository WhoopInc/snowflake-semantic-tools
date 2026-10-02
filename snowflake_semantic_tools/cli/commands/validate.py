"""`sst validate`: every offline rule, and the connected checks when they are asked for."""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.ownership import ownership_report
from snowflake_semantic_tools.app.validate import ValidateArtifacts, ValidationResult
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions
from snowflake_semantic_tools.cli.options import (
    database_option,
    model_path_options,
    selection_options,
    target_option,
    validation_options,
    with_model_paths,
)
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import strict_disagreement, validation_settings
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.project import connect
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity


@click.command()
@target_option()
@selection_options()
@database_option()
@validation_options()
@model_path_options()
@click.option("--verify-schema", is_flag=True)
@command_body("validate")
def validate(
    paths: ProjectPaths,
    options: GlobalOptions,
    target_name: str | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    database: str | None,
    manifest_path: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    dbt_dir: Path | None,
    semantic_dir: Path | None,
    verify_schema: bool,
) -> CommandResult:
    """Validate every offline rule, with optional connected checks.

    Exit 0 with no errors, and 1 with errors, or warnings under --strict. With
    --verify-schema, the columns each semantic view reads are looked up in the warehouse.
    """
    paths = with_model_paths(paths, dbt_dir, semantic_dir)
    compiled = compiling.compile_result(paths, target_name, manifest_path, database=database)
    if selected or excluded:
        compiled = compiling.selected_result(paths.project_dir, compiled, selected, excluded)
    effective_strict, effective_connected = validation_settings(
        paths,
        strict=strict,
        connected=snowflake_syntax_check,
    )
    port = None
    target = target_name or ""
    if effective_connected or verify_schema:
        profile, port = connect(paths, target_name)
        target = profile.target_name
    try:
        result = ValidateArtifacts(port, catalog=port, target=target, clock=SystemClock()).run(
            compiled,
            strict=effective_strict,
            connected=effective_connected,
            verify_schema=verify_schema,
        )
    finally:
        if port is not None:
            port.close()
    exit_code = OK if result.success else ERROR
    # `--show-info` also says which registered type owns each semantic-model file.
    owners = ownership_report(paths) if options.show_info else ()
    return CommandResult(
        exit_code,
        DiagnosticBag((*strict_disagreement(paths, strict), *result.diagnostics, *owners)),
        human=None if exit_code else lambda: _print_counts(result),
        promoted=result.promoted,
        gated=True,
    )


def _print_counts(result: ValidationResult) -> None:
    click.echo(
        f"validated {len(result.rendered)} artifact(s): "
        f"{result.diagnostics.count(Severity.ERROR)} errors, "
        f"{result.diagnostics.count(Severity.WARNING)} warnings"
    )
