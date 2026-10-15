"""`sst validate`: every offline rule, and the connected checks when they are asked for."""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.adapters.yaml.ownership import ownership_report
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.policy import strict_reach
from snowflake_semantic_tools.app.validate import ValidateArtifacts, ValidationResult
from snowflake_semantic_tools.app.validate_models import ModelCheck, configured_models
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.options import (
    database_option,
    defer_target_option,
    model_path_options,
    selection_options,
    target_option,
    threads_option,
    validation_options,
    with_model_paths,
)
from snowflake_semantic_tools.cli.output import counted, reported_diagnostics, summary_counts, suppressed_by_baseline
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import (
    project_config,
    severity_overrides_setting,
    strict_disagreement,
    threads_setting,
    validation_settings,
)
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.project import connect, open_connector
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.plan.selectors import SelectionScope


@click.command(cls=SstCommand)
@target_option()
@defer_target_option()
@selection_options()
@database_option()
@validation_options()
@model_path_options()
@click.option("--verify-schema", is_flag=True)
@threads_option()
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
    threads: int | None,
) -> CommandResult:
    """Validate every offline rule, with optional connected checks.

    Exit 0 with no errors, and 1 with errors, or warnings under --strict; a warning the
    baseline holds never blocks, and an expired baseline fails the run. The Snowflake syntax
    check connects, and runs unless --no-snowflake-syntax-check or
    `validation.snowflake_syntax_check: false` turns it off. With --verify-schema, the
    columns each semantic view reads are looked up in the warehouse. The syntax check also runs
    each model `snowflake.orchestration_models` allows in a throwaway agent. With more than one
    thread, the connected checks run on up to that many sessions at once, with the same
    findings.
    """
    paths = with_model_paths(paths, dbt_dir, semantic_dir)
    compiled = compiling.compile_result(paths, target_name, manifest_path, database=database)
    scope = SelectionScope()
    if selected or excluded:
        compiled, scope = compiling.selected_within(paths.project_dir, compiled, selected, excluded)
    effective_strict, effective_connected = validation_settings(
        paths,
        strict=strict,
        connected=snowflake_syntax_check,
    )
    port = None
    params: dict[str, object] = {}
    target = target_name or ""
    workers = threads_setting(paths, threads)
    if effective_connected or verify_schema:
        profile, port = connect(paths, target_name)
        target = profile.target_name
        params = profile.connection_params
    try:
        with ConnectorPool(workers, lambda: open_connector(params)) as pool, RUN_TIMINGS.phase("validate"):
            readers = Fanout(port, pool, workers) if port is not None else None
            models = ModelCheck(configured_models(project_config(paths)), readers) if readers is not None else None
            result = ValidateArtifacts(
                port, catalog=port, target=target, clock=SystemClock(), readers=readers, models=models
            ).run(
                compiled,
                strict=effective_strict,
                connected=effective_connected,
                verify_schema=verify_schema,
                overrides=severity_overrides_setting(paths),
                reaches=strict_reach(scope),
            )
    finally:
        if port is not None:
            port.close()
    exit_code = OK if result.success else ERROR
    # `--show-info` also says which registered type owns each semantic-model file.
    owners = ownership_report(paths, project_config(paths)) if options.show_info else ()
    diagnostics = DiagnosticBag((*strict_disagreement(paths, strict), *result.diagnostics, *owners))
    return CommandResult(
        exit_code,
        diagnostics,
        # Built when the envelope is, once the runner has matched the baseline.
        data=lambda: {
            "rules_run": result.rules_run,
            "artifacts_checked": result.artifacts_checked,
            "suppressed_by_baseline": suppressed_by_baseline(diagnostics),
        },
        human=lambda: _print_counts(result, diagnostics),
        promoted=result.promoted,
        gated=True,
    )


def _print_counts(result: ValidationResult, diagnostics: DiagnosticBag) -> None:
    """Print what was validated, once the baseline is matched, unless an error it does not hold remains.

    The counts are those of the diagnostics the run reports, so they match the summary line
    above them; a diagnostic the baseline holds is counted apart, so a run it lets pass
    reports no errors.
    """
    counts = summary_counts(reported_diagnostics(diagnostics))
    if counts["error"]:
        return
    held = f" ({counts['baselined']} baselined)" if counts["baselined"] else ""
    click.echo(
        f"validated {len(result.rendered)} artifact(s): "
        f"{counted(counts['error'], 'error')}, {counted(counts['warning'], 'warning')}{held}"
    )
