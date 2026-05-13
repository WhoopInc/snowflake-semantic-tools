"""
Diff Command

CLI command for previewing semantic view changes before deployment.
Compares proposed DDL (from manifest) against currently deployed views.
"""

import json as json_mod
import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.options import database_schema_options, target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import (
    build_snowflake_config,
    get_target_database_schema,
    setup_command,
)
from snowflake_semantic_tools.services.diff_service import DiffConfig, DiffService


@click.command(
    short_help="Preview semantic view changes before deployment",
)
@target_option
@database_schema_options
@click.option("--full", is_flag=True, help="Show complete SQL diff for changed views")
@click.option("--names-only", is_flag=True, help="Output only changed view names (for scripting)")
@click.option("--view", "-v", "view_filter", multiple=True, help="Diff specific view(s) only")
@click.option("--output", "-o", "output_file", help="Save diff to file")
@click.option(
    "--format",
    "-f",
    "output_format_flag",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format",
)
@click.option("--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def diff_cmd(ctx, dbt_target, db, schema, full, names_only, view_filter, output_file, output_format_flag, verbose):
    """Preview semantic view changes before deployment.

    Compares proposed DDL (from sst_manifest.json) against views currently
    deployed in Snowflake. Like 'terraform plan' for semantic views.

    \b
    Examples:
      sst diff                                Summary of changes
      sst diff --full                         Full SQL diff for changed views
      sst diff --names-only                   Just changed view names
      sst diff -v customer_360                Diff a specific view
      sst diff --format json                  Machine-readable output
      sst diff --full -o diff_report.md       Save diff to file

    \b
    Prerequisites:
      - Run 'sst compile' first (generates sst_manifest.json)
      - Snowflake credentials in ~/.dbt/profiles.yml

    \b
    Related Commands:
      sst compile     Compile metadata into manifest
      sst generate    Deploy semantic views to Snowflake
      sst deploy      Full deployment workflow
    """
    global_format = ctx.obj.get("output_format", "table") if ctx.obj else "table"
    quiet_mode = global_format == "json"
    output = CLIOutput(verbose=verbose, quiet=quiet_mode)
    output.info(f"Running with sst={__version__}")

    setup_command(verbose=verbose, quiet=quiet_mode, validate_config=True)

    target_db, target_schema = get_target_database_schema(
        dbt_target=dbt_target,
        db_override=db,
        schema_override=schema,
    )

    try:
        snowflake_config = build_snowflake_config(
            target=dbt_target,
            database=target_db,
            schema=target_schema,
            verbose=verbose,
        )
    except Exception as e:
        output.error(f"Failed to configure Snowflake: {e}")
        if verbose:
            traceback.print_exc()
        raise click.Abort()

    output.blank_line()
    output.info(f"Comparing: {target_db}.{target_schema}")
    output.info("Generating proposed DDL from manifest...")

    config = DiffConfig(
        database=target_db,
        schema=target_schema,
        views_filter=list(view_filter) if view_filter else None,
    )

    start = time.time()
    service = DiffService(snowflake_config)
    result = service.diff(config)
    duration = time.time() - start

    if not result.success:
        for err in result.errors:
            output.error(err)
        raise click.ClickException("Diff failed — see errors above")

    if output_format_flag == "json":
        _output_json(result, output_file)
        return

    if names_only:
        _output_names_only(result, output_file)
        return

    if full:
        _output_full(result, output, output_file, duration)
        return

    _output_summary(result, output, output_file, duration)


def _output_json(result, output_file):
    data = {
        "summary": {
            "new": len(result.new),
            "modified": len(result.modified),
            "unchanged": len(result.unchanged),
            "extra_deployed": len(result.extra_deployed),
        },
        "new": [v.name for v in result.new],
        "modified": [v.name for v in result.modified],
        "unchanged": [v.name for v in result.unchanged],
        "extra_deployed": result.extra_deployed,
        "errors": result.errors,
        "warnings": result.warnings,
    }
    text = json_mod.dumps(data, indent=2)
    if output_file:
        Path(output_file).write_text(text)
    else:
        click.echo(text)


def _output_names_only(result, output_file):
    names = [v.name for v in result.new + result.modified]
    text = "\n".join(names)
    if output_file:
        Path(output_file).write_text(text)
    else:
        click.echo(text)


def _output_full(result, output, output_file, duration):
    lines = []

    if result.new:
        lines.append(f"New views ({len(result.new)}):")
        for v in result.new:
            lines.append(f"  + {v.name}")
        lines.append("")

    if result.modified:
        lines.append(f"Modified views ({len(result.modified)}):")
        for v in result.modified:
            lines.append(f"  ~ {v.name}")
            if v.unified_diff:
                for dl in v.unified_diff.splitlines():
                    lines.append(f"    {dl}")
            lines.append("")

    if result.unchanged:
        lines.append(f"Unchanged views ({len(result.unchanged)}):")
        for v in result.unchanged:
            lines.append(f"    {v.name}")
        lines.append("")

    if result.extra_deployed:
        lines.append(f"Extra deployed (not in manifest):")
        for name in result.extra_deployed:
            lines.append(f"  ? {name}")
        lines.append("")

    for w in result.warnings:
        lines.append(f"WARNING: {w}")

    lines.append(f"Done in {duration:.1f}s")

    text = "\n".join(lines)
    if output_file:
        Path(output_file).write_text(text)
        output.success(f"Diff written to {output_file}")
    else:
        click.echo(text)


def _output_summary(result, output, output_file, duration):
    output.blank_line()
    output.header("Semantic View Diff")
    output.blank_line()

    total = len(result.new) + len(result.modified) + len(result.unchanged)
    click.echo(f"  New:        {len(result.new)} view(s)")
    click.echo(f"  Modified:   {len(result.modified)} view(s)")
    click.echo(f"  Unchanged:  {len(result.unchanged)} view(s)")

    if result.extra_deployed:
        click.echo(f"  Extra:      {len(result.extra_deployed)} view(s) in Snowflake but not in manifest")

    if result.new:
        output.blank_line()
        click.echo("  New:")
        for v in result.new:
            click.echo(f"    + {v.name}")

    if result.modified:
        output.blank_line()
        click.echo("  Modified:")
        for v in result.modified:
            click.echo(f"    ~ {v.name}")

    if result.extra_deployed:
        output.blank_line()
        click.echo("  Extra deployed:")
        for name in result.extra_deployed:
            click.echo(f"    ? {name}")

    for w in result.warnings:
        output.warning(w)

    output.blank_line()

    if result.has_changes:
        click.echo("  Use --full to see complete SQL diff")
        click.echo("  To deploy: sst deploy")
    else:
        click.echo("  No changes detected — Snowflake is up to date")

    output.blank_line()
    output.success("Done", duration=duration)
