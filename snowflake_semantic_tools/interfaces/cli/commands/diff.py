"""
Diff Command

Preview semantic view changes before deployment. Shows only what changed:
new, removed, and modified components (metrics, dimensions, relationships, etc).
"""

import json
import time
import traceback
from pathlib import Path
from typing import List

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.options import database_schema_options, target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import (
    build_snowflake_config,
    get_target_database_schema,
    setup_command,
)
from snowflake_semantic_tools.services.diff_service import ComponentChange, DiffConfig, DiffService, ViewDiff

_KIND_LABELS = {
    "TABLE": "Tables",
    "METRIC": "Metrics",
    "DIMENSION": "Dimensions",
    "FACT": "Facts",
    "RELATIONSHIP": "Relationships",
    "AI_VERIFIED_QUERY": "Verified Queries",
    "CUSTOM_INSTRUCTION": "Custom Instructions",
}

_STATUS_ICONS = {"new": "+", "removed": "-", "modified": "~"}


@click.command(
    short_help="Preview semantic view changes before deployment",
)
@target_option
@database_schema_options
@click.option("--full", is_flag=True, help="Show property-level details for modifications")
@click.option("--names-only", is_flag=True, help="Output only changed view names (for scripting)")
@click.option("--view", "-v", "view_filter", multiple=True, help="Diff specific view(s) only")
@click.option(
    "--format", "-f", "output_format_flag", type=click.Choice(["text", "json"]), default="text", help="Output format"
)
@click.option("--output", "-o", "output_file", help="Save diff to file")
@click.option("--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def diff_cmd(ctx, dbt_target, db, schema, full, names_only, view_filter, output_format_flag, output_file, verbose):
    """Preview semantic view changes before deployment.

    Compares proposed components (from sst_manifest.json) against views
    currently deployed in Snowflake. Only shows what changed.

    \b
    Examples:
      sst diff                                Component-level changes
      sst diff --full                         Property-level details
      sst diff --names-only                   Just changed view names
      sst diff -v customer_360                Diff a specific view
      sst diff --format json                  Machine-readable output

    \b
    Prerequisites:
      - Run 'sst compile' first
      - Snowflake credentials in ~/.dbt/profiles.yml

    \b
    Related Commands:
      sst compile     Compile metadata into manifest
      sst deploy      Deploy semantic views to Snowflake
    """
    global_format = ctx.obj.get("output_format", "table") if ctx.obj else "table"
    quiet_mode = global_format == "json"
    output = CLIOutput(verbose=verbose, quiet=quiet_mode)
    output.info(f"Running with sst={__version__}")

    setup_command(verbose=verbose, quiet=quiet_mode, validate_config=True)

    target_db, target_schema = get_target_database_schema(dbt_target=dbt_target, db_override=db, schema_override=schema)

    try:
        snowflake_config = build_snowflake_config(
            target=dbt_target, database=target_db, schema=target_schema, verbose=verbose
        )
    except Exception as e:
        output.error(f"Failed to configure Snowflake: {e}")
        if verbose:
            traceback.print_exc()
        raise click.Abort()

    config = DiffConfig(
        database=target_db,
        schema=target_schema,
        views_filter=list(view_filter) if view_filter else None,
    )

    output.blank_line()
    output.info(f"Comparing: {target_db}.{target_schema}")

    start = time.time()
    service = DiffService(snowflake_config)
    result = service.diff(config)
    duration = time.time() - start

    if not result.success:
        for err in result.errors:
            output.error(err)
        raise click.ClickException("Diff failed")

    if output_format_flag == "json":
        _output_json(result, output_file)
        return

    if names_only:
        _output_names_only(result, output_file)
        return

    _output_text(result, output, output_file, duration, full)


def _write_file(path: str, text: str):
    try:
        Path(path).write_text(text, encoding="utf-8")
    except OSError as e:
        raise click.ClickException(f"Could not write to {path}: {e}")


def _output_json(result, output_file):
    new_count = sum(1 for v in result.views if v.status == "new")
    removed_count = sum(1 for v in result.views if v.status == "removed")
    data = {
        "views": [],
        "summary": {
            "changed": result.changed_count,
            "unchanged": result.unchanged_count,
            "new": new_count,
            "removed": removed_count,
        },
    }
    if result.warnings:
        data["warnings"] = result.warnings
    for v in result.views:
        view_data = {"name": v.name, "status": v.status}
        if v.changes:
            view_data["changes"] = [
                {
                    k: v
                    for k, v in {
                        "kind": c.kind,
                        "name": c.name,
                        "table": c.table or None,
                        "status": c.status,
                        "detail": c.detail,
                        "old_value": c.old_value,
                        "new_value": c.new_value,
                    }.items()
                    if v is not None
                }
                for c in v.changes
            ]
        if v.proposed_counts:
            view_data["proposed_counts"] = v.proposed_counts
        data["views"].append(view_data)
    text = json.dumps(data, indent=2)
    if output_file:
        _write_file(output_file, text)
    else:
        click.echo(text)


def _output_names_only(result, output_file):
    names = [v.name for v in result.views if v.has_changes]
    text = "\n".join(names)
    if output_file:
        _write_file(output_file, text)
    else:
        click.echo(text)


def _output_text(result, output, output_file, duration, full):
    lines = []
    use_color = not output_file

    def _style(text, **kwargs):
        return click.style(text, **kwargs) if use_color else text

    for v in result.views:
        if v.status == "unchanged":
            continue

        if v.status == "removed":
            lines.append(_style(f"{v.name}", bold=True) + _style(": removed from manifest", fg="red"))
            lines.append("")
            continue

        if v.status == "new":
            counts = ", ".join(
                f"{n} {_KIND_LABELS.get(k, k).lower()}" for k, n in sorted(v.proposed_counts.items()) if n
            )
            lines.append(_style(f"{v.name}", bold=True) + _style(": new view ", fg="green") + f"({counts})")
            lines.append("")
        else:
            lines.append(_style(f"{v.name}:", bold=True))
            by_kind = {}
            for c in v.changes:
                by_kind.setdefault(c.kind, []).append(c)

            for kind in (
                "TABLE",
                "METRIC",
                "DIMENSION",
                "FACT",
                "RELATIONSHIP",
                "AI_VERIFIED_QUERY",
                "CUSTOM_INSTRUCTION",
            ):
                if kind not in by_kind:
                    continue
                label = _KIND_LABELS.get(kind, kind)
                lines.append(_style(f"  {label}:", dim=True))
                for c in by_kind[kind]:
                    icon = _STATUS_ICONS.get(c.status, "?")
                    short_name = c.name.split(".")[-1] if "." in c.name else c.name
                    detail = f"  {c.detail}" if c.detail else ""
                    color = {"new": "green", "removed": "red", "modified": "yellow"}.get(c.status)
                    lines.append(_style(f"    {icon} {short_name}{detail}", fg=color))
                    if full and c.status == "modified" and c.old_value and c.new_value:
                        old_text = c.old_value[:100] + ("..." if len(c.old_value) > 100 else "")
                        new_text = c.new_value[:100] + ("..." if len(c.new_value) > 100 else "")
                        lines.append(_style(f"        - {old_text}", fg="red"))
                        lines.append(_style(f"        + {new_text}", fg="green"))
            lines.append("")

    summary_parts = []
    if result.changed_count:
        summary_parts.append(_style(f"{result.changed_count} changed", fg="yellow"))
    if result.unchanged_count:
        summary_parts.append(_style(f"{result.unchanged_count} unchanged", dim=True))
    new_count = sum(1 for v in result.views if v.status == "new")
    if new_count:
        summary_parts.append(_style(f"{new_count} new", fg="green"))
    removed_count = sum(1 for v in result.views if v.status == "removed")
    if removed_count:
        summary_parts.append(_style(f"{removed_count} removed", fg="red"))
    lines.append(", ".join(summary_parts))

    text = "\n".join(lines)
    if output_file:
        _write_file(output_file, text)
        output.success(f"Diff written to {output_file}")
    else:
        output.blank_line()
        click.echo(text)
        output.blank_line()
        output.success("Done", duration=duration)
