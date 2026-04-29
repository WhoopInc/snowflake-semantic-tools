"""
List Command

CLI command for listing and exploring semantic model components.
Provides subcommands for each component type with filtering and output format options.
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.formatters import (
    format_csv_output,
    format_json_output,
    format_table,
    format_yaml_output,
    write_output,
)
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.services.list_semantic_components import (
    ListConfig,
    ListResult,
    SemanticComponentListService,
    _parse_tables_json,
)
from snowflake_semantic_tools.shared.events import setup_events


FORMAT_OPTION = click.option(
    "--format", "-f", "output_format", type=click.Choice(["table", "json", "yaml", "csv"]), default="table",
    help="Output format (default: table)"
)
OUTPUT_OPTION = click.option("--output", "-o", "output_file", help="Write output to file instead of stdout")
VERBOSE_OPTION = click.option("--verbose", "-v", is_flag=True, help="Show additional details")
QUIET_OPTION = click.option("--quiet", "-q", is_flag=True, help="Suppress all output except data")
DBT_OPTION = click.option(
    "--dbt", "dbt_path", help="dbt models path (auto-detected from config if not specified)"
)
SEMANTIC_OPTION = click.option(
    "--semantic", "semantic_path", help="Semantic models path (auto-detected from config if not specified)"
)
TABLE_FILTER_OPTION = click.option("--table", "table_filter", help="Filter by table name (substring match)")


def _setup(verbose: bool, quiet: bool):
    """Minimal CLI setup for list command (no config validation required)."""
    setup_events(verbose=verbose, quiet=quiet, show_timestamps=False)
    if verbose:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.DEBUG)
    elif quiet:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.ERROR)


def _build_config(dbt_path: Optional[str], semantic_path: Optional[str], table_filter: Optional[str]) -> ListConfig:
    return ListConfig(
        dbt_path=Path(dbt_path) if dbt_path else None,
        semantic_path=Path(semantic_path) if semantic_path else None,
        table_filter=table_filter,
    )


def _run_list(config: ListConfig) -> ListResult:
    service = SemanticComponentListService()
    return service.execute(config)


def _show_errors(result: ListResult, output: CLIOutput):
    """Display any errors from the list result."""
    for err in result.errors:
        output.warning(err)


def _relativize_path(path_str: str) -> str:
    """Convert absolute paths to relative for cleaner output."""
    try:
        return str(Path(path_str).relative_to(Path.cwd()))
    except (ValueError, TypeError):
        return path_str


@click.group("list", invoke_without_command=True)
@click.pass_context
def list_cmd(ctx):
    """
    List semantic model components.

    Explore metrics, relationships, filters, semantic views, tables,
    custom instructions, and verified queries defined in your project.

    \b
    Examples:
      sst list                    # Summary of all components
      sst list metrics            # List all metrics
      sst list tables             # List all tables
      sst list metrics --table orders --format json
    """
    if ctx.invoked_subcommand is None:
        ctx.invoke(summary)


@list_cmd.command("summary")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
def summary(output_format, output_file, verbose, quiet, dbt_path, semantic_path):
    """Show a summary of all semantic model components."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, None)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)

    if output_format == "json":
        data = {
            "summary": {
                "tables": len(result.tables),
                "metrics": len(result.metrics),
                "relationships": len(result.relationships),
                "filters": len(result.filters),
                "semantic_views": len(result.semantic_views),
                "custom_instructions": len(result.custom_instructions),
                "verified_queries": len(result.verified_queries),
            },
            "total_count": result.total_count,
        }
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {
            "summary": {
                "tables": len(result.tables),
                "metrics": len(result.metrics),
                "relationships": len(result.relationships),
                "filters": len(result.filters),
                "semantic_views": len(result.semantic_views),
                "custom_instructions": len(result.custom_instructions),
                "verified_queries": len(result.verified_queries),
            },
            "total_count": result.total_count,
        }
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Component", "Count"]
        rows = [
            ["Tables", str(len(result.tables))],
            ["Metrics", str(len(result.metrics))],
            ["Relationships", str(len(result.relationships))],
            ["Filters", str(len(result.filters))],
            ["Semantic Views", str(len(result.semantic_views))],
            ["Custom Instructions", str(len(result.custom_instructions))],
            ["Verified Queries", str(len(result.verified_queries))],
        ]
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header("Semantic Model Summary")
        output.blank_line()

    headers = ["Component", "Count"]
    rows = [
        ["Tables", str(len(result.tables))],
        ["Metrics", str(len(result.metrics))],
        ["Relationships", str(len(result.relationships))],
        ["Filters", str(len(result.filters))],
        ["Semantic Views", str(len(result.semantic_views))],
        ["Custom Instructions", str(len(result.custom_instructions))],
        ["Verified Queries", str(len(result.verified_queries))],
        ["─" * 20, "─" * 5],
        ["Total Components", str(result.total_count)],
    ]
    click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)
        click.echo(f"\n  Use 'sst list <component>' for details.")


@list_cmd.command("metrics")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
@TABLE_FILTER_OPTION
@click.option("--with-expr", is_flag=True, help="Include full expressions in output")
def metrics(output_format, output_file, verbose, quiet, dbt_path, semantic_path, table_filter, with_expr):
    """List all metrics defined in semantic models."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, table_filter)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)
    items = result.metrics

    if output_format == "json":
        data = {"metrics": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {"metrics": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Name", "Tables", "Description"]
        if with_expr:
            headers.append("Expression")
        rows = []
        for m in items:
            tables = m.get("tables", [])
            table_str = ", ".join(str(t) for t in tables) if tables else str(m.get("table_name") or "")
            row = [str(m.get("name") or ""), table_str, str(m.get("description") or "")]
            if with_expr:
                row.append(str(m.get("expr") or ""))
            rows.append(row)
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header(f"Metrics ({len(items)} total)")
        output.blank_line()

    if not items:
        click.echo("  (no metrics found)")
    else:
        headers = ["Name", "Tables", "Description"]
        if with_expr or verbose:
            headers.append("Expression")
        rows = []
        for m in items:
            tables = m.get("tables", [])
            table_str = ", ".join(str(t) for t in tables) if tables else str(m.get("table_name") or "")
            row = [str(m.get("name") or ""), table_str, str(m.get("description") or "")]
            if with_expr or verbose:
                row.append(str(m.get("expr") or ""))
            rows.append(row)
        click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)


@list_cmd.command("relationships")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
@TABLE_FILTER_OPTION
def relationships(output_format, output_file, verbose, quiet, dbt_path, semantic_path, table_filter):
    """List all relationships defined in semantic models."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, table_filter)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)
    items = result.relationships

    if output_format == "json":
        data = {"relationships": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {"relationships": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Name", "Left Table", "Right Table"]
        rows = [
            [str(r.get("relationship_name") or ""), str(r.get("left_table_name") or ""), str(r.get("right_table_name") or "")]
            for r in items
        ]
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header(f"Relationships ({len(items)} total)")
        output.blank_line()

    if not items:
        click.echo("  (no relationships found)")
    else:
        headers = ["Name", "Left Table", "\u2192", "Right Table"]
        rows = [
            [
                str(r.get("relationship_name") or ""),
                str(r.get("left_table_name") or ""),
                "\u2192",
                str(r.get("right_table_name") or ""),
            ]
            for r in items
        ]
        click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)


@list_cmd.command("filters")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
@TABLE_FILTER_OPTION
def filters(output_format, output_file, verbose, quiet, dbt_path, semantic_path, table_filter):
    """List all filters defined in semantic models."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, table_filter)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)
    items = result.filters

    if output_format == "json":
        data = {"filters": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {"filters": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Name", "Table", "Description", "Expression"]
        rows = [
            [str(f.get("name") or ""), str(f.get("table_name") or ""), str(f.get("description") or ""), str(f.get("expr") or "")]
            for f in items
        ]
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header(f"Filters ({len(items)} total)")
        output.blank_line()

    if not items:
        click.echo("  (no filters found)")
    else:
        headers = ["Name", "Table", "Description"]
        if verbose:
            headers.append("Expression")
        rows = []
        for f in items:
            row = [str(f.get("name") or ""), str(f.get("table_name") or ""), str(f.get("description") or "")]
            if verbose:
                row.append(str(f.get("expr") or ""))
            rows.append(row)
        click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)


@list_cmd.command("semantic-views")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
@TABLE_FILTER_OPTION
def semantic_views(output_format, output_file, verbose, quiet, dbt_path, semantic_path, table_filter):
    """List all semantic views defined in semantic models."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, table_filter)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)
    items = result.semantic_views

    if output_format == "json":
        data = {"semantic_views": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {"semantic_views": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Name", "Description", "Tables"]
        rows = []
        for v in items:
            tables = _parse_tables_json(v.get("tables", "[]"))
            rows.append([str(v.get("name") or ""), str(v.get("description") or ""), ", ".join(str(t) for t in tables)])
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header(f"Semantic Views ({len(items)} total)")
        output.blank_line()

    if not items:
        click.echo("  (no semantic views found)")
    else:
        headers = ["Name", "Description", "Tables"]
        if verbose:
            headers.append("Custom Instructions")
        rows = []
        for v in items:
            tables = _parse_tables_json(v.get("tables", "[]"))
            row = [str(v.get("name") or ""), str(v.get("description") or ""), ", ".join(str(t) for t in tables)]
            if verbose:
                ci = _parse_tables_json(v.get("custom_instructions", "[]"))
                row.append(", ".join(str(c) for c in ci))
            rows.append(row)
        click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)


@list_cmd.command("custom-instructions")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
def custom_instructions(output_format, output_file, verbose, quiet, dbt_path, semantic_path):
    """List all custom instructions defined in semantic models."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, None)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)
    items = result.custom_instructions

    if output_format == "json":
        data = {"custom_instructions": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {"custom_instructions": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Name", "Question Categorization", "SQL Generation"]
        rows = [
            [str(ci.get("name") or ""), str(ci.get("question_categorization") or ""), str(ci.get("sql_generation") or "")]
            for ci in items
        ]
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header(f"Custom Instructions ({len(items)} total)")
        output.blank_line()

    if not items:
        click.echo("  (no custom instructions found)")
    else:
        headers = ["Name"]
        if verbose:
            headers.extend(["Question Categorization", "SQL Generation"])
        rows = []
        for ci in items:
            row = [str(ci.get("name") or "")]
            if verbose:
                row.append(str(ci.get("question_categorization") or ""))
                row.append(str(ci.get("sql_generation") or ""))
            rows.append(row)
        click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)


@list_cmd.command("verified-queries")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
def verified_queries(output_format, output_file, verbose, quiet, dbt_path, semantic_path):
    """List all verified queries defined in semantic models."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, None)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)
    items = result.verified_queries

    if output_format == "json":
        data = {"verified_queries": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {"verified_queries": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Name", "Question", "Verified By", "Verified At"]
        rows = [
            [str(vq.get("name") or ""), str(vq.get("question") or ""), str(vq.get("verified_by") or ""), str(vq.get("verified_at") or "")]
            for vq in items
        ]
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header(f"Verified Queries ({len(items)} total)")
        output.blank_line()

    if not items:
        click.echo("  (no verified queries found)")
    else:
        headers = ["Name", "Question", "Verified By"]
        if verbose:
            headers.append("SQL")
        rows = []
        for vq in items:
            row = [str(vq.get("name") or ""), str(vq.get("question") or ""), str(vq.get("verified_by") or "")]
            if verbose:
                row.append(str(vq.get("sql") or ""))
            rows.append(row)
        click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)


@list_cmd.command("tables")
@FORMAT_OPTION
@OUTPUT_OPTION
@VERBOSE_OPTION
@QUIET_OPTION
@DBT_OPTION
@SEMANTIC_OPTION
@TABLE_FILTER_OPTION
def tables(output_format, output_file, verbose, quiet, dbt_path, semantic_path, table_filter):
    """List all tables with SST annotations."""
    machine_output = output_format in ("json", "yaml", "csv") or output_file
    _setup(verbose, quiet or machine_output)
    output = CLIOutput(verbose=verbose, quiet=quiet or machine_output)
    if not quiet and not machine_output:
        output.info(f"Running with sst={__version__}")

    config = _build_config(dbt_path, semantic_path, table_filter)
    start = time.time()
    result = _run_list(config)
    duration = time.time() - start

    _show_errors(result, output)
    items = result.tables

    if output_format == "json":
        data = {"tables": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_json_output(data), output_file)
        return

    if output_format == "yaml":
        data = {"tables": _relativize_source_files(items), "total_count": len(items)}
        write_output(format_yaml_output(data), output_file)
        return

    if output_format == "csv":
        headers = ["Name", "Database", "Schema", "Primary Key"]
        rows = [
            [str(t.get("table_name") or ""), str(t.get("database") or ""), str(t.get("schema") or ""), str(t.get("primary_key") or "")]
            for t in items
        ]
        write_output(format_csv_output(headers, rows), output_file)
        return

    if not quiet:
        output.blank_line()
        output.header(f"Tables ({len(items)} total)")
        output.blank_line()

    if not items:
        click.echo("  (no tables found)")
    else:
        headers = ["Name", "Database", "Schema", "Primary Key"]
        rows = [
            [str(t.get("table_name") or ""), str(t.get("database") or ""), str(t.get("schema") or ""), str(t.get("primary_key") or "")]
            for t in items
        ]
        click.echo(format_table(headers, rows))

    if not quiet:
        output.blank_line()
        output.success("Done", duration=duration)


def _relativize_source_files(items: list) -> list:
    """Relativize source_file paths in items for cleaner output."""
    result = []
    for item in items:
        if isinstance(item, dict) and "source_file" in item:
            item = {**item, "source_file": _relativize_path(item["source_file"])}
        result.append(item)
    return result
