"""
Compile Command

CLI command for compiling SST metadata into a local manifest.
No Snowflake connection required.
"""

from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.options import target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import setup_command
from snowflake_semantic_tools.services.compile import CompileConfig, CompileService


@click.command(
    short_help="Compile SST metadata into a local manifest",
)
@target_option
@click.option("--dbt", "dbt_path", type=click.Path(exists=True), default=None, help="Custom dbt models path")
@click.option(
    "--semantic", "semantic_path", type=click.Path(exists=True), default=None, help="Custom semantic models path"
)
@click.option("--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def compile_cmd(ctx, dbt_target, dbt_path, semantic_path, verbose):
    """Parse all YAML files and write target/sst_manifest.json.

    \b
    No Snowflake connection required. The manifest contains all metadata
    needed for validate and generate commands.

    \b
    Examples:
      sst compile                         Compile all metadata
      sst compile --verbose               Show detailed output
      sst compile --dbt models/marts      Custom dbt path

    \b
    Next Steps:
      sst validate          Validate compiled metadata
      sst generate --all    Generate semantic views from manifest
    """
    output_format = ctx.obj.get("output_format", "table") if ctx.obj else "table"
    quiet_mode = output_format == "json"
    output = CLIOutput(verbose=verbose, quiet=quiet_mode)
    output.info(f"Running with sst={__version__}")

    setup_command(verbose=verbose, quiet=quiet_mode, validate_config=True)

    config = CompileConfig(
        dbt_path=Path(dbt_path) if dbt_path else None,
        semantic_path=Path(semantic_path) if semantic_path else None,
        target_database=dbt_target,
    )

    service = CompileService()
    result = service.compile(config)

    if result.success:
        output.blank_line()
        output.success(f"Compiled in {result.duration:.1f}s")
        output.info(f"  Tables: {result.tables_count}")
        output.info(f"  Metrics: {result.metrics_count}")
        output.info(f"  Semantic views: {result.views_count}")
        output.info(f"  Files tracked: {result.files_tracked}")
        output.info(f"  Manifest: {result.manifest_path}")
    else:
        output.blank_line()
        for err in result.errors:
            output.error(err)
        raise click.Abort()
