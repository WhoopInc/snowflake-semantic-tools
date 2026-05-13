"""
Clean Command

Remove SST-generated artifacts from the target directory.
Preserves dbt artifacts (manifest.json, run_results.json, etc.).
"""

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.services.clean import clean


@click.command(
    short_help="Remove SST-generated artifacts",
)
@click.option("--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def clean_cmd(ctx, verbose):
    """Remove SST-generated artifacts from target/.

    Deletes sst_manifest.json and semantic_views/ while preserving
    dbt's compilation cache (manifest.json, partial_parse.msgpack, etc.).

    \b
    No Snowflake connection required.

    \b
    What gets removed:
      target/sst_manifest.json       Compiled manifest
      target/semantic_views/         Generated DDL SQL files

    \b
    What is preserved:
      target/manifest.json           dbt manifest
      target/run_results.json        dbt run results
      target/partial_parse.msgpack   dbt parse cache

    \b
    Related Commands:
      sst compile     Regenerate the manifest
      sst generate    Regenerate semantic view DDL
    """
    global_format = ctx.obj.get("output_format", "table") if ctx.obj else "table"
    quiet_mode = global_format == "json"
    output = CLIOutput(verbose=verbose, quiet=quiet_mode)
    output.info(f"Running with sst={__version__}")

    result = clean()

    if not result.success:
        for err in result.errors:
            output.error(err)
        raise click.ClickException("Clean failed")

    if not result.removed:
        output.info("Nothing to clean")
    else:
        output.blank_line()
        output.info("Cleaned:")
        for item in result.removed:
            click.echo(f"  {item}")
        output.blank_line()

    output.success("Done")
