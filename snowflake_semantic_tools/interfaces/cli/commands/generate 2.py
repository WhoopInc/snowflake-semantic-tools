"""
Generate Command

CLI command for generating SQL Semantic Views in Snowflake.
"""

import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import build_snowflake_config, setup_command
from snowflake_semantic_tools.services.generate_semantic_views import (
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
)
from snowflake_semantic_tools.shared.events import setup_events
from snowflake_semantic_tools.shared.progress import CLIProgressCallback


@click.command()
@click.option("--metadata-db", required=True, help="Database containing semantic metadata tables (SM_*)")
@click.option("--metadata-schema", required=True, help="Schema containing semantic metadata tables")
@click.option("--target-db", required=True, help="Target database for semantic views")
@click.option("--target-schema", required=True, help="Target schema for semantic views")
@click.option("--views", "-v", multiple=True, help="Specific views to generate (can repeat)")
@click.option("--all", "-a", is_flag=True, help="Generate all available views")
@click.option(
    "--defer-database",
    help="Override table database references (like dbt defer) - use production database instead of metadata database",
)
@click.option("--account", envvar="SNOWFLAKE_ACCOUNT", help="Snowflake account")
@click.option("--user", "-u", envvar="SNOWFLAKE_USER", help="Snowflake user")
@click.option("--role", envvar="SNOWFLAKE_ROLE", help="Snowflake role")
@click.option("--warehouse", "-w", envvar="SNOWFLAKE_WAREHOUSE", help="Snowflake warehouse")
@click.option("--dry-run", is_flag=True, help="Show what would be generated without executing")
@click.option("--verbose", is_flag=True, help="Verbose output")
def generate(
    metadata_db,
    metadata_schema,
    target_db,
    target_schema,
    views,
    all,
    defer_database,
    account,
    user,
    role,
    warehouse,
    dry_run,
    verbose,
):
    """
    Generate SQL Semantic Views from metadata.
    
    Creates native Snowflake SEMANTIC VIEW objects for BI tools
    and Cortex Analyst consumption.
    
    \b
    Semantic Views provide:
    - Native Snowflake integration
    - Query optimization
    - BI tool compatibility (Sigma, Hex, Tableau)
    - Cortex Analyst support
    
    \b
    Examples:
        # Generate all semantic views
        sst generate --metadata-db ANALYTICS --metadata-schema SEMANTIC \\
                    --target-db ANALYTICS --target-schema VIEWS --all
        
        # Generate specific views
        sst generate --metadata-db ANALYTICS --metadata-schema SEMANTIC \\
                    --target-db ANALYTICS --target-schema VIEWS \\
                    -v customer_360 -v sales_summary
        
        # Use defer for dev environment (reference prod tables)
        sst generate --metadata-db DEV --metadata-schema SEMANTIC \\
                    --target-db DEV --target-schema VIEWS \\
                    --defer-database ANALYTICS --all
        
        # Dry run to preview SQL
        sst generate --metadata-db ANALYTICS --metadata-schema SEMANTIC \\
                    --target-db ANALYTICS --target-schema VIEWS \\
                    --all --dry-run
    """
    # IMMEDIATE OUTPUT - show user command is running
    output = CLIOutput(verbose=verbose, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup and Snowflake configuration
    output.debug("Loading environment...")
    setup_command(verbose=verbose, validate_config=True)

    # Validate options
    if not views and not all:
        output.error("Either --views or --all must be specified")
        raise click.Abort()

    snowflake_config = build_snowflake_config(
        account=account,
        user=user,
        role=role,
        warehouse=warehouse,
        database=metadata_db,
        schema=metadata_schema,
        verbose=verbose,
    )

    # Create configuration
    config = UnifiedGenerationConfig(
        metadata_database=metadata_db,
        metadata_schema=metadata_schema,
        target_database=target_db,
        target_schema=target_schema,
        views_to_generate=list(views) if views else None,
        dry_run=dry_run,
        defer_database=defer_database,
    )

    # Create and execute service
    try:
        output.blank_line()
        output.info(f"Configured for Snowflake account: {snowflake_config.account}")
        output.info(f"Reading metadata from: {metadata_db}.{metadata_schema}", indent=1)
        output.info(f"Creating views in: {target_db}.{target_schema}", indent=1)
        if defer_database:
            output.info(f"Defer mode: Using {defer_database} for table references", indent=1)

        output.blank_line()
        output.info("Connecting to Snowflake...")

        service = SemanticViewGenerationService(snowflake_config)

        # Create progress callback from CLIOutput
        progress_callback = CLIProgressCallback(output)

        gen_start = time.time()
        result = service.generate(config, progress_callback=progress_callback)
        gen_duration = time.time() - gen_start

        # Display results with improved formatting
        output.blank_line()
        if result.success:
            output.success(f"Generation completed in {gen_duration:.1f}s")
        else:
            output.error(f"Generation failed in {gen_duration:.1f}s")

        # Show detailed summary
        result.print_summary()

        # If dry-run, show SQL sample
        if dry_run and result.sql_statements:
            output.blank_line()
            output.rule("=", width=60)
            output.info("SAMPLE SQL (DRY RUN - First View)")
            output.rule("=", width=60)
            first_view = list(result.sql_statements.keys())[0]
            click.echo(f"\n-- View: {first_view}")
            click.echo(result.sql_statements[first_view][:2000])
            if len(result.sql_statements[first_view]) > 2000:
                click.echo("... [truncated]")

        # Show dbt-style done line with view counts
        output.blank_line()
        success_count = result.views_created if hasattr(result, "views_created") else 0
        failed_count = len(result.errors) if hasattr(result, "errors") else 0
        total_count = success_count + failed_count

        output.done_line(passed=success_count, errored=failed_count, total=total_count)

        if not result.success:
            raise click.ClickException("Generation failed - see errors above")

    except Exception as e:
        output.blank_line()
        output.error(f"Generation error: {str(e)}")
        if verbose:
            traceback.print_exc()
        raise click.ClickException(str(e))
