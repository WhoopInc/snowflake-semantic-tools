"""
Generate Command

CLI command for generating SQL Semantic Views in Snowflake.
"""

import time
import traceback

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser
from snowflake_semantic_tools.interfaces.cli.defer import (
    DeferConfig,
    display_defer_info,
    get_modified_views_filter,
    resolve_defer_config,
)
from snowflake_semantic_tools.interfaces.cli.options import database_schema_options, defer_options, target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import (
    build_snowflake_config,
    get_target_database_schema,
    setup_command,
)
from snowflake_semantic_tools.services.generate_semantic_views import (
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
)
from snowflake_semantic_tools.shared.progress import CLIProgressCallback


@click.command()
@target_option
@database_schema_options
@defer_options
@click.option("--views", "-v", multiple=True, help="Specific views to generate (can repeat)")
@click.option("--all", "-a", is_flag=True, help="Generate all available views")
@click.option("--dry-run", is_flag=True, help="Show what would be generated without executing")
@click.option("--verbose", is_flag=True, help="Verbose output")
def generate(
    dbt_target,
    db,
    schema,
    defer_target,
    state,
    only_modified,
    no_defer,
    views,
    all,
    dry_run,
    verbose,
):
    """
    Generate SQL Semantic Views from metadata.

    Creates native Snowflake SEMANTIC VIEW objects for BI tools
    and Cortex Analyst consumption.

    Uses credentials from ~/.dbt/profiles.yml (profile name from dbt_project.yml).
    Database and schema default to values from the dbt profile if not specified.

    \b
    Semantic Views provide:
    - Native Snowflake integration
    - Query optimization
    - BI tool compatibility (Sigma, Hex, Tableau)
    - Cortex Analyst support

    \b
    Examples:
        # Generate all semantic views using profile defaults
        sst generate --all

        # Use specific dbt target
        sst generate --target prod --all

        # Override database/schema
        sst generate --db ANALYTICS --schema SEMANTIC --all

        # Generate specific views
        sst generate -v customer_360 -v sales_summary

        # With defer to use production table references
        sst generate --all --defer-target prod

        # Selective generation (only modified models)
        sst generate --all --defer-target prod --only-modified

        # Dry run to preview SQL
        sst generate --all --dry-run
    """
    # IMMEDIATE OUTPUT - show user command is running
    output = CLIOutput(verbose=verbose, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup
    output.debug("Setting up...")
    setup_command(verbose=verbose, validate_config=True)

    # Validate options
    if not views and not all:
        output.error("Either --views or --all must be specified")
        raise click.Abort()

    # Resolve database and schema from profile or CLI overrides
    target_db, target_schema = get_target_database_schema(
        dbt_target=dbt_target,
        db_override=db,
        schema_override=schema,
    )

    # Resolve defer configuration
    defer_config = resolve_defer_config(
        defer_target=defer_target,
        state_path=state,
        no_defer=no_defer,
        only_modified=only_modified,
    )

    # Build Snowflake config from dbt profile
    snowflake_config = build_snowflake_config(
        target=dbt_target,
        database=target_db,
        schema=target_schema,
        verbose=verbose,
    )

    # Load the defer manifest if enabled - the manifest contains the actual
    # database/schema for each table, which is needed for multi-database projects
    defer_manifest = None
    if defer_config.enabled and defer_config.manifest_path:
        defer_manifest = ManifestParser(defer_config.manifest_path)
        if defer_manifest.load():
            output.debug(f"Defer manifest loaded: {defer_config.manifest_path}")
            
            # Validate that manifest was compiled with the correct target (if target_name is available)
            manifest_target = defer_manifest.get_target_name()
            if manifest_target and defer_config.target:
                if manifest_target.lower() != defer_config.target.lower():
                    # Manifest has explicit target that doesn't match - this is an error
                    output.error(
                        f"Manifest target mismatch: manifest was compiled with '{manifest_target}' "
                        f"but you specified --defer-target {defer_config.target}"
                    )
                    output.blank_line()
                    output.info("To fix this, compile the manifest with the correct target:")
                    output.info(f"  dbt compile --target {defer_config.target}", indent=1)
                    output.blank_line()
                    output.info("Then re-run this command.")
                    raise click.Abort()
                else:
                    output.debug(f"Manifest target '{manifest_target}' matches defer target")
            
            # Log summary of what's in the manifest
            summary = defer_manifest.get_summary()
            if summary.get("loaded"):
                output.debug(f"Manifest contains {summary.get('total_models', 0)} models across databases: {summary.get('models_by_database', {})}")
        else:
            output.warning(f"Failed to load defer manifest from {defer_config.manifest_path}")
            defer_manifest = None

    # Create configuration
    config = UnifiedGenerationConfig(
        metadata_database=target_db,
        metadata_schema=target_schema,
        target_database=target_db,
        target_schema=target_schema,
        views_to_generate=list(views) if views else None,
        dry_run=dry_run,
        defer_manifest=defer_manifest,
    )

    # Create and execute service
    try:
        output.blank_line()
        profile_info = f"{snowflake_config.profile_name}.{snowflake_config.target_name}"
        output.info(f"Using dbt profile: {profile_info}")
        output.info(f"Reading metadata from: {target_db}.{target_schema}", indent=1)
        output.info(f"Creating views in: {target_db}.{target_schema}", indent=1)

        # Display defer info if enabled
        if defer_config.enabled:
            display_defer_info(output, defer_config)

        output.blank_line()
        output.info("Connecting to Snowflake...")

        # Use context manager for guaranteed Snowflake connection cleanup
        with SemanticViewGenerationService(snowflake_config) as service:
            # If --only-modified, filter views based on manifest comparison
            if defer_config.only_modified and defer_config.enabled:
                # Need to get available views first to filter them
                output.info("Validating metadata access...")
                available_views = service.get_available_views(target_db, target_schema)

                if available_views:
                    output.info(f"Found {len(available_views)} available views", indent=1)
                    filtered_views = get_modified_views_filter(defer_config, available_views, output)

                    if filtered_views is not None:
                        if len(filtered_views) == 0:
                            output.success("All views are up to date - nothing to regenerate")
                            return
                        config.views_to_generate = filtered_views
                        output.info(f"Filtering to {len(filtered_views)} view(s): {', '.join(filtered_views)}")

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
