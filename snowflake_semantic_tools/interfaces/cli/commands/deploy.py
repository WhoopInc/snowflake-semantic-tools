"""
Deploy Command

One-step deployment: validate → extract → generate semantic models and views.

Combines the three-step workflow (validate, extract, generate) into a single
atomic operation for simplified deployment and CI/CD integration.
"""

import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.defer import DeferConfig, display_defer_info, resolve_defer_config
from snowflake_semantic_tools.interfaces.cli.options import database_schema_options, defer_options, target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import (
    build_snowflake_config,
    get_target_database_schema,
    setup_command,
)
from snowflake_semantic_tools.services.deploy import DeployConfig, DeployService
from snowflake_semantic_tools.shared.progress import CLIProgressCallback


@click.command()
@target_option
@database_schema_options
@defer_options
@click.option(
    "--skip-validation", is_flag=True, help="Skip validation step (use when validation already run separately)"
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed progress (default: errors and warnings only)")
@click.option("--quiet", "-q", is_flag=True, help="Suppress all output except errors")
def deploy(dbt_target, db, schema, defer_target, state, only_modified, no_defer, skip_validation, verbose, quiet):
    """
    Deploy semantic models: validate → extract → generate in one step.

    Combines the full deployment workflow into a single command for convenience
    and consistency. Uses the same database and schema for both extraction
    (metadata tables) and generation (semantic views).

    Uses credentials from ~/.dbt/profiles.yml (profile name from dbt_project.yml).
    Database and schema default to values from the dbt profile if not specified.

    \b
    Examples:
        # Full deployment using profile defaults
        sst deploy

        # Full deployment to specific target
        sst deploy --db ANALYTICS --schema SEMANTIC

        # Use specific dbt target
        sst deploy --target prod

        # With defer to use production table references
        sst deploy --defer-target prod

        # Selective deployment (only modified models)
        sst deploy --defer-target prod --only-modified

        # Production deployment (validation already run)
        sst deploy --skip-validation

        # Quiet mode (errors only)
        sst deploy --quiet

    \b
    Workflow:
        1. Validate semantic models (unless --skip-validation)
        2. Extract metadata to {db}.{schema} tables
        3. Generate SQL semantic views
        4. Report summary (errors/warnings only by default)

    \b
    Notes:
        - Both extract and generate use the same --db and --schema
        - Use --defer-target to reference production tables while deploying to dev
        - Use --only-modified for faster iteration on large projects
        - Use --verbose to see detailed progress
        - Use --quiet to suppress all output except errors
        - Stops at first failure (validate, extract, or generate)
    """
    # IMMEDIATE OUTPUT - show user command is running
    output = CLIOutput(verbose=verbose, quiet=quiet)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup
    output.debug("Setting up...")
    setup_command(verbose=verbose, quiet=quiet, validate_config=True)

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

    try:
        output.debug("Building Snowflake configuration...")
        snowflake_config = build_snowflake_config(
            target=dbt_target,
            database=target_db,
            schema=target_schema,
            verbose=verbose,
        )
    except Exception as e:
        output.blank_line()
        output.error(f"Failed to configure Snowflake: {e}")
        if verbose:
            traceback.print_exc()
        raise click.Abort()

    # Determine defer database from manifest if enabled
    defer_database = None
    if defer_config.enabled and defer_config.manifest_path:
        defer_database = defer_config.target

    # Create deployment config
    config = DeployConfig(
        database=target_db,
        schema=target_schema,
        skip_validation=skip_validation,
        verbose=verbose,
        quiet=quiet,
        defer_database=defer_database,
        only_modified=defer_config.only_modified,
        defer_manifest_path=str(defer_config.manifest_path) if defer_config.manifest_path else None,
    )

    # Execute deployment
    try:
        output.blank_line()
        output.header("DEPLOYING SEMANTIC VIEWS TO SNOWFLAKE")
        output.info(f"Source: {Path.cwd()}")
        output.info(f"Target: {target_db}.{target_schema}")
        profile_info = f"{snowflake_config.profile_name}.{snowflake_config.target_name}"
        output.info(f"Profile: {profile_info}")

        # Display defer info if enabled
        if defer_config.enabled:
            display_defer_info(output, defer_config)

        output.blank_line()
        output.info("Starting deployment workflow...")
        output.debug("Connections will be established during workflow steps")

        service = DeployService(snowflake_config)

        # Create progress callback for service-level progress
        progress_callback = CLIProgressCallback(output)

        deploy_start = time.time()
        result = service.execute(config, progress_callback=progress_callback)
        deploy_duration = time.time() - deploy_start

        # Display results with improved formatting
        output.blank_line()
        if result.success:
            output.success(f"Deployment completed in {deploy_duration:.1f}s")
        else:
            output.error(f"Deployment failed in {deploy_duration:.1f}s")

        # Display summary
        result.print_summary(quiet=quiet)

        # Show dbt-style done line with actual error/warning counts
        output.blank_line()
        if result.success:
            output.done_line(passed=1, warned=0, errored=0, total=1)
        else:
            # Show actual validation errors and warnings if validation ran
            error_count = result.validation_errors if hasattr(result, "validation_errors") else 1
            warning_count = result.validation_warnings if hasattr(result, "validation_warnings") else 0
            output.done_line(passed=0, warned=warning_count, errored=error_count, total=1)

        # Exit with appropriate code
        if not result.success:
            raise click.ClickException("Deployment failed - see errors above")

    except click.ClickException:
        raise
    except Exception as e:
        output.blank_line()
        output.error(f"Deployment error: {str(e)}")
        if verbose:
            click.echo("\nFull error traceback:", err=True)
            traceback.print_exc()
        raise click.ClickException(str(e))
