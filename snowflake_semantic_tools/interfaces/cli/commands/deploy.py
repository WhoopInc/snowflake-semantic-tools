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
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import build_snowflake_config, setup_command
from snowflake_semantic_tools.services.deploy import DeployConfig, DeployService
from snowflake_semantic_tools.shared.events import setup_events
from snowflake_semantic_tools.shared.progress import CLIProgressCallback


@click.command()
@click.option("--db", required=True, help="Target database (used for both extraction and generation)")
@click.option("--schema", "-s", required=True, help="Target schema (used for both extraction and generation)")
@click.option(
    "--skip-validation", is_flag=True, help="Skip validation step (use when validation already run separately)"
)
@click.option("--account", envvar="SNOWFLAKE_ACCOUNT", help="Snowflake account")
@click.option("--user", "-u", envvar="SNOWFLAKE_USER", help="Snowflake user")
@click.option("--role", envvar="SNOWFLAKE_ROLE", help="Snowflake role")
@click.option("--warehouse", "-w", envvar="SNOWFLAKE_WAREHOUSE", help="Snowflake warehouse")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed progress (default: errors and warnings only)")
@click.option("--quiet", "-q", is_flag=True, help="Suppress all output except errors")
def deploy(db, schema, skip_validation, account, user, role, warehouse, verbose, quiet):
    """
    Deploy semantic models: validate → extract → generate in one step.

    Combines the full deployment workflow into a single command for convenience
    and consistency. Uses the same database and schema for both extraction
    (metadata tables) and generation (semantic views).

    \b
    Examples:
        # Full deployment to QA
        sst deploy --db ANALYTICS_QA --schema SEMANTIC

        # Production deployment (validation already run)
        sst deploy --db ANALYTICS --schema SEMANTIC --skip-validation

        # Quiet mode (errors only)
        sst deploy --db ANALYTICS --schema SEMANTIC --quiet

    \b
    Workflow:
        1. Validate semantic models (unless --skip-validation)
        2. Extract metadata to {db}.{schema} tables
        3. Generate SQL semantic views
        4. Report summary (errors/warnings only by default)

    \b
    Notes:
        - Both extract and generate use the same --db and --schema
        - Use --verbose to see detailed progress
        - Use --quiet to suppress all output except errors
        - Stops at first failure (validate, extract, or generate)
    """
    # IMMEDIATE OUTPUT - show user command is running
    output = CLIOutput(verbose=verbose, quiet=quiet)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup and Snowflake configuration
    output.debug("Loading environment...")
    setup_command(verbose=verbose, quiet=quiet, validate_config=True)

    try:
        output.debug("Building Snowflake configuration...")
        snowflake_config = build_snowflake_config(
            account=account, user=user, role=role, warehouse=warehouse, database=db, schema=schema, verbose=verbose
        )
    except Exception as e:
        output.blank_line()
        output.error(f"Failed to configure Snowflake: {e}")
        if verbose:
            traceback.print_exc()
        raise click.Abort()

    # Create deployment config
    config = DeployConfig(database=db, schema=schema, skip_validation=skip_validation, verbose=verbose, quiet=quiet)

    # Execute deployment
    try:
        output.blank_line()
        output.header("DEPLOYING SEMANTIC VIEWS TO SNOWFLAKE")
        output.info(f"Source: {Path.cwd()}")
        output.info(f"Target: {db}.{schema}")
        output.info(f"Snowflake: {snowflake_config.account}")

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
