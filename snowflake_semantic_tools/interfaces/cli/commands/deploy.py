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
from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser
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


@click.command(
    short_help="One-step: validate + extract + generate",
)
@target_option
@database_schema_options
@defer_options
@click.option(
    "--skip-validation", is_flag=True, help="Skip validation step (use when validation already run separately)"
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed progress (default: errors and warnings only)")
@click.option("--quiet", "-q", is_flag=True, help="Suppress all output except errors")
@click.pass_context
def deploy(ctx, dbt_target, db, schema, defer_target, state, only_modified, no_defer, skip_validation, verbose, quiet):
    """Deploy semantic models: validate + extract + generate in one step.

    The recommended way to deploy. Combines the full workflow into a single
    command, stopping at the first failure for safe CI/CD integration.

    \b
    Prerequisites:
      • 'dbt compile' has been run (manifest.json must exist)
      • Models annotated with config.meta.sst (use 'sst enrich' first)
      • Snowflake credentials in ~/.dbt/profiles.yml

    \b
    Examples:
      sst deploy                              Full deployment (profile defaults)
      sst deploy --target prod                Use 'prod' dbt target
      sst deploy --db ANALYTICS -s SEMANTIC   Override database/schema
      sst deploy --defer-target prod          Use prod table references
      sst deploy --only-modified              Only deploy changed models
      sst deploy --skip-validation            Skip if already validated
      sst deploy --quiet                      Errors only (CI/CD)

    \b
    Notes:
      • Both extract and generate use the same --db and --schema
      • Use --defer-target to reference production tables while deploying to dev
      • Use --only-modified for faster iteration on large projects

    \b
    What it does (in order):
      1. sst validate     Check for errors
      2. sst extract      Load metadata to Snowflake tables
      3. sst generate     Create semantic views
      Stops at first failure.

    \b
    Related Commands:
      sst validate            Run validation separately (faster iteration)
      sst generate --dry-run  Preview SQL without deploying
      sst list                Explore what was deployed
    """
    # IMMEDIATE OUTPUT - show user command is running
    output_format = ctx.obj.get("output_format", "table") if ctx.obj else "table"
    quiet_mode = output_format == "json" or quiet
    output = CLIOutput(verbose=verbose, quiet=quiet_mode)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup
    output.debug("Setting up...")
    setup_command(verbose=verbose, quiet=quiet_mode, validate_config=True)

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

    # Validate manifest target matches defer target (fail early with helpful message)
    if defer_config.enabled and defer_config.manifest_path:
        defer_manifest = ManifestParser(defer_config.manifest_path)
        if defer_manifest.load():
            manifest_target = defer_manifest.get_target_name()
            if manifest_target and defer_config.target:
                if manifest_target.lower() != defer_config.target.lower():
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

    # Create deployment config - the manifest path is passed so the service
    # can load it and look up each table's actual database/schema
    config = DeployConfig(
        database=target_db,
        schema=target_schema,
        skip_validation=skip_validation,
        verbose=verbose,
        quiet=quiet,
        only_modified=defer_config.only_modified,
        defer_database=defer_config.target,  # Defer target name for validation and summary
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

        if output_format == "json":
            import json as json_mod

            from snowflake_semantic_tools._version import __version__ as sst_version

            error_count = (
                result.validation_errors if hasattr(result, "validation_errors") else (0 if result.success else 1)
            )
            warning_count = result.validation_warnings if hasattr(result, "validation_warnings") else 0

            diagnostics = []
            if hasattr(result, "errors") and result.errors:
                for err in result.errors:
                    diagnostics.append({"severity": "error", "message": str(err), "code": "SST-D001"})

            json_output = {
                "tool": "sst",
                "version": sst_version,
                "schema_version": 1,
                "command": "deploy",
                "status": "success" if result.success else "error",
                "exit_code": 0 if result.success else 1,
                "duration_s": round(deploy_duration, 2),
                "diagnostics": diagnostics,
                "summary": {
                    "errors": error_count,
                    "warnings": warning_count,
                },
            }
            click.echo(json_mod.dumps(json_output, indent=2))
        else:
            result.print_summary(quiet=quiet)

            # Show dbt-style done line with actual error/warning counts
            output.blank_line()
            if result.success:
                output.done_line(passed=1, warned=0, errored=0, total=1)
            else:
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
