"""
Validate Command

CLI command for validating semantic models.
"""

import os
import sys
import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.infrastructure.dbt import DbtClient, DbtCompileError, DbtNotFoundError
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import setup_command
from snowflake_semantic_tools.services import SemanticMetadataCollectionValidationService
from snowflake_semantic_tools.services.validate_semantic_models import ValidateConfig
from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.config_utils import get_exclusion_patterns, get_exclusion_summary
from snowflake_semantic_tools.shared.events import setup_events


@click.command()
@click.option("--dbt", help="dbt models path (auto-detected from config if not specified)")
@click.option("--semantic", help="Semantic models path (auto-detected from config if not specified)")
@click.option("--strict", is_flag=True, help="Fail on warnings (not just errors)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option("--exclude", help="Comma-separated list of directories to exclude (e.g., _intermediate,staging)")
@click.option(
    "--dbt-compile", is_flag=True, help="Auto-run dbt compile to generate/refresh manifest.json before validation"
)
def validate(dbt, semantic, strict, verbose, exclude, dbt_compile):
    """
    Validate semantic models against dbt definitions.

    Checks for missing references, circular dependencies, duplicates,
    and performance issues. No Snowflake connection required.

    \b
    Examples:
      # Standard validation
      sst validate

      # Auto-compile dbt if manifest missing/stale (uses 'prod' target by default)
      sst validate --dbt-compile

      # Use custom target (e.g., 'ci' for CI/CD environments with private key auth)
      export DBT_TARGET=ci
      sst validate --dbt-compile

      # Validate with verbose output
      sst validate --verbose

      # Strict mode (fail on warnings)
      sst validate --strict

      # Custom paths
      sst validate --dbt models/ --semantic semantic_models/
    """
    # IMMEDIATE OUTPUT - show user command is running
    output = CLIOutput(verbose=verbose, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup (loads env, events, validates config, sets logging)
    output.debug("Loading environment...")
    setup_command(verbose=verbose, validate_config=True)

    if verbose:
        config_file = get_config()
        if config_file:
            output.debug("Found sst_config.yaml")

    # Run dbt compile if requested
    if dbt_compile:
        output.blank_line()
        output.info("Compiling dbt project...")

        # Use DBT_TARGET env var to allow customization (defaults to 'prod')
        # Note: Only used for dbt Core; Cloud CLI uses cloud environment
        dbt_target = os.getenv("DBT_TARGET", "prod")

        try:
            # Initialize dbt client (auto-detects Core vs Cloud CLI)
            dbt_client = DbtClient(project_dir=Path.cwd(), verbose=verbose)

            dbt_type_str = dbt_client.dbt_type.value
            if dbt_client.dbt_type.value == "core":
                output.debug(f"Detected dbt Core, using target: {dbt_target}")
            else:
                output.debug(f"Detected dbt Cloud CLI, using cloud environment")

            # Run dbt compile
            compile_start = time.time()
            result = dbt_client.compile(target=dbt_target)
            compile_duration = time.time() - compile_start

            if not result.success:
                output.blank_line()
                output.error("dbt compile failed", duration=compile_duration)
                output.blank_line()
                output.rule("=")

                # Show actual dbt error (might be in stdout or stderr)
                dbt_output = result.stderr or result.stdout or "No error output captured"
                click.echo("dbt error output:", err=True)
                click.echo(dbt_output, err=True)

                # Provide context-specific help based on dbt type
                output.blank_line()
                output.rule("=")
                click.echo("Troubleshooting:", err=True)

                if dbt_client.dbt_type.value == "cloud_cli":
                    # Cloud CLI-specific guidance
                    click.echo("  dbt Cloud CLI requires cloud environment configuration:", err=True)
                    click.echo("    1. Run: dbt environment show", err=True)
                    click.echo("       (This will show if your environment is set up)", err=True)
                    click.echo(
                        "    2. If not configured, visit dbt Cloud to set up your development environment", err=True
                    )
                    click.echo("    3. Then run: dbt environment configure", err=True)
                    click.echo("  Docs: https://docs.getdbt.com/docs/cloud/cloud-cli-installation", err=True)
                else:
                    # Core-specific guidance
                    click.echo("  dbt Core requires proper profiles.yml and credentials:", err=True)
                    click.echo("    1. Check profiles.yml exists in ~/.dbt/ or project root", err=True)
                    click.echo("    2. Verify Snowflake credentials in .env:", err=True)
                    click.echo(
                        "       SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE", err=True
                    )
                    click.echo("    3. Test manually: dbt compile --target {}", err=True)

                click.echo("\n  Common to both:", err=True)
                click.echo("    - Model SQL errors: Run 'dbt debug' to check", err=True)
                click.echo("    - Missing packages: Run 'dbt deps'", err=True)

                sys.exit(1)

            # Successful compile
            output.success("dbt compile completed", duration=compile_duration)
            manifest_path = dbt_client.get_manifest_path()
            if manifest_path.exists():
                output.debug(f"Generated manifest at: {manifest_path}")

        except DbtNotFoundError as e:
            click.echo(f"\nERROR: {e}", err=True)
            sys.exit(1)

    # Get exclusion patterns using reusable utility
    exclude_dirs = get_exclusion_patterns(cli_exclude=exclude)

    # Show exclusion info if any are configured
    if exclude_dirs and verbose:
        summary = get_exclusion_summary(cli_exclude=exclude)
        output.blank_line()
        if summary["config_patterns"]:
            output.debug(f"Config exclusions: {', '.join(summary['config_patterns'])}")
        if summary["cli_patterns"]:
            output.debug(f"CLI exclusions: {', '.join(summary['cli_patterns'])}")
        output.debug(f"Total exclusion patterns: {summary['total_count']}")

    # Create and execute service
    try:
        output.blank_line()
        output.info("Starting validation...")

        service = SemanticMetadataCollectionValidationService.create_from_config()

        config = ValidateConfig(
            dbt_path=Path(dbt) if dbt else None,
            semantic_path=Path(semantic) if semantic else None,
            strict_mode=strict,
            exclude_dirs=exclude_dirs if exclude_dirs else None,
        )

        val_start = time.time()
        result = service.execute(config, verbose=verbose)
        val_duration = time.time() - val_start

        # Display results with improved formatting
        output.blank_line()

        # Enhanced summary
        if result.is_valid:
            output.success(f"Validation completed in {val_duration:.1f}s")
        else:
            output.error(f"Validation completed in {val_duration:.1f}s")

        # Show detailed summary (includes comprehensive breakdown)
        # Note: Summary already shows all stats, so no need for redundant done line
        result.print_summary(verbose=verbose)

        # Exit with appropriate code
        if not result.is_valid:
            raise click.ClickException("Validation failed with errors")
        elif strict and result.has_warnings:
            raise click.ClickException("Validation failed with warnings (strict mode)")

    except click.ClickException:
        raise
    except Exception as e:
        output.blank_line()
        output.error(f"Validation error: {str(e)}")
        if verbose:
            traceback.print_exc()
        raise click.ClickException(str(e))
