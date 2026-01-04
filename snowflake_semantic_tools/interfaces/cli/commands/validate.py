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
from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.infrastructure.dbt import DbtClient, DbtCompileError, DbtNotFoundError
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import setup_command
from snowflake_semantic_tools.services import SemanticMetadataCollectionValidationService
from snowflake_semantic_tools.services.validate_semantic_models import ValidateConfig
from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.config_utils import get_exclusion_patterns, get_exclusion_summary
from snowflake_semantic_tools.shared.events import setup_events
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("cli.validate")


@click.command()
@click.option("--dbt", help="dbt models path (auto-detected from config if not specified)")
@click.option("--semantic", help="Semantic models path (auto-detected from config if not specified)")
@click.option("--strict", is_flag=True, help="Fail on warnings (not just errors)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option("--exclude", help="Comma-separated list of directories to exclude (e.g., _intermediate,staging)")
@click.option(
    "--dbt-compile", is_flag=True, help="Auto-run dbt compile to generate/refresh manifest.json before validation"
)
@click.option(
    "--verify-schema",
    is_flag=True,
    help="Connect to Snowflake to verify YAML columns exist in actual tables (requires credentials)",
)
def validate(dbt, semantic, strict, verbose, exclude, dbt_compile, verify_schema):
    """
    Validate semantic models against dbt definitions.

    Checks for missing references, circular dependencies, duplicates,
    and performance issues. No Snowflake connection required by default.

    \b
    Examples:
      # Standard validation (offline)
      sst validate

      # Verify columns exist in Snowflake (requires connection)
      sst validate --verify-schema

      # Auto-compile dbt if manifest missing/stale (uses profile's default target)
      sst validate --dbt-compile

      # Use custom target (e.g., 'prod' for production)
      export DBT_TARGET=prod
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

        # Use DBT_TARGET env var, or get default from profiles.yml
        dbt_target = os.getenv("DBT_TARGET")
        if not dbt_target:
            try:
                from snowflake_semantic_tools.infrastructure.dbt.profile_parser import DbtProfileParser

                parser = DbtProfileParser()
                profile_name = parser.get_profile_name()
                profiles = parser._load_profiles()
                profile = profiles.get(profile_name, {})
                dbt_target = profile.get("target", "dev")
            except Exception:
                # Fallback to 'dev' if we can't read profile
                dbt_target = "dev"

        output.info(f"Compiling dbt project (target: {dbt_target})...")

        try:
            # Initialize dbt client (auto-detects Core vs Cloud CLI)
            dbt_client = DbtClient(project_dir=Path.cwd(), verbose=verbose)

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
                    click.echo("    1. Check ~/.dbt/profiles.yml exists and is configured", err=True)
                    click.echo("    2. Verify profile name in dbt_project.yml matches profiles.yml", err=True)
                    click.echo("    3. Verify Snowflake credentials in profiles.yml:", err=True)
                    click.echo("       (account, user, role, warehouse)", err=True)
                    click.echo("    4. Test manually: dbt compile", err=True)

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

        # Run Snowflake schema verification if requested
        if verify_schema:
            output.blank_line()
            output.info("Verifying columns against Snowflake schema...")
            schema_result = _run_schema_verification(service, output, verbose)
            if schema_result:
                result.merge(schema_result)

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


def _run_schema_verification(
    service: SemanticMetadataCollectionValidationService,
    output: CLIOutput,
    verbose: bool,
) -> ValidationResult:
    """
    Run Snowflake schema verification.

    Connects to Snowflake and verifies that columns defined in YAML
    actually exist in the physical tables.

    Args:
        service: The validation service with parsed data
        output: CLI output handler
        verbose: Whether to show verbose output

    Returns:
        ValidationResult with schema verification issues, or None on connection failure
    """
    from typing import Optional

    try:
        # Import Snowflake components
        from snowflake_semantic_tools.core.validation.rules import SchemaValidator
        from snowflake_semantic_tools.infrastructure.dbt.profile_parser import DbtProfileParser
        from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient
        from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig

        # Get Snowflake config from dbt profile
        output.debug("Loading Snowflake credentials from dbt profile...")
        parser = DbtProfileParser()
        profile_name = parser.get_profile_name()
        target = os.getenv("DBT_TARGET")

        if not target:
            profiles = parser._load_profiles()
            profile = profiles.get(profile_name, {})
            target = profile.get("target", "dev")

        creds = parser.get_snowflake_credentials(profile_name, target)

        if not creds:
            output.warning("Could not load Snowflake credentials from dbt profile")
            result = ValidationResult()
            result.add_warning(
                "Schema verification skipped: Could not load Snowflake credentials. "
                "Ensure your dbt profile is configured correctly.",
                context={"issue": "missing_credentials"},
            )
            return result

        # Create Snowflake config
        config = SnowflakeConfig(
            account=creds.get("account", ""),
            user=creds.get("user", ""),
            password=creds.get("password"),
            role=creds.get("role"),
            warehouse=creds.get("warehouse"),
            database=creds.get("database"),
            schema=creds.get("schema"),
            authenticator=creds.get("authenticator"),
            private_key_path=creds.get("private_key_path"),
            private_key_passphrase=creds.get("private_key_passphrase"),
        )

        # Create Snowflake client
        output.debug(f"Connecting to Snowflake (account: {config.account})...")
        client = SnowflakeClient(config)

        # Get the dbt catalog from the service's last parse result
        # We need to build it from the parsed data
        if not hasattr(service, "_last_parse_result") or not service._last_parse_result:
            output.warning("No parsed data available for schema verification")
            result = ValidationResult()
            result.add_warning(
                "Schema verification skipped: No parsed data available. " "Run validation first.",
                context={"issue": "no_parse_result"},
            )
            return result

        # Build dbt catalog from parsed data
        dbt_catalog = _build_dbt_catalog_for_schema_check(service._last_parse_result)

        if not dbt_catalog:
            output.warning("No tables found for schema verification")
            result = ValidationResult()
            result.add_warning(
                "Schema verification skipped: No tables with columns found in parsed data.",
                context={"issue": "no_tables"},
            )
            return result

        output.debug(f"Verifying {len(dbt_catalog)} tables against Snowflake...")

        # Run schema validation
        schema_validator = SchemaValidator(client.metadata_manager)
        schema_result = schema_validator.validate(dbt_catalog)

        # Log summary
        if schema_result.error_count > 0:
            output.warning(f"Schema verification found {schema_result.error_count} column mismatches")
        else:
            output.success("All columns verified against Snowflake schema")

        return schema_result

    except ImportError as e:
        logger.warning(f"Could not import required modules for schema verification: {e}")
        result = ValidationResult()
        result.add_warning(
            f"Schema verification skipped: Missing required module: {e}",
            context={"issue": "import_error"},
        )
        return result
    except Exception as e:
        logger.error(f"Schema verification failed: {e}")
        result = ValidationResult()
        result.add_warning(
            f"Schema verification failed: {e}. Check your Snowflake credentials and connectivity.",
            context={"issue": "connection_error", "error": str(e)},
        )
        return result


def _build_dbt_catalog_for_schema_check(parse_result: dict) -> dict:
    """
    Build a dbt catalog structure for schema verification.

    Args:
        parse_result: The parsed data from the validation service

    Returns:
        Dictionary mapping table names to their info including columns
    """
    catalog = {}

    dbt_data = parse_result.get("dbt", {})

    # Get tables
    tables = dbt_data.get("sm_tables", [])
    for table in tables:
        if isinstance(table, dict):
            table_name = table.get("table_name", "").lower()
            if table_name:
                catalog[table_name] = {
                    "name": table_name,
                    "database": table.get("database"),
                    "schema": table.get("schema"),
                    "columns": {},
                }

    # Add columns from dimensions, facts, and time_dimensions
    for col_type in ["sm_dimensions", "sm_facts", "sm_time_dimensions"]:
        columns = dbt_data.get(col_type, [])
        for col in columns:
            if isinstance(col, dict):
                table_name = col.get("table_name", "").lower()
                col_name = col.get("name", "").lower()

                if table_name in catalog and col_name:
                    catalog[table_name]["columns"][col_name] = col

    return catalog
