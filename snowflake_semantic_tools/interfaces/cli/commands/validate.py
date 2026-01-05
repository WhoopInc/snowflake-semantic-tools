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
@click.option(
    "--target",
    "-t",
    help="Override database for schema verification (e.g., PROD, DEV). Uses manifest database if not specified.",
)
@click.option(
    "--snowflake-syntax-check/--no-snowflake-check",
    default=None,
    help="Validate SQL expressions against Snowflake. Catches typos like CUONT instead of COUNT. "
    "Default: uses validation.snowflake_syntax_check from sst_config.yaml if set.",
)
def validate(dbt, semantic, strict, verbose, exclude, dbt_compile, verify_schema, target, snowflake_syntax_check):
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

      # Verify against a specific database (e.g., PROD tables)
      sst validate --verify-schema --target PROD

      # Validate SQL syntax against Snowflake (catches typos like CUONT)
      sst validate --snowflake-syntax-check

      # Skip syntax check even if enabled in config
      sst validate --no-snowflake-check

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
            schema_result = _run_schema_verification(service, output, verbose, target)
            if schema_result:
                result.merge(schema_result)

        # Run Snowflake SQL syntax validation if enabled
        should_run_syntax_check = _should_run_syntax_check(snowflake_syntax_check)
        if should_run_syntax_check:
            output.blank_line()
            output.info("Validating SQL expressions against Snowflake...")
            syntax_result = _run_syntax_verification(service, output, verbose)
            if syntax_result:
                result.merge(syntax_result)

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
    target_database: str = None,
) -> ValidationResult:
    """
    Run Snowflake schema verification.

    Connects to Snowflake and verifies that columns defined in YAML
    actually exist in the physical tables.

    Args:
        service: The validation service with parsed data
        output: CLI output handler
        verbose: Whether to show verbose output
        target_database: Optional database override for schema verification

    Returns:
        ValidationResult with schema verification issues, or None on connection failure
    """
    try:
        # Import Snowflake components
        from snowflake_semantic_tools.core.validation.rules import SchemaValidator
        from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

        # Get target from environment
        output.debug("Loading Snowflake credentials from dbt profile...")
        target = os.getenv("DBT_TARGET")

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
        dbt_catalog = _build_dbt_catalog_for_schema_check(service._last_parse_result, target_database)

        if not dbt_catalog:
            output.warning("No tables found for schema verification")
            result = ValidationResult()
            result.add_warning(
                "Schema verification skipped: No tables with columns found in parsed data.",
                context={"issue": "no_tables"},
            )
            return result

        db_info = f" (database override: {target_database})" if target_database else ""
        output.debug(f"Verifying {len(dbt_catalog)} tables against Snowflake...{db_info}")

        # Use snowflake_session for guaranteed cleanup
        with snowflake_session(target=target) as client:
            output.debug(f"Connecting to Snowflake (account: {client.connection_manager.config.account})...")

            # Run schema validation
            schema_validator = SchemaValidator(client.metadata_manager)
            schema_result = schema_validator.validate(dbt_catalog)

            # Log summary
            if schema_result.error_count > 0:
                output.warning(f"Schema verification found {schema_result.error_count} column mismatches")
            else:
                output.success("All columns verified against Snowflake schema")

            return schema_result

    except click.ClickException as e:
        # Handle credential loading failures from snowflake_session
        result = ValidationResult()
        result.add_warning(
            f"Schema verification skipped: {e.message}",
            context={"issue": "missing_credentials"},
        )
        return result
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


def _build_dbt_catalog_for_schema_check(parse_result: dict, target_database: str = None) -> dict:
    """
    Build a dbt catalog structure for schema verification.

    Args:
        parse_result: The parsed data from the validation service
        target_database: Optional database override (e.g., "PROD" to verify against prod)

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
                # Use target_database override if provided, otherwise use manifest database
                database = target_database.upper() if target_database else table.get("database")
                catalog[table_name] = {
                    "name": table_name,
                    "database": database,
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


def _should_run_syntax_check(cli_flag: bool = None) -> bool:
    """
    Determine if Snowflake syntax check should run.

    Priority:
    1. CLI flag (--snowflake-syntax-check or --no-snowflake-check)
    2. Config file (validation.snowflake_syntax_check)
    3. Default: False

    Args:
        cli_flag: Explicit CLI flag value, or None if not specified

    Returns:
        True if syntax check should run
    """
    # CLI flag takes precedence
    if cli_flag is not None:
        return cli_flag

    # Check config file
    try:
        config = get_config()
        if config:
            validation_config = config.get("validation", {})
            return validation_config.get("snowflake_syntax_check", False)
    except Exception:
        pass

    # Default: disabled (opt-in feature)
    return False


def _run_syntax_verification(
    service: SemanticMetadataCollectionValidationService,
    output: CLIOutput,
    verbose: bool,
) -> ValidationResult:
    """
    Run Snowflake SQL syntax verification.

    Validates SQL expressions in metrics, filters, and verified queries
    by compiling them against Snowflake. Catches typos and Snowflake-specific
    syntax errors before deployment.

    Args:
        service: The validation service with parsed data
        output: CLI output handler
        verbose: Whether to show verbose output

    Returns:
        ValidationResult with syntax verification issues, or None on connection failure
    """
    try:
        # Import components
        from snowflake_semantic_tools.core.validation.rules import SnowflakeSyntaxValidator
        from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

        # Get the parse result from the service
        if not hasattr(service, "_last_parse_result") or not service._last_parse_result:
            output.warning("No parsed data available for syntax verification")
            result = ValidationResult()
            result.add_warning(
                "Syntax verification skipped: No parsed data available. Run validation first.",
                context={"issue": "no_parse_result"},
            )
            return result

        # Use snowflake_session for guaranteed cleanup
        target = os.getenv("DBT_TARGET")

        with snowflake_session(target=target) as client:
            output.debug(f"Connecting to Snowflake (account: {client.connection_manager.config.account})...")

            # Run syntax validation
            syntax_validator = SnowflakeSyntaxValidator(client)
            syntax_result = syntax_validator.validate(service._last_parse_result)

            # Log summary
            if syntax_result.error_count > 0:
                output.warning(f"SQL syntax check found {syntax_result.error_count} error(s)")
            else:
                output.success("All SQL expressions validated successfully")

            return syntax_result

    except click.ClickException as e:
        # Handle credential loading failures from snowflake_session
        result = ValidationResult()
        result.add_warning(
            f"Syntax verification skipped: {e.message}",
            context={"issue": "missing_credentials"},
        )
        return result
    except ImportError as e:
        logger.warning(f"Could not import required modules for syntax verification: {e}")
        result = ValidationResult()
        result.add_warning(
            f"Syntax verification skipped: Missing required module: {e}",
            context={"issue": "import_error"},
        )
        return result
    except Exception as e:
        logger.error(f"Syntax verification failed: {e}")
        result = ValidationResult()
        result.add_warning(
            f"Syntax verification failed: {e}. Check your Snowflake credentials and connectivity.",
            context={"issue": "connection_error", "error": str(e)},
        )
        return result
