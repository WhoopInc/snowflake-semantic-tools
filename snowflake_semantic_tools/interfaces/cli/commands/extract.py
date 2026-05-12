"""
Extract Command

CLI command for extracting semantic metadata.
"""

import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.options import database_schema_options, target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import (
    build_snowflake_config,
    get_target_database_schema,
    setup_command,
)
from snowflake_semantic_tools.services import SemanticMetadataExtractionService
from snowflake_semantic_tools.services.extract_semantic_metadata import ExtractConfig
from snowflake_semantic_tools.shared.progress import CLIProgressCallback


@click.command(
    short_help="Load metadata to Snowflake tables (before generate)",
)
@target_option
@database_schema_options
@click.option("--dbt", help="dbt models path (auto-detected from config if not specified)")
@click.option("--semantic", help="Semantic models path (auto-detected from config if not specified)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def extract(ctx, dbt_target, db, schema, dbt, semantic, verbose):
    """Load semantic metadata from dbt YAML into Snowflake tables.

    Parses dbt models and semantic model YAML, resolves templates, and
    populates SM_* metadata tables in Snowflake. These tables are read
    by 'sst generate' to create semantic views.

    \b
    Prerequisites:
      • 'sst validate' passes (or use 'sst deploy' to combine steps)
      • Snowflake credentials in ~/.dbt/profiles.yml
      • Database/schema must exist (SST creates tables, not databases)

    \b
    Examples:
      sst extract                              Profile defaults
      sst extract --target prod                Use 'prod' dbt target
      sst extract --db MY_DB -s MY_SCHEMA      Override database/schema
      sst extract --dbt models/ --semantic sm/ Custom paths

    \b
    Next Step:
      sst generate --all      Create semantic views from extracted metadata

    \b
    Related Commands:
      sst validate            Check for errors before extracting
      sst deploy              Run validate + extract + generate in one step
      sst enrich              Populate metadata if models are incomplete
    """
    # IMMEDIATE OUTPUT
    output_format = ctx.obj.get("output_format", "table") if ctx.obj else "table"
    quiet_mode = output_format == "json"
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

    # Build Snowflake config from dbt profile
    snowflake_config = build_snowflake_config(
        target=dbt_target,
        database=target_db,
        schema=target_schema,
        verbose=verbose,
    )

    # Create and execute service
    try:
        output.blank_line()
        profile_info = f"{snowflake_config.profile_name}.{snowflake_config.target_name}"
        output.info(f"Using dbt profile: {profile_info}")
        output.info(f"Target: {target_db}.{target_schema}", indent=1)

        # Use context manager for guaranteed Snowflake connection cleanup
        with SemanticMetadataExtractionService.create_from_config(snowflake_config) as service:
            config = ExtractConfig(
                database=target_db,
                schema=target_schema,
                dbt_path=Path(dbt) if dbt else None,
                semantic_path=Path(semantic) if semantic else None,
            )

            # Create progress callback from CLIOutput
            progress_callback = CLIProgressCallback(output)

            extract_start = time.time()
            result = service.execute(config, progress_callback=progress_callback)
            extract_duration = time.time() - extract_start

            # Display results with improved formatting
            output.blank_line()
            if result.success:
                output.success(f"Extraction completed in {extract_duration:.1f}s")
            else:
                output.error(f"Extraction failed in {extract_duration:.1f}s")

            if output_format == "json":
                import json as json_mod

                from snowflake_semantic_tools._version import __version__ as sst_version

                diagnostics = []
                if hasattr(result, "errors") and result.errors:
                    for err in result.errors:
                        diagnostics.append({"severity": "error", "message": str(err), "code": "SST-E001"})

                rows_loaded = result.rows_loaded if hasattr(result, "rows_loaded") else 0

                json_output = {
                    "tool": "sst",
                    "version": sst_version,
                    "schema_version": 1,
                    "command": "extract",
                    "status": "success" if result.success else "error",
                    "exit_code": 0 if result.success else 1,
                    "duration_s": round(extract_duration, 2),
                    "diagnostics": diagnostics,
                    "summary": {
                        "errors": len(diagnostics),
                        "warnings": 0,
                        "rows_loaded": rows_loaded,
                    },
                }
                click.echo(json_mod.dumps(json_output, indent=2))
            else:
                result.print_summary()

            if not result.success:
                raise click.ClickException("Extraction failed")

    except click.ClickException:
        raise  # Re-raise click exceptions as-is
    except Exception as e:
        if verbose:
            click.echo("Full error traceback:", err=True)
            traceback.print_exc()
        raise click.ClickException(str(e))
