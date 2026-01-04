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


@click.command()
@target_option
@database_schema_options
@click.option("--dbt", help="dbt models path (auto-detected from config if not specified)")
@click.option("--semantic", help="Semantic models path (auto-detected from config if not specified)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def extract(dbt_target, db, schema, dbt, semantic, verbose):
    """
    Extract semantic metadata from dbt models to Snowflake.

    Parses dbt and semantic model YAML files, resolves templates,
    and loads structured metadata to Snowflake tables.

    Uses credentials from ~/.dbt/profiles.yml (profile name from dbt_project.yml).
    Database and schema default to values from the dbt profile if not specified.

    \b
    Examples:
        # Extract metadata using profile defaults
        sst extract

        # Extract to specific database/schema
        sst extract --db MY_DB -s MY_SCHEMA

        # Use specific dbt target
        sst extract --target prod

        # Override profile with explicit values
        sst extract --target dev --db ANALYTICS_DEV -s SEMANTIC

        # With custom paths
        sst extract --dbt models/ --semantic semantic_models/
    """
    # IMMEDIATE OUTPUT
    output = CLIOutput(verbose=verbose, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup
    output.debug("Setting up...")
    setup_command(verbose=verbose, validate_config=True)

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

            # Display results (summary already shows everything needed)
            result.print_summary()

            # No need for done line - summary box is comprehensive
            if not result.success:
                raise click.ClickException("Extraction failed")

    except click.ClickException:
        raise  # Re-raise click exceptions as-is
    except Exception as e:
        if verbose:
            click.echo("Full error traceback:", err=True)
            traceback.print_exc()
        raise click.ClickException(str(e))
