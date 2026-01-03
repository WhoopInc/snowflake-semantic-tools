"""
Extract Command

CLI command for extracting semantic metadata.
"""

import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import build_snowflake_config, setup_command
from snowflake_semantic_tools.services import SemanticMetadataExtractionService
from snowflake_semantic_tools.services.extract_semantic_metadata import ExtractConfig
from snowflake_semantic_tools.shared.progress import CLIProgressCallback


@click.command()
@click.option("--target", "-t", "dbt_target", help="dbt target from profiles.yml (default: uses profile's default)")
@click.option("--db", required=True, help="Target database for loading metadata")
@click.option("--schema", "-s", required=True, help="Target schema for loading metadata")
@click.option("--dbt", help="dbt models path (auto-detected from config if not specified)")
@click.option("--semantic", help="Semantic models path (auto-detected from config if not specified)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def extract(dbt_target, db, schema, dbt, semantic, verbose):
    """
    Extract semantic metadata from dbt models to Snowflake.

    Parses dbt and semantic model YAML files, resolves templates,
    and loads structured metadata to Snowflake tables.

    Uses credentials from ~/.dbt/profiles.yml (profile name from dbt_project.yml).

    \b
    Examples:
        # Extract metadata to Snowflake (uses default target)
        sst extract --db MY_DB -s MY_SCHEMA

        # Use specific dbt target
        sst extract --target prod --db PROD_DB -s SEMANTIC

        # With custom paths
        sst extract --db MY_DB -s MY_SCHEMA --dbt models/ --semantic semantic_models/
    """
    # IMMEDIATE OUTPUT
    output = CLIOutput(verbose=verbose, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup
    output.debug("Setting up...")
    setup_command(verbose=verbose, validate_config=True)

    # Build Snowflake config from dbt profile
    snowflake_config = build_snowflake_config(
        target=dbt_target,
        database=db,
        schema=schema,
        verbose=verbose,
    )

    # Create and execute service
    try:
        output.blank_line()
        profile_info = f"{snowflake_config.profile_name}.{snowflake_config.target_name}"
        output.info(f"Using dbt profile: {profile_info}")
        output.info(f"Target: {db}.{schema}", indent=1)

        service = SemanticMetadataExtractionService.create_from_config(snowflake_config)

        config = ExtractConfig(
            database=db,
            schema=schema,
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
