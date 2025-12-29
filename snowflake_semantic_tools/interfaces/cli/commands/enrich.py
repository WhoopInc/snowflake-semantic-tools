"""
Enrich Command

CLI command for enriching dbt YAML metadata with semantic information.
"""

import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import setup_command
from snowflake_semantic_tools.services.enrich_metadata import EnrichmentConfig, MetadataEnrichmentService
from snowflake_semantic_tools.shared.events import setup_events
from snowflake_semantic_tools.shared.progress import CLIProgressCallback
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


def _determine_components(
    enrich_all,
    synonyms,
    column_types,
    data_types,
    sample_values,
    detect_enums,
    primary_keys,
    table_synonyms,
    column_synonyms,
):
    """
    Determine which components to enrich based on CLI flags.

    Logic:
    - --all: Everything including synonyms
    - --synonyms: Both table and column synonyms
    - Individual flags: Explicit components only
    - No flags: Default (all standard, no synonyms)

    Returns:
        List of component names to enrich
    """
    # --all means EVERYTHING including synonyms
    if enrich_all:
        return [
            "column-types",
            "data-types",
            "sample-values",
            "detect-enums",
            "primary-keys",
            "table-synonyms",
            "column-synonyms",
        ]

    # If ANY individual flags set, use only those
    if any(
        [column_types, data_types, sample_values, detect_enums, primary_keys, table_synonyms, column_synonyms, synonyms]
    ):
        components = []

        if column_types:
            components.append("column-types")
        if data_types:
            components.append("data-types")
        if sample_values:
            components.append("sample-values")
        if detect_enums:
            components.append("detect-enums")
        if primary_keys:
            components.append("primary-keys")

        # --synonyms is shorthand for both
        if synonyms:
            components.append("table-synonyms")
            components.append("column-synonyms")
        else:
            if table_synonyms:
                components.append("table-synonyms")
            if column_synonyms:
                components.append("column-synonyms")

        return components

    # No flags: Default (all standard components, NO synonyms)
    # This is backward compatible behavior
    return ["column-types", "data-types", "sample-values", "detect-enums", "primary-keys"]


@click.command()
@click.argument("target_path", type=click.Path(exists=True))
@click.option("--database", "-d", required=False, help="Target database (optional if manifest.json exists)")
@click.option("--schema", "-s", required=False, help="Target schema (optional if manifest.json exists)")
@click.option(
    "--manifest", type=click.Path(exists=True), help="Path to manifest.json (default: ./target/manifest.json)"
)
@click.option("--allow-non-prod", is_flag=True, help="Allow enrichment from non-production manifest")
# Issue #44: Removed --pk-candidates flag (undocumented, adds complexity)
# Primary key detection now happens automatically via --primary-keys flag
@click.option("--column-types", "-ct", is_flag=True, help="Enrich column types (dimension/fact/time_dimension)")
@click.option("--data-types", "-dt", is_flag=True, help="Enrich data types (map Snowflake types)")
@click.option("--sample-values", "-sv", is_flag=True, help="Enrich sample values (queries data - SLOW)")
@click.option("--detect-enums", "-de", is_flag=True, help="Detect enum columns (low cardinality)")
@click.option("--primary-keys", "-pk", is_flag=True, help="Validate primary key candidates")
@click.option("--table-synonyms", "-ts", is_flag=True, help="Generate table-level synonyms via Cortex LLM")
@click.option("--column-synonyms", "-cs", is_flag=True, help="Generate column-level synonyms via Cortex LLM")
@click.option(
    "--synonyms",
    "-syn",
    is_flag=True,
    help="Generate both table and column synonyms (shorthand for --table-synonyms --column-synonyms)",
)
@click.option("--all", "enrich_all", is_flag=True, help="Enrich ALL components including synonyms")
@click.option("--force-synonyms", is_flag=True, help="Overwrite existing synonyms (re-generate even if synonyms exist)")
@click.option(
    "--force-column-types", is_flag=True, help="Overwrite existing column types (re-infer even if types exist)"
)
@click.option(
    "--force-data-types", is_flag=True, help="Overwrite existing data types (re-map even if data types exist)"
)
@click.option(
    "--force-primary-keys",
    is_flag=True,
    help="Overwrite existing primary keys (re-validate even if primary key exists)",
)
@click.option("--force-all", is_flag=True, help="Overwrite ALL existing values (force refresh everything)")
@click.option("--exclude", help="Comma-separated list of directories to exclude")
@click.option("--dry-run", is_flag=True, help="Preview changes without writing files")
@click.option("--fail-fast", is_flag=True, help="Stop on first error")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def enrich(
    target_path,
    database,
    schema,
    manifest,
    allow_non_prod,
    exclude,
    dry_run,
    fail_fast,
    verbose,
    column_types,
    data_types,
    sample_values,
    detect_enums,
    primary_keys,
    table_synonyms,
    column_synonyms,
    synonyms,
    enrich_all,
    force_synonyms,
    force_column_types,
    force_data_types,
    force_primary_keys,
    force_all,
):
    """
    Enrich dbt YAML metadata with semantic information.

    Automatically populates meta.sst blocks with:

    \b
    - Column types (dimension, fact, time_dimension)
    - Data types (mapped from Snowflake)
    - Sample values (fresh from Snowflake)
    - Primary keys (intelligently detected)
    - Enum detection (based on cardinality)

    Database and schema can now be auto-detected from dbt manifest.json:

    \b
    Examples:

    \b
        # NEW: Auto-detect from manifest (requires 'dbt compile' first)
        dbt compile --target prod
        sst enrich models/analytics/memberships/

        # Explicit database/schema (backward compatible, no manifest needed)
        sst enrich models/analytics/memberships/ --database ANALYTICS --schema memberships

        # Use specific manifest file
        sst enrich models/ --manifest target_prod/manifest.json

        # Allow non-production manifest
        sst enrich models/ --allow-non-prod

        # Dry run to preview changes
        sst enrich models/analytics/ --dry-run --verbose

        # Exclude specific directories
        sst enrich models/analytics/ --exclude data_science,year_in_review
    """
    # IMMEDIATE OUTPUT - show user command is running
    output = CLIOutput(verbose=verbose, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup
    output.debug("Loading environment...")
    setup_command(verbose=verbose, validate_config=True)

    # Parse excluded directories
    excluded_dirs = None
    if exclude:
        excluded_dirs = [d.strip() for d in exclude.split(",")]
        output.debug(f"Excluding directories: {', '.join(excluded_dirs)}")

    # Determine which components to enrich
    components = _determine_components(
        enrich_all,
        synonyms,
        column_types,
        data_types,
        sample_values,
        detect_enums,
        primary_keys,
        table_synonyms,
        column_synonyms,
    )

    # Create configuration
    # Issue #44: primary_key_candidates removed from CLI (use --primary-keys for auto-detection)
    config = EnrichmentConfig(
        target_path=target_path,
        database=database,
        schema=schema,
        excluded_dirs=excluded_dirs,
        dry_run=dry_run,
        fail_fast=fail_fast,
        manifest_path=Path(manifest) if manifest else None,
        require_prod_target=not allow_non_prod,
        allow_non_prod=allow_non_prod,
        components=components,
        enrich_all=enrich_all,
        enrich_synonyms=synonyms or table_synonyms or column_synonyms,
        force_synonyms=force_synonyms or force_all,
        force_column_types=force_column_types or force_all,
        force_data_types=force_data_types or force_all,
        force_primary_keys=force_primary_keys or force_all,
        force_all=force_all,
    )

    # Execute enrichment
    try:
        output.blank_line()
        output.info(f"Connecting to Snowflake...")

        service = MetadataEnrichmentService(config)

        connect_start = time.time()
        service.connect()
        connect_duration = time.time() - connect_start

        output.success("Connected to Snowflake", duration=connect_duration)

        output.blank_line()

        # Create progress callback from CLIOutput
        progress_callback = CLIProgressCallback(output)

        enrich_start = time.time()
        result = service.enrich(progress_callback=progress_callback)
        enrich_duration = time.time() - enrich_start

        service.close()

        # Display results with improved formatting
        output.blank_line()
        if result.status == "success":
            output.success(f"Enrichment completed in {enrich_duration:.1f}s")
        elif result.status == "partial":
            output.warning(f"Enrichment completed with errors in {enrich_duration:.1f}s")
        else:
            output.error(f"Enrichment failed in {enrich_duration:.1f}s")

        # Show detailed summary
        result.print_summary()

        # Show dbt-style done line
        output.blank_line()
        success_count = result.models_enriched if hasattr(result, "models_enriched") else 0
        failed_count = len(result.failed_models) if hasattr(result, "failed_models") else 0
        total_count = success_count + failed_count

        output.done_line(passed=success_count, errored=failed_count, total=total_count)

        # Exit with appropriate code
        if result.status == "failed":
            raise click.ClickException("Enrichment failed")
        elif result.status == "partial":
            output.blank_line()
            output.warning("Some models failed to process. See logs for details.")

    except Exception as e:
        output.blank_line()
        output.error(f"Enrichment error: {str(e)}")
        logger.error(f"Enrichment failed: {e}", exc_info=verbose)
        raise click.ClickException(str(e))
