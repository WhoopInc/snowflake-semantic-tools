"""
Generate Command

CLI command for generating SQL Semantic Views in Snowflake.
"""

import time
import traceback
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser
from snowflake_semantic_tools.interfaces.cli.defer import DeferConfig, get_modified_views_filter, resolve_defer_config
from snowflake_semantic_tools.interfaces.cli.options import database_schema_options, defer_options, target_option
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.interfaces.cli.utils import (
    build_snowflake_config,
    get_target_database_schema,
    setup_command,
)
from snowflake_semantic_tools.services.generate_semantic_views import (
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
)
from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.progress import CLIProgressCallback


@click.command(
    short_help="Create Snowflake semantic views from metadata",
)
@target_option
@database_schema_options
@defer_options
@click.option("--views", "-v", multiple=True, help="Specific views to generate (can repeat)")
@click.option("--all", "-a", is_flag=True, help="Generate all available views")
@click.option("--dry-run", is_flag=True, help="Generate SQL without executing; writes to --output-dir")
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Output directory for dry-run SQL files (default: target/semantic_views/)",
)
@click.option("--threads", type=int, default=None, help="Concurrent views (default: from sst_config.yml or 1)")
@click.option("--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def generate(
    ctx,
    dbt_target,
    db,
    schema,
    defer_target,
    state,
    only_modified,
    no_defer,
    views,
    all,
    dry_run,
    output_dir,
    threads,
    verbose,
):
    """Create Snowflake SEMANTIC VIEW objects from extracted metadata.

    Reads the SM_* metadata tables (populated by 'sst extract') and creates
    native Snowflake semantic views for BI tools and Cortex Analyst.

    \b
    Prerequisites:
      • 'sst extract' has been run (metadata tables must exist)
      • Snowflake credentials in ~/.dbt/profiles.yml
      • Must specify --all or --views (one is required)

    \b
    Examples:
      sst generate --all                          Generate all views
      sst generate --all --target prod            Use 'prod' target
      sst generate --all --db ANALYTICS -s SEM    Override db/schema
      sst generate -v customer_360 -v sales       Specific views only
      sst generate --all --defer-target prod      Use prod table refs
      sst generate --all --only-modified          Only changed models
      sst generate --all --dry-run                Preview SQL output
      sst generate --all --dry-run --output-dir out/ Write SQL to custom dir

    \b
    Next Step:
      Query your semantic views in Snowflake or connect BI tools

    \b
    Related Commands:
      sst extract             Must run before generate (loads metadata tables)
      sst deploy              Run validate + extract + generate in one step
      sst list semantic-views See what views would be generated
    """
    # IMMEDIATE OUTPUT - show user command is running
    output_format = ctx.obj.get("output_format", "table") if ctx.obj else "table"
    quiet_mode = output_format == "json"
    output = CLIOutput(verbose=verbose, quiet=quiet_mode)
    output.info(f"Running with sst={__version__}")

    # Common CLI setup
    output.debug("Setting up...")
    setup_command(verbose=verbose, quiet=quiet_mode, validate_config=True)

    # Resolve threads from CLI > config > default(1)
    config = get_config()
    effective_threads = threads if threads is not None else config.get("generation.threads", 1)
    if effective_threads < 1:
        output.error("--threads must be at least 1")
        raise click.Abort()
    if effective_threads > 16:
        output.error("--threads cannot exceed 16")
        raise click.Abort()
    view_timeout = config.get("generation.view_timeout", 300)

    # Validate options
    if not views and not all:
        output.error("Either --views or --all must be specified")
        raise click.Abort()

    if output_dir and not dry_run:
        output.error("--output-dir can only be used with --dry-run")
        raise click.Abort()

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

    # Build Snowflake config from dbt profile
    snowflake_config = build_snowflake_config(
        target=dbt_target,
        database=target_db,
        schema=target_schema,
        verbose=verbose,
    )

    # Load the defer manifest if enabled - the manifest contains the actual
    # database/schema for each table, which is needed for multi-database projects
    defer_manifest = None
    if defer_config.enabled and defer_config.manifest_path:
        defer_manifest = ManifestParser(defer_config.manifest_path)
        if defer_manifest.load():
            output.debug(f"Defer manifest loaded: {defer_config.manifest_path}")

            # Validate that manifest was compiled with the correct target (if target_name is available)
            manifest_target = defer_manifest.get_target_name()
            if manifest_target and defer_config.target:
                if manifest_target.lower() != defer_config.target.lower():
                    # Manifest has explicit target that doesn't match - this is an error
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
                else:
                    output.debug(f"Manifest target '{manifest_target}' matches defer target")

            # Log summary of what's in the manifest
            summary = defer_manifest.get_summary()
            if summary.get("loaded"):
                output.debug(
                    f"Manifest contains {summary.get('total_models', 0)} models across databases: {summary.get('models_by_database', {})}"
                )
        else:
            output.warning(f"Failed to load defer manifest from {defer_config.manifest_path}")
            defer_manifest = None

    # Create configuration
    gen_config = UnifiedGenerationConfig(
        metadata_database=target_db,
        metadata_schema=target_schema,
        target_database=target_db,
        target_schema=target_schema,
        views_to_generate=list(views) if views else None,
        dry_run=dry_run,
        defer_manifest=defer_manifest,
        threads=effective_threads,
        view_timeout=view_timeout,
    )

    # Create and execute service
    try:
        output.blank_line()
        profile_info = f"{snowflake_config.profile_name}.{snowflake_config.target_name}"
        config_items = [
            ("Profile", profile_info),
            ("Read from", f"{target_db}.{target_schema}"),
            ("Create in", f"{target_db}.{target_schema}"),
        ]
        if defer_config.enabled:
            if defer_config.target:
                config_items.append(("Defer target", defer_config.target))
            if defer_config.manifest_path:
                config_items.append(("Defer manifest", str(defer_config.manifest_path)))
        if effective_threads > 1:
            config_items.append(("Threads", str(effective_threads)))
        else:
            config_items.append(("Threads", "1 (sequential)"))
        output.config_table(config_items)

        # Display defer warning if applicable
        if defer_config.enabled and defer_config.manifest_target_warning:
            output.warning(defer_config.manifest_target_warning)

        output.blank_line()
        output.info("Connecting to Snowflake...")

        # Use context manager for guaranteed Snowflake connection cleanup
        with SemanticViewGenerationService(snowflake_config) as service:
            # If --only-modified, filter views based on manifest comparison
            if defer_config.only_modified and defer_config.enabled:
                # Need to get available views first to filter them
                output.info("Validating metadata access...")
                available_views = service.get_available_views(target_db, target_schema)

                if available_views:
                    output.info(f"Found {len(available_views)} available views", indent=1)
                    filtered_views = get_modified_views_filter(defer_config, available_views, output)

                    if filtered_views is not None:
                        if len(filtered_views) == 0:
                            output.success("All views are up to date - nothing to regenerate")
                            return
                        gen_config.views_to_generate = filtered_views
                        output.info(f"Filtering to {len(filtered_views)} view(s): {', '.join(filtered_views)}")

            # Create progress callback from CLIOutput
            progress_callback = CLIProgressCallback(output)

            gen_start = time.time()
            result = service.generate(gen_config, progress_callback=progress_callback)
            gen_duration = time.time() - gen_start

            # Display results with improved formatting
            output.blank_line()
            if result.success:
                output.success(f"Generation completed in {gen_duration:.1f}s")
            else:
                output.error(f"Generation failed in {gen_duration:.1f}s")

            if output_format == "json":
                import json as json_mod

                from snowflake_semantic_tools._version import __version__ as sst_version

                success_count = result.views_created if hasattr(result, "views_created") else 0
                failed_count = len(result.errors) if hasattr(result, "errors") else 0

                diagnostics = []
                if hasattr(result, "errors") and result.errors:
                    for err in result.errors:
                        diagnostics.append({"severity": "error", "message": str(err), "code": "SST-G001"})

                json_output = {
                    "tool": "sst",
                    "version": sst_version,
                    "schema_version": 1,
                    "command": "generate",
                    "status": "success" if result.success else "error",
                    "exit_code": 0 if result.success else 1,
                    "duration_s": round(gen_duration, 2),
                    "diagnostics": diagnostics,
                    "summary": {
                        "errors": failed_count,
                        "warnings": 0,
                        "views_created": success_count,
                    },
                }
                click.echo(json_mod.dumps(json_output, indent=2))
            else:
                # Show detailed summary
                result.print_summary()

                # If dry-run, write SQL files to output directory
                if dry_run and result.sql_statements:
                    output_path = Path(output_dir or "target/semantic_views")
                    try:
                        output_path.mkdir(parents=True, exist_ok=True)

                        for view_name, sql in result.sql_statements.items():
                            safe_name = view_name.replace("/", "_").replace("\\", "_")
                            sql_file = output_path / f"{safe_name}.sql"
                            sql_file.write_text(sql, encoding="utf-8")

                        output.blank_line()
                        output.success(
                            f"Generated SQL written to {output_path.resolve()}/ "
                            f"({len(result.sql_statements)} file(s))"
                        )
                    except OSError as e:
                        output.blank_line()
                        output.error(f"SST-G006: Failed to write SQL files to {output_path}: {e}")
                elif dry_run:
                    output.blank_line()
                    output.warning("Dry-run produced no SQL statements (check for errors above)")

                # Show dbt-style done line with view counts
                output.blank_line()
                success_count = result.views_created if hasattr(result, "views_created") else 0
                failed_count = len(result.errors) if hasattr(result, "errors") else 0
                total_count = success_count + failed_count

                output.done_line(passed=success_count, errored=failed_count, total=total_count)

            if not result.success:
                raise click.ClickException("Generation failed - see errors above")

    except KeyboardInterrupt:
        output.blank_line()
        output.warning("Generation interrupted by user (Ctrl+C)")
        raise SystemExit(130)
    except click.ClickException:
        raise
    except Exception as e:
        output.blank_line()
        output.error(f"Generation error: {str(e)}")
        if verbose:
            traceback.print_exc()
        raise click.ClickException(str(e))
