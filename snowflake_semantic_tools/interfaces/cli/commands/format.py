"""
Format Command

Formats dbt YAML files to ensure consistent structure, ordering, and style.
"""

from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.services.format_yaml import FormattingConfig, YAMLFormattingService
from snowflake_semantic_tools.services.sanitize_yaml import YAMLSanitizationService
from snowflake_semantic_tools.shared.config_validator import validate_cli_config
from snowflake_semantic_tools.shared.events import setup_events
from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.file_utils import expand_path_pattern

logger = get_logger(__name__)


@click.command()
@click.argument("path", type=click.Path())
@click.option("--dry-run", is_flag=True, help="Preview changes without modifying files")
@click.option("--check", is_flag=True, help="Check if files need formatting (exit code 1 if changes needed)")
@click.option(
    "--force", is_flag=True, help="Always write files, even if content appears unchanged (useful for IDE cache issues)"
)
@click.option(
    "--sanitize",
    is_flag=True,
    help="Sanitize problematic characters (apostrophes in synonyms/sample_values, Jinja in descriptions)",
)
def format_cmd(path: str, dry_run: bool, check: bool, force: bool, sanitize: bool):
    """
    Format dbt YAML files for consistency.

    Applies standardized formatting to dbt model YAML files:
    - Standardizes field ordering
    - Removes excessive blank lines
    - Ensures consistent indentation
    - Formats multi-line descriptions

    PATH: File or directory to format

    Examples:

        # Format a single file
        sst format models/analytics/users/users.yml

        # Format all files in a directory
        sst format models/analytics/

        # Preview changes without modifying files
        sst format models/ --dry-run

        # Check if formatting is needed (CI/CD)
        sst format models/ --check

        # Force write even if content appears unchanged
        sst format models/ --force
    """
    # IMMEDIATE OUTPUT
    output = CLIOutput(verbose=False, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Setup event system for clean CLI output
    setup_events(verbose=False, show_timestamps=True)

    # Validate config (uses events for user-facing messages)
    validate_cli_config(fail_on_errors=True)

    try:
        # Expand wildcards in path if present
        expanded_paths = expand_path_pattern(path)
        if not expanded_paths:
            raise click.ClickException(f"No files found matching pattern '{path}'")

        # If sanitize flag provided, run sanitization first
        if sanitize:
            output.blank_line()
            output.info("Sanitizing YAML files...")
            sanitizer = YAMLSanitizationService()
            for expanded_path in expanded_paths:
                sanitize_result = sanitizer.sanitize_directory(expanded_path, dry_run=dry_run)
                sanitizer.print_summary(sanitize_result, dry_run=dry_run)

            if dry_run:
                # In dry-run, stop here (don't format)
                return

        # Continue with formatting
        config = FormattingConfig(dry_run=dry_run, check_only=check, force=force)
        service = YAMLFormattingService(config)

        # Format each expanded path and aggregate results
        total_files_processed = 0
        total_files_formatted = 0
        total_files_needing_formatting = 0
        total_errors = 0

        for expanded_path in expanded_paths:
            result = service.format_path(expanded_path)
            total_files_processed += result.get("files_processed", 0)
            total_files_formatted += result.get("files_formatted", 0)
            total_files_needing_formatting += result.get("files_needing_formatting", 0)
            total_errors += result.get("errors", 0)

        result = {
            "files_processed": total_files_processed,
            "files_formatted": total_files_formatted,
            "files_needing_formatting": total_files_needing_formatting,
            "errors": total_errors,
        }

        # Show results with context
        files_processed = total_files_processed
        files_formatted = total_files_formatted
        files_needing_formatting = total_files_needing_formatting
        errors = total_errors

        if check:
            if files_needing_formatting > 0:
                click.echo(f"\n[FAIL] {files_needing_formatting} of {files_processed} file(s) need formatting")
                exit(1)
            else:
                click.echo(f"\n[PASS] All {files_processed} file(s) are properly formatted")
                exit(0)
        else:
            if dry_run:
                if files_needing_formatting > 0:
                    click.echo(f"\n[DRY RUN] Would format {files_needing_formatting} of {files_processed} file(s)")
                else:
                    click.echo(f"\n[DRY RUN] All {files_processed} file(s) are already formatted correctly")
            else:
                if files_formatted > 0:
                    click.echo(f"\n[SUCCESS] Formatted {files_formatted} of {files_processed} file(s)")
                else:
                    click.echo(f"\n[SUCCESS] All {files_processed} file(s) are already formatted correctly")

                if errors > 0:
                    click.echo(f"[WARNING] {errors} file(s) had errors", err=True)

    except Exception as e:
        logger.error(f"Formatting failed: {e}")
        click.echo(f"\n[ERROR] {str(e)}", err=True)
        exit(1)
