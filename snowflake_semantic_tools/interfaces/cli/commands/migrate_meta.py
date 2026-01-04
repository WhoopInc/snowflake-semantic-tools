"""
Migrate Meta Command

Migrates dbt YAML files from legacy meta.sst format to the new config.meta.sst
format required by dbt Fusion.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import click
from ruamel.yaml import YAML, YAMLError

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.shared.config_validator import validate_cli_config
from snowflake_semantic_tools.shared.events import setup_events
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


def _migrate_node_meta(node: Dict[str, Any], node_type: str) -> Tuple[bool, List[str]]:
    """
    Migrate a single node (model or column) from meta.sst to config.meta.sst.

    Args:
        node: The node dictionary to migrate
        node_type: 'model' or 'column' for logging

    Returns:
        Tuple of (was_migrated, list of migration notes)
    """
    notes = []

    # Check if legacy meta.sst exists
    if "meta" not in node or not isinstance(node.get("meta"), dict):
        return False, notes

    meta = node["meta"]

    # Check for sst or genie
    old_sst = meta.get("sst", meta.get("genie", {}))
    if not old_sst:
        return False, notes

    # Record what we're migrating from
    source = "meta.genie" if "genie" in meta else "meta.sst"
    notes.append(f"Migrating {node_type} from {source} to config.meta.sst")

    # Ensure config.meta.sst structure exists
    if "config" not in node:
        node["config"] = {}
    if "meta" not in node["config"]:
        node["config"]["meta"] = {}
    if "sst" not in node["config"]["meta"]:
        node["config"]["meta"]["sst"] = {}

    # Merge old values into new location (preserve any existing new values)
    for key, value in old_sst.items():
        if key not in node["config"]["meta"]["sst"]:
            node["config"]["meta"]["sst"][key] = value

    # Remove old location
    if "genie" in meta:
        del meta["genie"]
    if "sst" in meta:
        del meta["sst"]

    # Clean up empty meta
    if not meta:
        del node["meta"]

    return True, notes


def _migrate_yaml_file(file_path: Path, dry_run: bool = False, backup: bool = False) -> Dict[str, Any]:
    """
    Migrate a single YAML file from meta.sst to config.meta.sst format.

    Args:
        file_path: Path to the YAML file
        dry_run: If True, don't write changes
        backup: If True, create a .bak file before modifying

    Returns:
        Dict with migration results
    """
    result = {
        "file": str(file_path),
        "status": "unchanged",
        "models_migrated": 0,
        "columns_migrated": 0,
        "notes": [],
        "error": None,
    }

    try:
        # Setup ruamel.yaml to preserve formatting
        yaml = YAML()
        yaml.preserve_quotes = True
        yaml.map_indent = 2
        yaml.sequence_indent = 4
        yaml.sequence_dash_offset = 2
        yaml.width = 4096

        # Read the file
        with open(file_path, "r", encoding="utf-8") as f:
            content = yaml.load(f)

        if not content:
            return result

        # Check for models
        models = content.get("models", [])
        if not models:
            return result

        any_changes = False

        # Process each model
        for model in models:
            if not isinstance(model, dict):
                continue

            model_name = model.get("name", "unknown")

            # Migrate model-level meta
            migrated, notes = _migrate_node_meta(model, f"model '{model_name}'")
            if migrated:
                any_changes = True
                result["models_migrated"] += 1
                result["notes"].extend(notes)

            # Migrate column-level meta
            columns = model.get("columns", [])
            for column in columns:
                if not isinstance(column, dict):
                    continue

                col_name = column.get("name", "unknown")
                migrated, notes = _migrate_node_meta(column, f"column '{model_name}.{col_name}'")
                if migrated:
                    any_changes = True
                    result["columns_migrated"] += 1
                    result["notes"].extend(notes)

        if not any_changes:
            return result

        result["status"] = "migrated" if not dry_run else "would_migrate"

        if not dry_run:
            # Create backup if requested
            if backup:
                backup_path = file_path.with_suffix(file_path.suffix + ".bak")
                with open(backup_path, "w", encoding="utf-8") as f:
                    with open(file_path, "r", encoding="utf-8") as original:
                        f.write(original.read())
                result["notes"].append(f"Backup created: {backup_path}")

            # Write the migrated content
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(content, f)

    except YAMLError as e:
        result["status"] = "error"
        result["error"] = f"YAML parsing error: {e}"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result


def _discover_yaml_files(path: Path, recursive: bool = True) -> List[Path]:
    """
    Discover YAML files in a path.

    Args:
        path: File or directory path
        recursive: If True, search recursively

    Returns:
        List of YAML file paths
    """
    yaml_files = []

    if path.is_file():
        if path.suffix in [".yml", ".yaml"]:
            yaml_files.append(path)
    elif path.is_dir():
        pattern = "**/*.yml" if recursive else "*.yml"
        yaml_files.extend(path.glob(pattern))
        pattern = "**/*.yaml" if recursive else "*.yaml"
        yaml_files.extend(path.glob(pattern))

    return sorted(yaml_files)


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Preview changes without modifying files")
@click.option("--backup", is_flag=True, help="Create .bak backup files before modifying")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed migration notes")
def migrate_meta(path: str, dry_run: bool, backup: bool, verbose: bool):
    """
    Migrate dbt YAML files from meta.sst to config.meta.sst format.

    This command migrates SST metadata from the legacy location (meta.sst)
    to the new dbt Fusion compatible location (config.meta.sst).

    IMPORTANT: This migration is required for dbt Fusion compatibility.
    Column-level meta: will cause errors in dbt Fusion if not migrated.

    PATH: File or directory to migrate

    Examples:

        # Preview migration for a directory
        sst migrate-meta models/ --dry-run

        # Migrate all YAML files in a directory
        sst migrate-meta models/

        # Migrate with backups
        sst migrate-meta models/ --backup

        # Migrate a single file
        sst migrate-meta models/analytics/users/users.yml
    """
    # IMMEDIATE OUTPUT
    output = CLIOutput(verbose=verbose, quiet=False)
    output.info(f"Running with sst={__version__}")

    # Setup event system for clean CLI output
    setup_events(verbose=verbose, show_timestamps=True)

    # Validate config (uses events for user-facing messages)
    validate_cli_config(fail_on_errors=True)

    try:
        target_path = Path(path)
        yaml_files = _discover_yaml_files(target_path)

        if not yaml_files:
            output.warning(f"No YAML files found in {path}")
            return

        output.blank_line()
        if dry_run:
            output.info(f"[DRY RUN] Scanning {len(yaml_files)} YAML file(s)...")
        else:
            output.info(f"Migrating {len(yaml_files)} YAML file(s)...")
        output.blank_line()

        # Process files
        total_models = 0
        total_columns = 0
        files_migrated = 0
        files_with_errors = 0

        for yaml_file in yaml_files:
            result = _migrate_yaml_file(yaml_file, dry_run=dry_run, backup=backup)

            if result["status"] == "error":
                files_with_errors += 1
                click.echo(f"  [ERROR] {yaml_file}: {result['error']}", err=True)
            elif result["status"] in ["migrated", "would_migrate"]:
                files_migrated += 1
                total_models += result["models_migrated"]
                total_columns += result["columns_migrated"]

                status = "[WOULD MIGRATE]" if dry_run else "[MIGRATED]"
                click.echo(
                    f"  {status} {yaml_file} "
                    f"({result['models_migrated']} model(s), {result['columns_migrated']} column(s))"
                )

                if verbose:
                    for note in result["notes"]:
                        click.echo(f"    - {note}")

        # Summary
        output.blank_line()
        if dry_run:
            if files_migrated > 0:
                click.echo(
                    f"[DRY RUN] Would migrate {files_migrated} file(s): "
                    f"{total_models} model(s), {total_columns} column(s)"
                )
            else:
                click.echo(f"[DRY RUN] No files need migration (all {len(yaml_files)} file(s) are up to date)")
        else:
            if files_migrated > 0:
                click.echo(
                    f"[SUCCESS] Migrated {files_migrated} file(s): "
                    f"{total_models} model(s), {total_columns} column(s)"
                )
            else:
                click.echo(f"[SUCCESS] No files needed migration (all {len(yaml_files)} file(s) are up to date)")

        if files_with_errors > 0:
            click.echo(f"[WARNING] {files_with_errors} file(s) had errors", err=True)
            exit(1)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        click.echo(f"\n[ERROR] {str(e)}", err=True)
        exit(1)

