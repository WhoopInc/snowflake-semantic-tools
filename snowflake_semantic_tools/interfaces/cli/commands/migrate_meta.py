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
from ruamel.yaml.comments import CommentedMap

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.shared.config_validator import validate_cli_config
from snowflake_semantic_tools.shared.events import setup_events
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


def _insert_key_after(node: CommentedMap, after_key: str, new_key: str, new_value: Any) -> None:
    """
    Insert a new key-value pair after a specific key in a CommentedMap.
    
    This preserves the YAML key ordering when adding new keys.
    
    Args:
        node: The CommentedMap to modify
        after_key: The key after which to insert
        new_key: The new key to insert
        new_value: The value for the new key
    """
    if not isinstance(node, CommentedMap):
        node[new_key] = new_value
        return
    
    # Find the position of after_key
    keys = list(node.keys())
    if after_key in keys:
        pos = keys.index(after_key) + 1
        node.insert(pos, new_key, new_value)
    else:
        # If after_key not found, try to insert after 'description' or at position 2
        if "description" in keys:
            pos = keys.index("description") + 1
            node.insert(pos, new_key, new_value)
        elif len(keys) >= 2:
            node.insert(2, new_key, new_value)
        else:
            node[new_key] = new_value


def _migrate_node_meta(node: Dict[str, Any], node_type: str, is_model: bool = False) -> Tuple[bool, List[str]]:
    """
    Migrate a single node (model or column) from meta.sst to config.meta.sst.

    Args:
        node: The node dictionary to migrate
        node_type: 'model' or 'column' for logging
        is_model: True if this is a model-level node (affects key positioning)

    Returns:
        Tuple of (was_migrated, list of migration notes)
    """
    notes = []

    # Check if legacy meta.sst exists
    if "meta" not in node or not isinstance(node.get("meta"), dict):
        return False, notes

    meta = node["meta"]
    old_sst = meta.get("sst", {})
    if not old_sst:
        return False, notes

    notes.append(f"Migrating {node_type} from meta.sst to config.meta.sst")

    # Build the new config structure
    new_config_meta_sst = {}
    for key, value in old_sst.items():
        new_config_meta_sst[key] = value

    # Handle config insertion with proper positioning
    if "config" not in node:
        # Create new config with proper structure
        new_config = CommentedMap()
        new_config["meta"] = CommentedMap()
        new_config["meta"]["sst"] = new_config_meta_sst
        
        # Insert config after 'description' for models, or after 'data_tests' for columns
        if is_model:
            _insert_key_after(node, "description", "config", new_config)
        else:
            # For columns, insert after data_tests if present, otherwise after description
            if "data_tests" in node:
                _insert_key_after(node, "data_tests", "config", new_config)
            else:
                _insert_key_after(node, "description", "config", new_config)
    else:
        # config exists, just add/update meta.sst
        if "meta" not in node["config"]:
            node["config"]["meta"] = {}
        if "sst" not in node["config"]["meta"]:
            node["config"]["meta"]["sst"] = {}
        
        # Merge values
        for key, value in old_sst.items():
            if key not in node["config"]["meta"]["sst"]:
                node["config"]["meta"]["sst"][key] = value

    # Remove old location
    del meta["sst"]

    # Clean up empty meta
    if not meta:
        del node["meta"]

    return True, notes


def _ensure_blank_lines(content: Any) -> None:
    """
    Ensure proper blank lines in the YAML structure.
    
    - Blank line before 'columns:' key in each model
    - Blank line before each column entry in the columns list
    
    Args:
        content: The ruamel.yaml content object
    """
    if not content:
        return
        
    models = content.get("models", [])
    if not models or not hasattr(models, "ca"):
        return
    
    for idx, model in enumerate(models):
        if not isinstance(model, CommentedMap):
            continue
            
        # Add blank line before 'columns' key in the model
        if "columns" in model and hasattr(model, "ca"):
            # Add a blank line comment before 'columns' key
            model.yaml_set_comment_before_after_key("columns", before="\n")
        
        # Add blank lines between column entries
        columns = model.get("columns", [])
        if columns and hasattr(columns, "ca"):
            for col_idx in range(1, len(columns)):
                # Add blank line comment before each column (except first)
                columns.yaml_set_comment_before_after_key(col_idx, before="\n")


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
            migrated, notes = _migrate_node_meta(model, f"model '{model_name}'", is_model=True)
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
                migrated, notes = _migrate_node_meta(column, f"column '{model_name}.{col_name}'", is_model=False)
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

            # Ensure proper blank lines formatting
            _ensure_blank_lines(content)

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

