"""
Defer Module

Centralized defer logic for CLI commands. Ensures consistent behavior
across extract, generate, and deploy commands.

This module handles:
- Defer configuration resolution from CLI flags and config files
- Manifest path resolution with dbt Core vs Cloud CLI support
- Explicit error handling for unsupported dbt Cloud CLI features
- Consistent display of defer information to users

Key Design Principle: No silent failures. If a feature isn't supported
for dbt Cloud CLI, we raise an explicit error rather than silently ignoring.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import click

from snowflake_semantic_tools.infrastructure.dbt import DbtClient, DbtType
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DeferConfig:
    """
    Resolved defer configuration from CLI flags and sst_config.yml.

    This dataclass represents the fully resolved defer settings after
    considering CLI flags, config file, and defaults.
    """

    enabled: bool
    target: Optional[str] = None
    state_path: Optional[Path] = None
    manifest_path: Optional[Path] = None  # Resolved path to manifest.json
    only_modified: bool = False

    # Source of the configuration for informational display
    source: str = "none"  # "cli", "config", or "none"

    # Warning if manifest may not match defer target
    manifest_target_warning: Optional[str] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.only_modified and not self.enabled:
            raise ValueError("only_modified requires defer to be enabled")


# =============================================================================
# Configuration Resolution
# =============================================================================


def resolve_defer_config(
    defer_target: Optional[str] = None,
    state_path: Optional[str] = None,
    no_defer: bool = False,
    only_modified: bool = False,
    project_dir: Optional[Path] = None,
) -> DeferConfig:
    """
    Resolve defer configuration from CLI flags and config file.

    Used by: generate, deploy commands

    Priority (highest to lowest):
    1. --no-defer flag (disables everything)
    2. CLI flags (--defer-target, --state)
    3. Config file (defer.target, defer.state_path)

    Args:
        defer_target: Target name for defer (from --defer-target flag)
        state_path: Path to state artifacts (from --state flag)
        no_defer: If True, disable defer entirely (from --no-defer flag)
        only_modified: Only process changed models (from --only-modified flag)
        project_dir: dbt project directory

    Returns:
        DeferConfig with resolved settings

    Raises:
        click.ClickException: For invalid combinations or missing artifacts
    """
    project_dir = project_dir or Path.cwd()

    # Priority 1: --no-defer disables everything
    if no_defer:
        logger.debug("Defer disabled via --no-defer flag")
        return DeferConfig(enabled=False, source="cli")

    # Get config file settings
    config = get_config()
    config_defer_target = config.get("defer.target")
    config_state_path = config.get("defer.state_path")
    config_auto_compile = config.get("defer.auto_compile", False)

    # Priority 2: CLI flags override config
    effective_target = defer_target or config_defer_target
    effective_state_path = Path(state_path) if state_path else None
    if not effective_state_path and config_state_path:
        effective_state_path = Path(config_state_path)

    # Determine source
    if defer_target or state_path:
        source = "cli"
    elif config_defer_target or config_state_path:
        source = "config"
    else:
        source = "none"

    # If no defer target specified, defer is disabled
    if not effective_target:
        if only_modified:
            raise click.ClickException(
                "--only-modified requires --defer-target (or defer.target in sst_config.yml).\n\n"
                "Selective generation compares your current manifest to the defer manifest\n"
                "to determine which models changed.\n\n"
                "Usage: sst generate --all --defer-target prod --only-modified"
            )
        return DeferConfig(enabled=False, source=source)

    # Validate dbt Cloud CLI compatibility before resolving manifest
    dbt_client = DbtClient(project_dir=project_dir)
    validate_dbt_cloud_cli_compatibility(
        dbt_type=dbt_client.dbt_type,
        defer_target=effective_target,
        auto_compile=config_auto_compile,
        state_path=effective_state_path,
    )

    # Resolve manifest path
    manifest_path, manifest_warning = resolve_defer_manifest(
        defer_target=effective_target,
        state_path=effective_state_path,
        project_dir=project_dir,
        dbt_client=dbt_client,
    )

    return DeferConfig(
        enabled=True,
        target=effective_target,
        state_path=effective_state_path,
        manifest_path=manifest_path,
        only_modified=only_modified,
        source=source,
        manifest_target_warning=manifest_warning,
    )


# =============================================================================
# Manifest Resolution
# =============================================================================


def resolve_defer_manifest(
    defer_target: str,
    state_path: Optional[Path] = None,
    project_dir: Optional[Path] = None,
    dbt_client: Optional[DbtClient] = None,
) -> tuple:
    """
    Resolve path to defer manifest.

    Priority:
    1. Explicit --state path
    2. Config state_path
    3. Auto-detect paths (target_prod/, prod_run_artifacts/, etc.)
    4. For dbt Core only: suggest compile command

    Args:
        defer_target: Target name for defer (e.g., 'prod')
        state_path: Explicit path to state artifacts
        project_dir: dbt project directory
        dbt_client: Optional DbtClient for type detection

    Returns:
        Tuple of (Path to manifest.json, Optional warning message)

    Raises:
        click.ClickException: If manifest cannot be found
    """
    project_dir = project_dir or Path.cwd()

    if dbt_client is None:
        dbt_client = DbtClient(project_dir=project_dir)

    is_cloud_cli = dbt_client.dbt_type == DbtType.CLOUD_CLI

    # Priority 1: Explicit --state flag
    if state_path:
        manifest = state_path / "manifest.json"
        if manifest.exists():
            logger.info(f"Using defer manifest from --state: {manifest}")
            return (manifest, None)
        raise click.ClickException(
            f"Manifest not found at: {manifest}\n\n" f"Make sure the state directory contains a manifest.json file."
        )

    # Priority 2: Auto-detect common paths
    # Prefer target-specific paths first, then fall back to default target/
    target_specific_paths = [
        project_dir / f"target_{defer_target}" / "manifest.json",
        project_dir / f"{defer_target}_run_artifacts" / "manifest.json",
        project_dir / "prod_run_artifacts" / "manifest.json",
        project_dir / "artifacts" / defer_target / "manifest.json",
    ]

    # Check target-specific paths first
    for path in target_specific_paths:
        if path.exists():
            logger.info(f"Auto-detected defer manifest: {path}")
            return (path, None)

    # Fall back to default target/ - but warn that it may not match defer target
    default_manifest = project_dir / "target" / "manifest.json"
    if default_manifest.exists():
        if is_cloud_cli:
            warning_msg = (
                f"Using manifest from ./target/ for defer target '{defer_target}'. "
                f"This manifest may have been compiled with a different target. "
                f"For accurate defer with dbt Cloud CLI, download prod artifacts from dbt Cloud "
                f"and use --state flag."
            )
        else:
            warning_msg = (
                f"Using manifest from ./target/ for defer target '{defer_target}'. "
                f"This manifest may have been compiled with a different target. "
                f"For accurate defer, run: dbt compile --target {defer_target}"
            )
        logger.warning(warning_msg)
        return (default_manifest, warning_msg)

    search_paths = target_specific_paths + [default_manifest]

    # Format search paths for error message
    searched_str = "\n".join(f"  - {p}" for p in search_paths)

    # Priority 3: Error with helpful guidance
    if is_cloud_cli:
        raise click.ClickException(
            f"Defer manifest not found for target '{defer_target}'.\n\n"
            f"Searched:\n{searched_str}\n\n"
            f"You're using dbt Cloud CLI, which cannot compile with a different target locally.\n\n"
            f"Options:\n"
            f"  1. Download artifacts from dbt Cloud:\n"
            f"     - Go to dbt Cloud → Jobs → Select prod job → Artifacts\n"
            f"     - Download manifest.json to ./{defer_target}_run_artifacts/\n\n"
            f"  2. Use CI pipeline artifacts:\n"
            f"     - If your CI saves prod artifacts, point to them:\n"
            f"       --state /path/to/ci/artifacts\n\n"
            f"  3. Add to sst_config.yml:\n"
            f"     defer:\n"
            f"       target: {defer_target}\n"
            f"       state_path: ./{defer_target}_run_artifacts"
        )
    else:
        raise click.ClickException(
            f"Defer manifest not found for target '{defer_target}'.\n\n"
            f"Searched:\n{searched_str}\n\n"
            f"Since you're using dbt Core, you can generate it:\n"
            f"  dbt compile --target {defer_target}\n\n"
            f"Or specify a path: --state /path/to/artifacts"
        )


# =============================================================================
# dbt Cloud CLI Compatibility
# =============================================================================


def validate_dbt_cloud_cli_compatibility(
    dbt_type: DbtType,
    defer_target: Optional[str] = None,
    auto_compile: bool = False,
    state_path: Optional[Path] = None,
) -> None:
    """
    Validate that requested features are compatible with dbt Cloud CLI.

    Raises explicit errors instead of silently ignoring flags.
    This ensures users get clear feedback when a feature won't work
    as expected with dbt Cloud CLI.

    Used by: All commands that interact with dbt

    Args:
        dbt_type: Type of dbt installation (Core, Cloud CLI, Unknown)
        defer_target: Defer target if specified
        auto_compile: Whether auto_compile is enabled in config
        state_path: Explicit state path if provided

    Raises:
        click.ClickException: For unsupported features with clear guidance
    """
    # Only validate for dbt Cloud CLI
    if dbt_type != DbtType.CLOUD_CLI:
        return

    # Check for auto_compile setting (not supported with Cloud CLI)
    if auto_compile:
        raise click.ClickException(
            "defer.auto_compile is not supported with dbt Cloud CLI.\n\n"
            "Your sst_config.yml has:\n"
            "  defer:\n"
            "    auto_compile: true\n\n"
            "But dbt Cloud CLI cannot compile with different targets locally.\n"
            "Remove auto_compile or set it to false, and provide a state_path instead:\n"
            "  defer:\n"
            "    target: prod\n"
            "    state_path: ./prod_run_artifacts\n"
            "    auto_compile: false  # or remove this line"
        )

    # If defer target is set but no state path, warn that auto-compile won't work
    # (The actual error will be raised by resolve_defer_manifest if manifest not found)
    if defer_target and not state_path:
        logger.debug(
            f"dbt Cloud CLI detected with --defer-target {defer_target} but no --state. "
            f"Will search for existing manifest; cannot auto-compile."
        )


# =============================================================================
# Display Helpers
# =============================================================================


def display_defer_info(output: CLIOutput, defer_config: DeferConfig) -> None:
    """
    Display defer configuration to user in consistent format.

    Used by: generate, deploy commands

    Args:
        output: CLIOutput instance for formatted output
        defer_config: Resolved defer configuration
    """
    if not defer_config.enabled:
        return

    output.info(f"Defer mode enabled (source: {defer_config.source})")

    if defer_config.target:
        output.info(f"  Defer target: {defer_config.target}", indent=1)

    if defer_config.manifest_path:
        output.info(f"  Using manifest: {defer_config.manifest_path}", indent=1)

    if defer_config.only_modified:
        output.info("  Selective generation: only modified models", indent=1)

    # Show warning if manifest may not match defer target
    if defer_config.manifest_target_warning:
        output.warning(defer_config.manifest_target_warning)


def get_defer_summary(defer_config: DeferConfig) -> Dict[str, Any]:
    """
    Get a dictionary summary of defer configuration for logging/debugging.

    Args:
        defer_config: Resolved defer configuration

    Returns:
        Dictionary with defer settings
    """
    return {
        "enabled": defer_config.enabled,
        "target": defer_config.target,
        "state_path": str(defer_config.state_path) if defer_config.state_path else None,
        "manifest_path": str(defer_config.manifest_path) if defer_config.manifest_path else None,
        "only_modified": defer_config.only_modified,
        "source": defer_config.source,
    }


# =============================================================================
# Modified View Detection
# =============================================================================


def get_modified_views_filter(
    defer_config: DeferConfig,
    available_views: list,
    output: Optional[CLIOutput] = None,
) -> Optional[list]:
    """
    Get list of view names that should be regenerated based on model changes.

    This function compares the current manifest with the defer manifest to identify
    which models have changed, then filters the available views to only those
    that reference changed models.

    Args:
        defer_config: Resolved defer configuration (must have only_modified=True)
        available_views: List of dicts with 'name' and 'tables' keys from metadata
        output: Optional CLIOutput for user feedback

    Returns:
        List of view names to regenerate, or None if all should be regenerated
        Empty list means no views need regeneration (all up to date)
    """
    if not defer_config.only_modified:
        return None

    if not defer_config.manifest_path:
        if output:
            output.warning("Cannot determine modified models: no defer manifest available")
        return None

    try:
        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        # Load current manifest (from ./target/)
        current_manifest = ManifestParser()
        if not current_manifest.load():
            if output:
                output.warning("Could not load current manifest for comparison")
            return None

        # Load defer manifest
        defer_manifest = ManifestParser(defer_config.manifest_path)
        if not defer_manifest.load():
            if output:
                output.warning("Could not load defer manifest for comparison")
            return None

        # Compare manifests
        diff = current_manifest.compare_to(defer_manifest)

        if diff.total_changes == 0:
            if output:
                output.info("No model changes detected - all views up to date")
            return []

        changed_models = set(m.lower() for m in diff.changed)
        if output:
            output.info(f"Detected {len(changed_models)} changed model(s): {', '.join(sorted(changed_models))}")

        # Filter views to only those referencing changed models
        views_to_regenerate = []
        for view in available_views:
            view_name = view.get("name") or view.get("NAME")
            tables_data = view.get("tables") or view.get("TABLES") or ""

            # Parse tables - could be JSON array string, list, or comma-separated
            tables = []
            if isinstance(tables_data, str):
                # Try JSON first
                try:
                    import json

                    parsed = json.loads(tables_data)
                    if isinstance(parsed, list):
                        tables = [str(t).lower() for t in parsed]
                    else:
                        tables = [tables_data.lower()]
                except (json.JSONDecodeError, TypeError):
                    # Fall back to comma-separated
                    tables = [t.strip().lower() for t in tables_data.split(",")]
            elif isinstance(tables_data, list):
                tables = [str(t).lower() for t in tables_data]

            # Check if any table in this view matches a changed model
            for table in tables:
                # Handle fully qualified names (DATABASE.SCHEMA.TABLE)
                simple_name = table.split(".")[-1].lower()
                if simple_name in changed_models:
                    views_to_regenerate.append(view_name)
                    break

        if output:
            if views_to_regenerate:
                output.info(f"Will regenerate {len(views_to_regenerate)} view(s) referencing changed models")
            else:
                output.info("No views reference the changed models")

        return views_to_regenerate

    except Exception as e:
        logger.warning(f"Error determining modified views: {e}")
        if output:
            output.warning(f"Could not filter views by modification: {e}")
        return None
