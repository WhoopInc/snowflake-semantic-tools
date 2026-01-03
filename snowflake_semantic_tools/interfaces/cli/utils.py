"""
CLI Utilities

Shared utilities for CLI commands to ensure consistency.
Reduces boilerplate and ensures all commands follow the same patterns.

Authentication is handled via dbt's ~/.dbt/profiles.yml file.
"""

import logging
from pathlib import Path
from typing import Optional

import click

from snowflake_semantic_tools.infrastructure.dbt import DbtClient, DbtProfileNotFoundError, DbtType
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.shared.config_validator import validate_cli_config
from snowflake_semantic_tools.shared.events import setup_events


def setup_command(verbose: bool = False, quiet: bool = False, validate_config: bool = True) -> None:
    """
    Common setup for all CLI commands.

    Performs standard initialization:
    1. Setup event system
    2. Validate sst_config.yaml (optional)
    3. Set logging level

    Args:
        verbose: Enable verbose logging
        quiet: Suppress non-error output
        validate_config: If True, validates sst_config.yaml exists
    """
    # Step 1: Setup events
    setup_events(verbose=verbose, quiet=quiet, show_timestamps=True)

    # Step 2: Validate config (if needed)
    if validate_config:
        validate_cli_config(fail_on_errors=True)

    # Step 3: Set logging level
    if verbose:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.DEBUG)
    elif quiet:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.ERROR)
    else:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.WARNING)


def build_snowflake_config(
    target: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    project_dir: Optional[Path] = None,
    verbose: bool = False,
) -> SnowflakeConfig:
    """
    Build SnowflakeConfig from dbt profiles.yml.

    Reads connection parameters from ~/.dbt/profiles.yml using the profile
    name specified in dbt_project.yml.

    Args:
        target: dbt target name (e.g., 'dev', 'prod'). Uses default if not specified.
        database: Override database from profile (for --database flag)
        schema: Override schema from profile (for --schema flag)
        project_dir: Path to dbt project directory. Defaults to current dir.
        verbose: Enable verbose logging

    Returns:
        Configured SnowflakeConfig instance

    Raises:
        click.ClickException: If profile cannot be loaded with helpful error message
    """
    try:
        return SnowflakeConfig.from_dbt_profile(
            target=target,
            project_dir=project_dir,
            database_override=database,
            schema_override=schema,
            verbose=verbose,
        )
    except DbtProfileNotFoundError as e:
        # Detect dbt type for contextual error message
        try:
            dbt_client = DbtClient()
            dbt_type = dbt_client.dbt_type
        except Exception:
            dbt_type = DbtType.UNKNOWN

        if dbt_type == DbtType.CLOUD_CLI:
            raise click.ClickException(
                "dbt Cloud CLI detected but no profiles.yml found.\n\n"
                "SST requires direct Snowflake access via profiles.yml.\n"
                "dbt Cloud doesn't expose credentials locally.\n\n"
                "To fix: Create ~/.dbt/profiles.yml with your Snowflake credentials.\n\n"
                "See: https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup\n\n"
                "For step-by-step instructions for dbt Cloud users, see:\n"
                "https://github.com/WhoopInc/snowflake-semantic-tools/blob/main/docs/authentication.md"
            )
        else:
            raise click.ClickException(str(e))
    except Exception as e:
        # Re-raise profile parse errors with their messages
        raise click.ClickException(str(e))
