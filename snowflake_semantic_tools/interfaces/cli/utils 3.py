"""
CLI Utilities

Shared utilities for CLI commands to ensure consistency.
Reduces boilerplate and ensures all commands follow the same patterns.
"""

import logging
import os
from pathlib import Path
from typing import Optional

import click

from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.shared.config_validator import validate_cli_config
from snowflake_semantic_tools.shared.events import setup_events


def load_environment(verbose: bool = False) -> None:
    """
    Load environment variables from .env file.

    Ensures all CLI commands have access to credentials and configuration
    from .env files. Loads from current directory.

    Args:
        verbose: If True, logs which .env file was loaded
    """
    try:
        from dotenv import load_dotenv

        # Load from current directory
        env_path = Path.cwd() / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            if verbose:
                click.echo(f"Loaded environment from: {env_path}")
        else:
            # Try parent directories (up to 3 levels)
            for parent in [Path.cwd().parent, Path.cwd().parent.parent, Path.cwd().parent.parent.parent]:
                env_path = parent / ".env"
                if env_path.exists():
                    load_dotenv(dotenv_path=env_path)
                    if verbose:
                        click.echo(f"Loaded environment from: {env_path}")
                    break
    except ImportError:
        # python-dotenv not installed (optional dependency)
        pass
    except Exception as e:
        # Don't fail if .env loading fails
        if verbose:
            click.echo(f"Warning: Could not load .env: {e}")


def setup_command(verbose: bool = False, quiet: bool = False, validate_config: bool = True) -> None:
    """
    Common setup for all CLI commands.

    Performs standard initialization:
    1. Load environment variables
    2. Setup event system
    3. Validate sst_config.yaml (optional)
    4. Set logging level

    Args:
        verbose: Enable verbose logging
        quiet: Suppress non-error output
        validate_config: If True, validates sst_config.yaml exists
    """
    # Step 1: Load environment
    load_environment(verbose=verbose)

    # Step 2: Setup events
    setup_events(verbose=verbose, quiet=quiet, show_timestamps=True)

    # Step 3: Validate config (if needed)
    if validate_config:
        validate_cli_config(fail_on_errors=True)

    # Step 4: Set logging level
    if verbose:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.DEBUG)
    elif quiet:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.ERROR)
    else:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.WARNING)


def build_snowflake_config(
    account: Optional[str] = None,
    user: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    verbose: bool = False,
) -> SnowflakeConfig:
    """
    Build SnowflakeConfig from CLI options and environment variables.

    Common pattern used by enrich, extract, generate, and deploy commands.

    Args:
        account: Snowflake account (or from env)
        user: Snowflake user (or from env)
        role: Snowflake role (or from env)
        warehouse: Snowflake warehouse (or from env)
        database: Target database
        schema: Target schema
        verbose: Enable verbose logging

    Returns:
        Configured SnowflakeConfig instance
    """
    # Detect authentication method (centralized logic)
    password, private_key_path, authenticator = SnowflakeConfig.detect_auth_method(verbose=verbose)

    # Support both SNOWFLAKE_USER and SNOWFLAKE_USERNAME (dbt uses USERNAME)
    env_user = os.getenv("SNOWFLAKE_USER") or os.getenv("SNOWFLAKE_USERNAME")

    # Build config with fallbacks to env vars and prompts
    return SnowflakeConfig(
        account=account or os.getenv("SNOWFLAKE_ACCOUNT") or click.prompt("Snowflake account"),
        user=user or env_user or click.prompt("Snowflake user"),
        role=role or os.getenv("SNOWFLAKE_ROLE") or click.prompt("Snowflake role"),
        warehouse=warehouse or os.getenv("SNOWFLAKE_WAREHOUSE") or click.prompt("Snowflake warehouse"),
        database=database,
        schema=schema,
        password=password,
        private_key_path=private_key_path,
        authenticator=authenticator,
    )
