"""
Main CLI Module

Central command-line interface orchestrator for Snowflake Semantic Tools.

Provides the main `sst` command group that organizes all subcommands and
handles global configuration like environment variable loading and version
management. Uses Click framework for robust command-line parsing and help
generation.

The CLI automatically loads .env files for configuration, making it easy
to manage different environments without exposing credentials in scripts
or command history.
"""

# Load environment variables from .env files
# IMPORTANT: Only load from current working directory to respect user's environment
# This ensures that when running SST from a dbt repo, we use that repo's credentials
# Use explicit path to current working directory to avoid searching parent directories
import os
from pathlib import Path

import click
from dotenv import load_dotenv

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.commands import deploy, enrich, extract, format, generate, validate

cwd_env = os.path.join(os.getcwd(), ".env")
if os.path.exists(cwd_env):
    load_dotenv(cwd_env, override=True)
else:
    # No .env in current directory - user must provide env vars another way
    pass


@click.group()
@click.version_option(version=__version__, prog_name="snowflake-semantic-tools")
def cli():
    """
    Snowflake Semantic Tools - Semantic Model Management for Snowflake using dbt

    This toolkit provides comprehensive semantic modeling capabilities:

    \b
    - ENRICH: Automatically populate dbt YAML metadata with semantic information
    - FORMAT: Standardize YAML file structure and formatting
    - VALIDATE: Check semantic models against dbt definitions
    - EXTRACT: Parse and load semantic metadata to Snowflake
    - GENERATE: Create Snowflake semantic views and/or YAML models
    - DEPLOY: One-step validate → extract → generate workflow

    Use --help with any command for detailed options.

    Note: All commands validate configuration automatically using sst_config.yml.
    Missing required fields will cause commands to exit with an error.
    """
    # Validate config for all commands
    # This runs once when CLI group is initialized
    from snowflake_semantic_tools.shared.config import get_config
    from snowflake_semantic_tools.shared.config_validator import validate_and_report_config

    # Setup events early so config validation messages appear correctly
    from snowflake_semantic_tools.shared.events import setup_events
    from snowflake_semantic_tools.shared.utils.logger import get_logger

    setup_events(verbose=False, show_timestamps=False)

    # Validate config (warns on missing optional, errors on missing required)
    # Note: We use fail_on_errors=False here because some commands might handle it differently
    # Individual commands can call validate_and_report_config with fail_on_errors=True
    try:
        config = get_config()
        config_path = config._find_config_file() if hasattr(config, "_find_config_file") else None
        validate_and_report_config(
            config._config if hasattr(config, "_config") else {},
            config_path=config_path,
            fail_on_errors=False,  # Let individual commands decide to fail
        )
    except Exception as e:
        # Don't block CLI initialization if config validation fails
        # Individual commands will handle this
        logger = get_logger(__name__)
        logger.debug(f"Config validation during CLI init: {e}")
    pass


# Add commands to the CLI group
cli.add_command(enrich.enrich, name="enrich")
cli.add_command(format.format_cmd, name="format")
cli.add_command(extract.extract, name="extract")
cli.add_command(validate.validate, name="validate")
cli.add_command(generate.generate, name="generate")
cli.add_command(deploy.deploy, name="deploy")


if __name__ == "__main__":
    cli()
