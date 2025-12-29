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

Performance Notes:
- Issue #10: Commands are lazily loaded to keep `sst --version` fast (<100ms)
- Issue #31: Config validation is skipped for --help to avoid errors when exploring CLI
"""

import os
import sys

import click

# Issue #10: Package __init__.py now uses lazy imports, so this is fast
from snowflake_semantic_tools._version import __version__


def _is_help_or_version_request() -> bool:
    """Check if user is requesting help or version (no config validation needed)."""
    return "--help" in sys.argv or "-h" in sys.argv or "--version" in sys.argv


def _load_env():
    """Load environment variables from .env file in current directory."""
    # Only load from current working directory to respect user's environment
    # This ensures that when running SST from a dbt repo, we use that repo's credentials
    from dotenv import load_dotenv

    cwd_env = os.path.join(os.getcwd(), ".env")
    if os.path.exists(cwd_env):
        load_dotenv(cwd_env, override=True)


# Issue #10: Lazy command loading for fast --version
# Commands are imported only when actually invoked, not at module load time
class LazyCommand(click.Command):
    """Lazily load command module only when the command is invoked."""

    def __init__(self, name, import_path, command_name):
        super().__init__(name, callback=None)
        self._import_path = import_path
        self._command_name = command_name
        self._loaded_command = None

    def _load_command(self):
        if self._loaded_command is None:
            import importlib

            module = importlib.import_module(self._import_path)
            self._loaded_command = getattr(module, self._command_name)
        return self._loaded_command

    def invoke(self, ctx):
        return self._load_command().invoke(ctx)

    def get_help(self, ctx):
        return self._load_command().get_help(ctx)

    def get_short_help_str(self, limit=150):
        return self._load_command().get_short_help_str(limit)

    def get_params(self, ctx):
        return self._load_command().get_params(ctx)

    @property
    def params(self):
        return self._load_command().params


class LazyGroup(click.Group):
    """Click group with lazy command loading."""

    def __init__(self, *args, lazy_commands=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._lazy_commands = lazy_commands or {}

    def list_commands(self, ctx):
        return sorted(self._lazy_commands.keys())

    def get_command(self, ctx, name):
        if name in self._lazy_commands:
            import_path, command_name = self._lazy_commands[name]
            import importlib

            module = importlib.import_module(import_path)
            return getattr(module, command_name)
        return None


# Define lazy command mappings (module path, command function name)
LAZY_COMMANDS = {
    "enrich": ("snowflake_semantic_tools.interfaces.cli.commands.enrich", "enrich"),
    "format": ("snowflake_semantic_tools.interfaces.cli.commands.format", "format_cmd"),
    "extract": ("snowflake_semantic_tools.interfaces.cli.commands.extract", "extract"),
    "validate": ("snowflake_semantic_tools.interfaces.cli.commands.validate", "validate"),
    "generate": ("snowflake_semantic_tools.interfaces.cli.commands.generate", "generate"),
    "deploy": ("snowflake_semantic_tools.interfaces.cli.commands.deploy", "deploy"),
}


@click.group(cls=LazyGroup, lazy_commands=LAZY_COMMANDS)
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
    # Issue #31: Skip config validation for --help and --version requests
    # This allows users to explore CLI without needing valid config
    if _is_help_or_version_request():
        return

    # Load .env only when actually running commands
    _load_env()

    # Setup events early so config validation messages appear correctly
    from snowflake_semantic_tools.shared.events import setup_events
    from snowflake_semantic_tools.shared.utils.logger import get_logger

    setup_events(verbose=False, show_timestamps=False)

    # Validate config (warns on missing optional, errors on missing required)
    # Note: We use fail_on_errors=False here because some commands might handle it differently
    # Individual commands can call validate_and_report_config with fail_on_errors=True
    try:
        from snowflake_semantic_tools.shared.config import get_config
        from snowflake_semantic_tools.shared.config_validator import validate_and_report_config

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


if __name__ == "__main__":
    cli()
