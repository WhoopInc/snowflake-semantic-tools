"""
Main CLI Module

Central command-line interface orchestrator for Snowflake Semantic Tools.

Provides the main `sst` command group that organizes all subcommands and
handles global configuration and version management. Uses Click framework
for robust command-line parsing and help generation.

Authentication is handled via dbt's ~/.dbt/profiles.yml file, aligning
SST with dbt conventions for a single source of truth.

Performance Notes:
- Issue #10: Commands are lazily loaded to keep `sst --version` fast (<100ms)
- Issue #31: Config validation is skipped for --help to avoid errors when exploring CLI
"""

import sys

import click

# Issue #10: Package __init__.py now uses lazy imports, so this is fast
from snowflake_semantic_tools._version import __version__


def _is_help_or_version_request() -> bool:
    """Check if user is requesting help or version (no config validation needed)."""
    return "--help" in sys.argv or "-h" in sys.argv or "--version" in sys.argv


def _is_init_command() -> bool:
    """Check if user is running init command (no config validation needed)."""
    return len(sys.argv) > 1 and sys.argv[1] == "init"


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


COMMAND_ORDER = [
    "init",
    "debug",
    "enrich",
    "format",
    "compile",
    "validate",
    "diff",
    "extract",
    "generate",
    "deploy",
    "drop",
    "list",
    "migrate-meta",
]


class LazyGroup(click.Group):
    """Click group with lazy command loading."""

    def __init__(self, *args, lazy_commands=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._lazy_commands = lazy_commands or {}

    def list_commands(self, ctx):
        ordered = [c for c in COMMAND_ORDER if c in self._lazy_commands]
        extras = sorted(set(self._lazy_commands.keys()) - set(COMMAND_ORDER))
        return ordered + extras

    def get_command(self, ctx, name):
        if name in self._lazy_commands:
            import_path, command_name = self._lazy_commands[name]
            import importlib

            module = importlib.import_module(import_path)
            return getattr(module, command_name)
        return None


# Define lazy command mappings (module path, command function name)
LAZY_COMMANDS = {
    "init": ("snowflake_semantic_tools.interfaces.cli.commands.init", "init"),
    "debug": ("snowflake_semantic_tools.interfaces.cli.commands.debug", "debug"),
    "enrich": ("snowflake_semantic_tools.interfaces.cli.commands.enrich", "enrich"),
    "format": ("snowflake_semantic_tools.interfaces.cli.commands.format", "format_cmd"),
    "compile": ("snowflake_semantic_tools.interfaces.cli.commands.compile", "compile_cmd"),
    "extract": ("snowflake_semantic_tools.interfaces.cli.commands.extract", "extract"),
    "validate": ("snowflake_semantic_tools.interfaces.cli.commands.validate", "validate"),
    "diff": ("snowflake_semantic_tools.interfaces.cli.commands.diff", "diff_cmd"),
    "generate": ("snowflake_semantic_tools.interfaces.cli.commands.generate", "generate"),
    "deploy": ("snowflake_semantic_tools.interfaces.cli.commands.deploy", "deploy"),
    "drop": ("snowflake_semantic_tools.interfaces.cli.commands.drop", "drop"),
    "list": ("snowflake_semantic_tools.interfaces.cli.commands.list", "list_cmd"),
    "migrate-meta": ("snowflake_semantic_tools.interfaces.cli.commands.migrate_meta", "migrate_meta"),
}


@click.group(cls=LazyGroup, lazy_commands=LAZY_COMMANDS)
@click.version_option(version=__version__, prog_name="snowflake-semantic-tools")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["table", "json", "plain"], case_sensitive=False),
    default="table",
    help="Output format: table (default), json (machine-readable), plain (no colors/timestamps)",
)
@click.pass_context
def cli(ctx, output):
    """Snowflake Semantic Tools — build and deploy Snowflake Semantic Views from dbt.

    \b
    Workflow (run commands in this order):
      1. sst init        Set up SST in your dbt project (one-time)
      2. sst enrich      Auto-populate column metadata from Snowflake
      3. sst validate    Check for errors offline (no Snowflake needed)
      4. sst deploy      Validate + extract + generate in one step
    \b
    Quick Start:
      cd your-dbt-project/
      sst init
      sst enrich models/
      sst deploy
    \b
    Prerequisites:
      • A dbt project with dbt_project.yml
      • Snowflake credentials in ~/.dbt/profiles.yml
      • Models annotated with config.meta.sst (use 'sst enrich' to bootstrap)
      • A compiled manifest (run 'dbt compile' or use 'sst validate --dbt-compile')
    \b
    Exit Codes:
      0  Success
      1  Errors found (validation/generation failures)
      2  Configuration or internal error
    \b
    Global Options:
      --output json    Machine-readable output for CI/CD and AI agents
      --output plain   No colors or timestamps (for piping)
    """
    ctx.ensure_object(dict)
    ctx.obj["output_format"] = output

    if _is_help_or_version_request() or _is_init_command():
        return

    from snowflake_semantic_tools.shared.events import setup_events
    from snowflake_semantic_tools.shared.utils.logger import get_logger

    setup_events(verbose=False, show_timestamps=False)

    try:
        from snowflake_semantic_tools.shared.config import get_config
        from snowflake_semantic_tools.shared.config_validator import validate_and_report_config

        config = get_config()
        config_path = config._find_config_file() if hasattr(config, "_find_config_file") else None
        validate_and_report_config(
            config._config if hasattr(config, "_config") else {},
            config_path=config_path,
            fail_on_errors=False,
        )
    except Exception as e:
        logger = get_logger(__name__)
        logger.debug(f"Config validation during CLI init: {e}")


if __name__ == "__main__":
    cli()
