"""The `sst` entry point: the root group, with every command registered on it.

The console scripts in `pyproject.toml` name `cli` here, and `python -m
snowflake_semantic_tools.cli.main` runs `main`. Every Snowflake connection is opened
through this module's `SnowflakeConnector`, looked up when the connection is opened, so
replacing that one attribute -- as the recorded-Snowflake helpers and the reference
project do -- runs every command against a double.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.cli import commands  # imported for its effect: registering every command on `cli`
from snowflake_semantic_tools.cli.group import cli

__all__ = ["SnowflakeConnector", "cli", "main"]


def main() -> None:
    """Run `sst`; `python -m snowflake_semantic_tools.cli.main` enters here."""
    cli(standalone_mode=True)


if __name__ == "__main__":
    main()
