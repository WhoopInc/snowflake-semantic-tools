"""
Shared CLI Options

Reusable Click option decorators to ensure consistency across commands.
This module centralizes common CLI options to follow DRY principles.
"""

from functools import wraps
from typing import Callable

import click


def target_option(f: Callable) -> Callable:
    """
    Common target option for dbt profile selection.

    Adds --target / -t option to specify the dbt profile target.

    Example:
        @click.command()
        @target_option
        def my_command(dbt_target: str):
            ...
    """
    f = click.option(
        "--target",
        "-t",
        "dbt_target",
        help="dbt target from profiles.yml (default: uses profile's default)",
    )(f)
    return f


def database_schema_options(f: Callable) -> Callable:
    """
    Common database/schema options for commands.

    Adds --db and --schema options. These are now optional,
    defaulting to values from the dbt profile.

    Example:
        @click.command()
        @database_schema_options
        def my_command(db: str, schema: str):
            ...
    """

    @click.option(
        "--schema",
        "-s",
        required=False,
        help="Target schema (default: from profile)",
    )
    @click.option(
        "--db",
        required=False,
        help="Target database (default: from profile)",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


def defer_options(f: Callable) -> Callable:
    """
    Common defer-related options for generate/deploy commands.

    Adds defer options for manifest-based table reference resolution:
    - --defer-target: dbt target to use for table references
    - --state: Path to defer state artifacts directory
    - --only-modified: Only process changed models
    - --no-defer: Disable defer (overrides config)

    Example:
        @click.command()
        @defer_options
        def my_command(defer_target: str, state: str, only_modified: bool, no_defer: bool):
            ...
    """

    @click.option(
        "--no-defer",
        is_flag=True,
        help="Disable defer (overrides config)",
    )
    @click.option(
        "--only-modified",
        is_flag=True,
        help="Only process changed models (requires defer)",
    )
    @click.option(
        "--state",
        type=click.Path(exists=True, file_okay=False, dir_okay=True),
        help="Path to defer state artifacts directory",
    )
    @click.option(
        "--defer-target",
        help="dbt target for table references (e.g., 'prod')",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


def verbose_quiet_options(f: Callable) -> Callable:
    """
    Common verbose/quiet options for output control.

    Adds --verbose / -v and --quiet / -q options.

    Example:
        @click.command()
        @verbose_quiet_options
        def my_command(verbose: bool, quiet: bool):
            ...
    """

    @click.option(
        "--quiet",
        "-q",
        is_flag=True,
        help="Suppress all output except errors",
    )
    @click.option(
        "--verbose",
        "-v",
        is_flag=True,
        help="Verbose output",
    )
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper
