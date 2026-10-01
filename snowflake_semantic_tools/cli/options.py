"""The click options several commands share, each declared once.

Every factory returns a decorator for one option, or for options that always appear
together and in this order. A command applies them where each option sits in its own
signature: `--help` and the generated CLI reference list options in decorator order.
Help text is not set here; `help_text.document_options` gives each flag its one shared
description, so every command that takes a flag describes it the same way.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

Decorator = Callable[[Callable[..., Any]], Callable[..., Any]]


def stacked(*decorators: Decorator) -> Decorator:
    """Apply `decorators` as if they were written one above the other, in the order given."""

    def apply(function: Callable[..., Any]) -> Callable[..., Any]:
        for decorator in reversed(decorators):
            function = decorator(function)
        return function

    return apply


def project_dir_option(*, exists: bool = True, show_default: bool | None = None) -> Decorator:
    """`--project-dir`, the current directory by default; it must exist unless `exists` is false."""
    return click.option(
        "--project-dir",
        type=click.Path(exists=exists, file_okay=False, path_type=Path),
        default=Path("."),
        show_default=show_default,
    )


def target_option() -> Decorator:
    """`--target`, passed to the command as `target_name`."""
    return click.option("--target", "target_name")


def manifest_option() -> Decorator:
    """`--manifest`, an existing dbt `manifest.json`, passed as `manifest_path`."""
    return click.option(
        "--manifest",
        "manifest_path",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
    )


def project_options() -> Decorator:
    """`--project-dir`, `--target`, and `--manifest`: the project a command reads, and its target."""
    return stacked(project_dir_option(), target_option(), manifest_option())


def select_option(*, multiple: bool = True) -> Decorator:
    """`--select`, passed as `selected`: repeatable unless `multiple` is false."""
    return click.option("--select", "selected", multiple=multiple)


def selection_options() -> Decorator:
    """`--select` and `--exclude`, both repeatable, passed as `selected` and `excluded`."""
    return stacked(select_option(), click.option("--exclude", "excluded", multiple=True))


def prune_option() -> Decorator:
    """The `--prune` flag."""
    return click.option("--prune", is_flag=True)


def partial_option() -> Decorator:
    """The `--partial` flag."""
    return click.option("--partial", is_flag=True)


def validation_options() -> Decorator:
    """`--strict/--no-strict` and `--snowflake-syntax-check/--no-...`; None when neither is given."""
    return stacked(
        click.option("--strict/--no-strict", default=None),
        click.option("--snowflake-syntax-check/--no-snowflake-syntax-check", default=None),
    )


def sql_out_option() -> Decorator:
    """`--sql-out`, a directory for each change's statements."""
    return click.option("--sql-out", type=click.Path(file_okay=False, path_type=Path))


def fail_fast_option() -> Decorator:
    """The `--fail-fast` flag."""
    return click.option("--fail-fast", is_flag=True)


def output_option() -> Decorator:
    """`--output human|json`, human by default."""
    return click.option("--output", type=click.Choice(["human", "json"]), default="human")
