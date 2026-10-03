"""The click options several commands share, each declared once.

Every factory returns a decorator for one option, or for options that always appear
together and in this order. A command applies them where each option sits in its own
signature: `--help` and the generated CLI reference list options in decorator order.
Help text is not set here; `help_text.document_options` gives each flag its one shared
description, so every command that takes a flag describes it the same way. The global
options -- `--output`, `--project-dir`, `--manifest`, and the rest -- live in `cli.globals`.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from snowflake_semantic_tools.adapters.locations import ProjectPaths

Decorator = Callable[[Callable[..., Any]], Callable[..., Any]]


def stacked(*decorators: Decorator) -> Decorator:
    """Apply `decorators` as if they were written one above the other, in the order given."""

    def apply(function: Callable[..., Any]) -> Callable[..., Any]:
        for decorator in reversed(decorators):
            function = decorator(function)
        return function

    return apply


def target_option() -> Decorator:
    """`--target`/`-t`, else `$SST_TARGET`, passed to the command as `target_name`."""
    return click.option("--target", "-t", "target_name", envvar="SST_TARGET")


def database_option() -> Decorator:
    """`--database`: where a command reads from, never where an artifact is published."""
    return click.option("--database")


def _check_selectors(ctx: click.Context, param: click.Parameter, value: Any) -> Any:
    # Imported here: the selector wiring imports the root group, which imports this module.
    from snowflake_semantic_tools.cli.wiring.compile import check_selectors

    return check_selectors(ctx, param, value)


def select_option(*, multiple: bool = True) -> Decorator:
    """`--select`, passed as `selected`: repeatable unless `multiple` is false; checked as it is parsed."""
    return click.option("--select", "selected", multiple=multiple, callback=_check_selectors)


def selection_options() -> Decorator:
    """`--select` and `--exclude`, both repeatable, passed as `selected` and `excluded`."""
    return stacked(select_option(), click.option("--exclude", "excluded", multiple=True, callback=_check_selectors))


def state_option() -> Decorator:
    """`--state`, else `$SST_STATE_DIR`: the directory holding a previous run's `manifest.json`."""
    return click.option(
        "--state", "state_dir", type=click.Path(file_okay=False, path_type=Path), envvar="SST_STATE_DIR"
    )


def prune_option() -> Decorator:
    """The `--prune` flag."""
    return click.option("--prune", is_flag=True)


def partial_option() -> Decorator:
    """The `--partial` flag."""
    return click.option("--partial", is_flag=True)


def validation_options() -> Decorator:
    """`--strict/--no-strict` (else `$SST_STRICT`) and the syntax-check pair; None when unset."""
    from snowflake_semantic_tools.cli.globals import STRICT_BOOL

    return stacked(
        click.option("--strict/--no-strict", default=None, envvar="SST_STRICT", type=STRICT_BOOL),
        click.option("--snowflake-syntax-check/--no-snowflake-syntax-check", default=None),
    )


def defer_target_option() -> Decorator:
    """`--defer-target` (else `$SST_DEFER_TARGET`), the target dbt objects resolve to, and `--no-defer`.

    The runner carries both onto the run's `ProjectPaths`, which `adapters.deferral` reads.
    """
    return stacked(
        click.option("--defer-target", "defer_target", envvar="SST_DEFER_TARGET"),
        click.option("--no-defer", "no_defer", is_flag=True),
    )


def sql_out_option() -> Decorator:
    """`--sql-out`, a directory for each change's statements."""
    return click.option("--sql-out", type=click.Path(file_okay=False, path_type=Path))


def fail_fast_option() -> Decorator:
    """The `--fail-fast` flag."""
    return click.option("--fail-fast", is_flag=True)


def fail_fast_pair() -> Decorator:
    """`--fail-fast/--no-fail-fast`, None when neither is given so a config key decides."""
    return click.option("--fail-fast/--no-fail-fast", default=None)


def threads_option() -> Decorator:
    """`--threads`, 1 to 16, else `$SST_THREADS`; None when neither is given."""
    return click.option("--threads", type=click.IntRange(1, 16), envvar="SST_THREADS")


def no_detailed_exitcode_option() -> Decorator:
    """The `--no-detailed-exitcode` flag, which turns exit 2 into exit 0."""
    return click.option("--no-detailed-exitcode", is_flag=True)


def no_validate_option() -> Decorator:
    """The `--no-validate` flag, which plans without validating."""
    return click.option("--no-validate", is_flag=True)


def model_path_options() -> Decorator:
    """`--dbt` and `--semantic`, existing paths standing in for the dbt and semantic models paths."""
    existing = click.Path(exists=True, file_okay=False, path_type=Path)
    return stacked(
        click.option("--dbt", "dbt_dir", type=existing),
        click.option("--semantic", "semantic_dir", type=existing),
    )


def with_model_paths(files: ProjectPaths, dbt_dir: Path | None, semantic_dir: Path | None) -> ProjectPaths:
    """Return `files` reading `--dbt` and `--semantic` in place of the configured paths, when given."""
    return dataclasses.replace(
        files,
        model_paths=(_project_relative(files, dbt_dir),) if dbt_dir is not None else files.model_paths,
        semantic_models_dir=(
            _project_relative(files, semantic_dir) if semantic_dir is not None else files.semantic_models_dir
        ),
    )


def _project_relative(files: ProjectPaths, path: Path) -> str:
    """Name a path as the configuration would: relative to the project, else in full."""
    try:
        return path.resolve().relative_to(files.project_dir.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())
