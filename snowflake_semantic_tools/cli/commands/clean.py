"""`sst clean`: remove SST's local build directory, and touch nothing in Snowflake or authored.

`--dry-run` lists what would be removed and removes nothing.
"""

from __future__ import annotations

import shutil

import click

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.cli.exit_codes import ERROR
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag


@click.command()
@click.option("--dry-run", is_flag=True)
@command_body("clean")
def clean(paths: ProjectPaths, dry_run: bool) -> CommandResult:
    """Remove local SST build artifacts only; never touch Snowflake.

    Exit 1 when the build directory cannot be removed.

    Diagnostics:
        SST-PRT010: the build directory could not be removed.
    """
    path = target_dir(paths.project_dir)
    existed = path.exists()
    if dry_run:
        data = {"removed": [], "would_remove": [str(path)] if existed else []}
        return CommandResult(
            data=data, human=lambda: click.echo(f"would remove {path}" if existed else "nothing to remove")
        )
    if existed:
        try:
            shutil.rmtree(path)
        except OSError as exc:
            diagnostic = D("SST-PRT010", subject="cli", path=str(path), detail=str(exc))
            return CommandResult(ERROR, DiagnosticBag((diagnostic,)), {"removed": [], "would_remove": [str(path)]})
    data = {"removed": [str(path)] if existed else [], "would_remove": []}
    return CommandResult(
        data=data,
        human=lambda: click.echo(f"removed {path}" if existed else f"nothing to remove at {path}"),
    )
