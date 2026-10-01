"""`sst clean`: remove SST's local build directory, and touch nothing in Snowflake."""

from __future__ import annotations

import shutil
from pathlib import Path

import click

from ..options import output_option, project_dir_option
from ..runner import CommandResult, command_body
from ..wiring.project import target_dir


@click.command()
@project_dir_option()
@output_option()
@command_body("clean")
def clean(project_dir: Path, output: str) -> CommandResult:
    """Remove local SST build artifacts only; never touch Snowflake."""
    path = target_dir(project_dir)
    existed = path.exists()
    shutil.rmtree(path, ignore_errors=True)
    return CommandResult(
        data={"removed": str(path), "existed": existed},
        human=lambda: click.echo(f"removed {path}" if existed else f"nothing to remove at {path}"),
    )
