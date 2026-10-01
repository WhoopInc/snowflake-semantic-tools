"""`sst list`: the compiled artifacts, with the status the local state file records for each."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.fs.local import STATE_FILE_GLOB, StateFileStore
from snowflake_semantic_tools.app.listing import ArtifactSummary, list_artifacts
from snowflake_semantic_tools.cli.options import output_option, project_dir_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest
from snowflake_semantic_tools.cli.wiring.project import target_dir


@click.command(name="list")
@project_dir_option()
@output_option()
@command_body("list")
def list_command(project_dir: Path, output: str) -> CommandResult:
    """List compiled artifacts and cached application status."""
    manifest = compiled_manifest(project_dir)
    states = tuple(sorted(target_dir(project_dir).glob(STATE_FILE_GLOB)))
    state = StateFileStore(states[0]).read_local() if len(states) == 1 else None
    summaries = list_artifacts(manifest, state)
    data = [dataclasses.asdict(item) for item in summaries]
    return CommandResult(data=data, human=lambda: _print_summaries(summaries))


def _print_summaries(summaries: tuple[ArtifactSummary, ...]) -> None:
    for item in summaries:
        click.echo(f"{item.key} {item.status} {item.target} {item.fingerprint}")
