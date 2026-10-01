"""`sst debug`: what `sst` resolved for a project -- its profile, target, and state table."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import click

from ...adapters.dbt.profiles import load_profile_target
from ...domain.model.diagnostic import DiagnosticBag
from ..options import output_option, project_dir_option, target_option
from ..runner import CommandResult, command_body
from ..wiring.project import open_connector


@click.command()
@project_dir_option()
@target_option()
@click.option("--test-connection", is_flag=True)
@output_option()
@command_body("debug")
def debug(project_dir: Path, target_name: str | None, test_connection: bool, output: str) -> CommandResult:
    """Show resolved project, profile, target, and optional connection identity."""
    profile = load_profile_target(project_dir, target_name)
    data: dict[str, object] = {
        "project_dir": str(project_dir.resolve()),
        "profile": profile.profile_name,
        "target": profile.target_name,
        "database": profile.identity.database.sql,
        "schema": profile.identity.schema.sql,
        "state_table": profile.state_table.sql,
        "authentication": profile.authentication,
    }
    if test_connection:
        port = open_connector(profile.connection_params)
        try:
            data["current_role"] = port.current_role()
            data["current_account"] = port.current_account_locator()
        finally:
            port.close()
    return CommandResult(diagnostics=DiagnosticBag(profile.diagnostics), data=data, human=lambda: _print_fields(data))


def _print_fields(data: Mapping[str, object]) -> None:
    for key, value in data.items():
        click.echo(f"{key}: {value}")
