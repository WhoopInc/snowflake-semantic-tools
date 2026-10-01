"""`sst migrate refs`: rewrite a 0.3 project's references into the 1.0 dialect."""

from __future__ import annotations

from pathlib import Path

import click

from ...adapters.yaml.migrate import filter_sites, semantic_files, write_file
from ...app.migrate_refs import MigrateRefs, MigrationReport
from ..exit_codes import CHANGES, ERROR, OK
from ..options import output_option, project_dir_option
from ..runner import CommandResult, command_body
from ..settings import semantic_models_dir


@click.group()
@click.pass_context
def migrate(ctx: click.Context) -> None:
    """Rewrite a 0.3 project into the 1.0 dialect."""
    inherited = dict(ctx.default_map or {})
    ctx.default_map = {name: dict(inherited) for name in ("refs",)}


@migrate.command(name="refs")
@project_dir_option()
@click.option("--write", "write_files", is_flag=True, help="Rewrite files in place instead of reporting.")
@output_option()
@command_body("migrate refs")
def migrate_refs_command(project_dir: Path, write_files: bool, output: str) -> CommandResult:
    """Rewrite legacy table()/column() globals to ref(), and label boolean filters.

    Dry-run by default: exit 2 when rewrites are pending, 0 when there are none,
    and 1 when a table() call sits where no rewrite is safe.
    """
    files = semantic_files(project_dir, semantic_models_dir(project_dir))
    report = MigrateRefs(files, filter_sites).run()
    if write_files:
        for item in report.changed:
            write_file(project_dir, item.path, item.result.text)
    if report.untouched:
        exit_code = ERROR
    elif report.changed and not write_files:
        exit_code = CHANGES
    else:
        exit_code = OK
    return CommandResult(
        exit_code,
        data=_report_data(report, write_files),
        human=lambda: _print_report(report, write_files),
    )


def _report_data(report: MigrationReport, write_files: bool) -> dict[str, object]:
    return {
        "written": write_files and bool(report.changed),
        "files": [
            {
                "path": item.path,
                "rewrites": item.counts(),
                "untouched": [
                    {
                        "line": entry.line,
                        "column": entry.col,
                        "text": entry.text,
                        "reason": entry.reason,
                    }
                    for entry in item.result.untouched
                ],
            }
            for item in report.files
            if item.result.changed or item.result.untouched
        ],
    }


def _print_report(report: MigrationReport, write_files: bool) -> None:
    """Print each rewritten file's counts on stdout and each call left unchanged on stderr."""
    for item in report.files:
        if item.result.changed:
            counts = ", ".join(f"{value} {kind}" for kind, value in item.counts().items() if value)
            click.echo(f"{'rewrote' if write_files else 'would rewrite'} {item.path}: {counts}")
        for entry in item.result.untouched:
            click.echo(f"{item.path}:{entry.line}:{entry.col}: left {entry.text} unchanged: {entry.reason}", err=True)
    if not report.changed and not report.untouched:
        click.echo("no legacy references found")
