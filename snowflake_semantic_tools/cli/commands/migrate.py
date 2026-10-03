"""`sst migrate refs`: rewrite a 0.3 project's references into the 1.0 dialect."""

from __future__ import annotations

import click

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.migrate import filter_sites, semantic_files, write_file
from snowflake_semantic_tools.app.migrate_refs import MigrateRefs, MigrationReport
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.runner import CommandResult, WriteFailure, command_body
from snowflake_semantic_tools.cli.settings import semantic_models_dir


@click.group()
@click.pass_context
def migrate(ctx: click.Context) -> None:
    """Rewrite a 0.3 project into the 1.0 dialect."""
    inherited = dict(ctx.default_map or {})
    ctx.default_map = {name: dict(inherited) for name in ("refs",)}


@migrate.command(name="refs")
@click.option("--write", "write_files", is_flag=True, help="Rewrite files in place instead of reporting.")
@command_body("migrate refs")
def migrate_refs_command(paths: ProjectPaths, write_files: bool) -> CommandResult:
    """Rewrite legacy table()/column() globals to ref(), and label boolean filters.

    Dry-run by default: exit 2 when rewrites are pending, 0 when there are none,
    and 1 when a table() call sits where no rewrite is safe.
    """
    project_dir = paths.project_dir
    files = semantic_files(project_dir, semantic_models_dir(paths))
    report = MigrateRefs(files, filter_sites).run()
    if write_files:
        for item in report.changed:
            try:
                write_file(project_dir, item.path, item.result.text)
            except OSError as exc:
                raise WriteFailure(project_dir / item.path, exc) from exc
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
