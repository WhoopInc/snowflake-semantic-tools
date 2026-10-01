"""`sst docs`: write, or check, the reference pages generated from the engine's own registries."""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.cli.exit_codes import ERROR, EXIT_CODE_DOCS, OK
from snowflake_semantic_tools.cli.group import cli
from snowflake_semantic_tools.cli.help_text import command_docs, option_docs
from snowflake_semantic_tools.cli.options import output_option, project_dir_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.domain.render.reference_docs import reference_pages


@click.command()
@project_dir_option(exists=False)
@click.option("--check", is_flag=True)
@output_option()
@command_body("docs")
def docs(project_dir: Path, check: bool, output: str) -> CommandResult:
    """Write the generated reference pages under docs/reference/.

    The artifact, error-code, configuration, and command-line references are
    rendered from the engine's own registries, so they cannot drift from what the
    engine accepts. With --check, nothing is written and the command exits 1 when
    a committed page differs.
    """
    pages = reference_pages(command_docs(cli), option_docs(cli), EXIT_CODE_DOCS)
    drifted = [
        path
        for path, text in sorted(pages.items())
        if not (project_dir / path).is_file() or (project_dir / path).read_text(encoding="utf-8") != text
    ]
    if not check:
        for path in drifted:
            (project_dir / path).parent.mkdir(parents=True, exist_ok=True)
            (project_dir / path).write_text(pages[path], encoding="utf-8")
    exit_code = ERROR if check and drifted else OK
    data = {"pages": sorted(pages), "drifted": drifted, "written": [] if check else drifted}
    return CommandResult(exit_code, data=data, human=lambda: _print_pages(drifted, len(pages), check=check))


def _print_pages(drifted: list[str], page_count: int, *, check: bool) -> None:
    for path in drifted:
        click.echo(f"out of date: {path}" if check else f"wrote {path}")
    if check and drifted:
        click.echo("run `sst docs` to regenerate the reference pages")
    else:
        click.echo(f"{page_count} reference page(s) current")
