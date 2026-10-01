"""`sst init`: scaffold a minimal project, never overwriting a file that exists."""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.cli.options import output_option, project_dir_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body


@click.command()
@project_dir_option(exists=False, show_default=True)
@output_option()
@command_body("init")
def init(project_dir: Path, output: str) -> CommandResult:
    """Create a minimal SST project scaffold without overwriting files."""
    created: list[str] = []
    config = project_dir / "sst_config.yml"
    if not config.exists():
        project_dir.mkdir(parents=True, exist_ok=True)
        config.write_text(
            "project:\n  semantic_models_dir: semantic_models\n\n"
            "validation:\n  strict: false\n  snowflake_syntax_check: true\n\n"
            'state:\n  +database: "{{ target.database }}"\n'
            '  +schema: "{{ target.schema }}"\n  +table: SST_STATE\n',
            encoding="utf-8",
        )
        created.append("sst_config.yml")
    views = project_dir / "semantic_models" / "semantic_views"
    views.mkdir(parents=True, exist_ok=True)
    return CommandResult(
        data={"created": created},
        human=lambda: click.echo(f"initialized SST project; created {len(created)} file(s)"),
    )
