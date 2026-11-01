"""`sst schema`: write the JSON Schemas of SST's authored files and `sst_config.yml`.

One `<kind>.schema.json` per kind, `--kind` picking some, under `sst_target/schemas/` or
`--output-dir`. Each is generated from the tables SST reads by, so an editor that loads one
checks a file as SST would. Only a file that differs from the one on disk is written.
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.locations import ProjectPaths, sst_target_dir
from snowflake_semantic_tools.adapters.paths import output_root
from snowflake_semantic_tools.cli.exit_codes import OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body, write_text
from snowflake_semantic_tools.domain.render.json_schema import file_schemas

# Where the schemas are written without --output-dir, under SST's build directory.
SCHEMAS_DIR = "schemas"


@click.command(cls=SstCommand)
@click.option("--kind", "kinds", type=click.Choice(tuple(file_schemas())), multiple=True)
@click.option("--output-dir", type=click.Path(file_okay=False, path_type=Path))
@command_body("schema", config=ConfigNeed.OPTIONAL)
def schema(paths: ProjectPaths, kinds: tuple[str, ...], output_dir: Path | None) -> CommandResult:
    """Write the JSON Schemas of SST's authored files and sst_config.yml.

    One <kind>.schema.json per file kind, under sst_target/schemas/, or --output-dir; --kind
    picks some of them. Each schema is generated from the tables SST reads files by, so an
    editor that loads it reports what SST would: a key SST does not read, a required key
    missing, a value outside its choices or of another kind. Only a file that differs from the
    one on disk is written. Exit 1 when one cannot be written.
    """
    root = paths.project_dir
    directory = output_dir if output_dir is not None else sst_target_dir(root) / SCHEMAS_DIR
    every = file_schemas()
    chosen = kinds or tuple(every)
    pages = {directory / f"{kind}.schema.json": json.dumps(every[kind], indent=2) + "\n" for kind in chosen}
    written = [path for path, text in pages.items() if not path.is_file() or path.read_text(encoding="utf-8") != text]
    for path in written:
        write_text(output_root(root, directory), path, pages[path])
    data = {
        "kinds": list(chosen),
        "output_dir": str(directory),
        "written": [str(path) for path in written],
    }
    return CommandResult(OK, data=data, human=lambda: _print_written(written, len(pages)))


def _print_written(written: list[Path], count: int) -> None:
    for path in written:
        click.echo(f"wrote {path}")
    click.echo(f"{count} schema(s) current")
