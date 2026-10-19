"""`sst clean`: remove SST's local build directory, and touch nothing in Snowflake or authored.

`--dry-run` lists what would be removed and removes nothing.
"""

from __future__ import annotations

import click

from snowflake_semantic_tools.adapters.locations import LEGACY_SST_TARGET, ProjectPaths
from snowflake_semantic_tools.adapters.paths import remove_tree_within
from snowflake_semantic_tools.cli.exit_codes import ERROR
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag


@click.command(cls=SstCommand)
@click.option("--dry-run", is_flag=True)
@command_body("clean")
def clean(paths: ProjectPaths, dry_run: bool) -> CommandResult:
    """Remove local SST build artifacts only; never touch Snowflake.

    Removes `sst_target/` under the project, where SST writes the compiled manifest, saved plans
    and observations, statement files, local state, the run log, and `sst ask` transcripts; and
    `target/sst/`, where SST wrote them before they moved, when an earlier run left it. Authored files, `.sst/` with its
    baseline, and dbt's `target/` are kept. With --dry-run, nothing is removed. Exit 1 when a
    build directory cannot be removed, or a symbolic link is on the way to it.

    Diagnostics:
        SST-PRT010: a build directory could not be removed, or is reached through a link.
    """
    found = tuple(
        path for path in (target_dir(paths.project_dir), paths.project_dir / LEGACY_SST_TARGET) if path.exists()
    )
    shown = ", ".join(str(path) for path in found)
    if dry_run:
        data = {"removed": [], "would_remove": [str(path) for path in found]}
        return CommandResult(data=data, headline=f"would remove {shown}" if found else "nothing to remove")
    removed: list[str] = []
    for path in found:
        try:
            remove_tree_within(paths.project_dir, path)
        except OSError as exc:
            diagnostic = D("SST-PRT010", subject="cli", path=str(path), detail=str(exc))
            left = [str(item) for item in found if str(item) not in removed]
            return CommandResult(ERROR, DiagnosticBag((diagnostic,)), {"removed": removed, "would_remove": left})
        removed.append(str(path))
    data = {"removed": removed, "would_remove": []}
    return CommandResult(
        data=data,
        headline=f"removed {shown}" if found else f"nothing to remove at {target_dir(paths.project_dir)}",
    )
