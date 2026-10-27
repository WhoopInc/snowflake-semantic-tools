"""`sst test`: smoke-probe what `sst apply` published.

The run first compiles the project and stops on its errors, then refuses to probe unless
`sst compile` wrote the manifest the project compiles to now.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.smoke import SmokePublished
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.options import (
    defer_target_option,
    fail_fast_option,
    selection_options,
    target_option,
    threads_option,
)
from snowflake_semantic_tools.cli.output import print_progress
from snowflake_semantic_tools.cli.render.diagnostics import plural
from snowflake_semantic_tools.cli.render.progress import active_display
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.settings import threads_setting
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest, current_manifest, refuse_stale_inputs
from snowflake_semantic_tools.cli.wiring.project import connect, open_connector, project_inputs, state_store
from snowflake_semantic_tools.cli.wiring.selection import narrowed, noted
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.ports.progress import NO_PROGRESS


@click.command(cls=SstCommand, name="test")
@selection_options()
@target_option()
@defer_target_option()
@threads_option()
@fail_fast_option()
@command_body("test")
def test_command(
    paths: ProjectPaths,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    target_name: str | None,
    manifest_path: Path | None,
    threads: int | None,
    fail_fast: bool,
) -> CommandResult:
    """Smoke-probe what `sst apply` published: each view, metric, verified query, tool, and agent.

    Once `sst compile` has written the manifest, SST proves it owns each published object the
    selection compiles, then runs a read-only query against it: each semantic view, its metrics
    and verified queries, and a DESCRIBE of each tool, agent, and CoWork profile. A probe passes
    when its query runs. Nothing is written, no lock is taken, and no agent runs; `sst eval`
    runs agents. `--select` and
    `--exclude` narrow the artifacts probed. Exit 1 when the project does not compile or a
    probe fails; 3 when the selection compiles nothing to probe; 4 when the compiled manifest is
    missing or stale; and 5 when Snowflake cannot be reached.
    """
    refuse_stale_inputs(paths, target_name, manifest_path, before="test")
    compiled = compiling.compile_result(paths, target_name, manifest_path)
    if not compiled.success:
        return CommandResult(ERROR, compiled.diagnostics)
    result, notes = narrowed(paths, compiled, selected, excluded, manifest_path, target_name)
    _refuse_nothing_to_probe(paths, result)
    inputs = project_inputs(paths, target_name, manifest_path)
    workers = threads_setting(paths, threads, default=1)
    return noted(_run_smoke(paths, target_name, (compiled, result), inputs, fail_fast, workers), notes)


def _refuse_nothing_to_probe(paths: ProjectPaths, result: CompileResult) -> None:
    """Refuse a run that would probe nothing, before connecting.

    Raises:
        ProjectError: `sst compile` has written no manifest (SST-MAN001), or the selection
            compiles no artifact with a smoke probe (SST-DIS012).
    """
    compiled_manifest(paths.project_dir)
    if not any(item.rendered_artifact.smoke for item in result.compiled):
        diagnostic = D("SST-DIS012")
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _run_smoke(
    paths: ProjectPaths,
    target_name: str | None,
    results: tuple[CompileResult, CompileResult],
    inputs: YamlProjectInputs,
    fail_fast: bool,
    threads: int,
) -> CommandResult:
    """Probe the published objects once SST is proven to own them; exit 1 when a check or probe fails.

    `results` is the whole compile, which the compiled manifest must match, and the selection
    the probes run on. The markers are read, and the probes run, on up to `threads` sessions
    at once; every session but the first is closed before the connection is.
    """
    full, result = results
    manifest = current_manifest(paths.project_dir, full, inputs, before="test")
    profile, port = connect(paths, target_name)
    params = profile.connection_params
    try:
        with ConnectorPool(threads, lambda: open_connector(params)) as pool:
            readers = Fanout(port, pool, threads)
            smoke = SmokePublished(
                port,
                state_store(paths, profile.target_name),
                readers,
                phases=RUN_TIMINGS,
                progress=active_display() or NO_PROGRESS,
            ).run(
                result,
                manifest,
                target=profile.identity,
                state_table=profile.state_table,
                fail_fast=fail_fast,
            )
    finally:
        port.close()
    if smoke.unpublished:
        print_progress(f"- smoke probes skipped: {plural(smoke.unpublished, 'artifact')} not published as compiled")
    attempted = len(smoke.attempted)
    return CommandResult(
        OK if smoke.success else ERROR,
        smoke.diagnostics,
        data={"attempted": attempted},
        headline=f"{plural(attempted, 'smoke probe')} passed",
    )
