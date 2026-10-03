"""`sst compile`: render every artifact and write the canonical manifest, offline.

The canonical manifest, `target/sst/manifest.json`, always holds everything that compiled,
even with `--select`, which narrows only what is reported and emitted. `--emit-ddl` writes
every rendered payload and `--emit-agent-spec` every agent's rendered specification, one
file per artifact; both are offline, because rendering is a pure function.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.partial import partial_refusal, partial_split
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.options import (
    database_option,
    defer_target_option,
    model_path_options,
    partial_option,
    select_option,
    target_option,
    with_model_paths,
)
from snowflake_semantic_tools.cli.plan_output import artifact_suffix
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, project_path, write_text
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.state import Manifest

_DIRECTORY = click.Path(file_okay=False, path_type=Path)


@click.command()
@target_option()
@defer_target_option()
@database_option()
@model_path_options()
@click.option("--emit-ddl", "emit_ddl_dir", type=_DIRECTORY)
@click.option("--emit-agent-spec", "agent_spec_dir", type=_DIRECTORY)
@select_option(multiple=False)
@partial_option()
@command_body("compile")
def compile(
    paths: ProjectPaths,
    target_name: str | None,
    database: str | None,
    emit_ddl_dir: Path | None,
    agent_spec_dir: Path | None,
    selected: str | None,
    partial: bool,
    manifest_path: Path | None,
    dbt_dir: Path | None,
    semantic_dir: Path | None,
) -> CommandResult:
    """Compile every artifact and write the canonical manifest, offline.

    Diagnostics:
        SST-MAN008: the compile failed, so no manifest was written.
        SST-MAN007: the canonical manifest could not be written.
    """
    paths = with_model_paths(paths, dbt_dir, semantic_dir)
    full_result = compiling.compile_result(paths, target_name, manifest_path, database=database)
    split = partial_split(full_result) if partial and not full_result.success else None
    if not full_result.success and split is None:
        refusal = partial_refusal(full_result) if partial else None
        errors = full_result.diagnostics.count(Severity.ERROR)
        unwritten = D("SST-MAN008", detail=f"{errors} error(s); no manifest was written")
        return CommandResult(
            ERROR, DiagnosticBag((*full_result.diagnostics, *((refusal,) if refusal else ()), unwritten))
        )
    # `--partial` writes the manifest for what can publish and still exits 1.
    healthy = full_result if split is None else split.healthy
    shown = full_result.diagnostics if split is None else DiagnosticBag((*healthy.diagnostics, *split.notices))
    excluded = None if split is None else split.excluded
    destination = target_dir(paths.project_dir) / "manifest.json"
    manifest = build_manifest(paths, healthy, manifest_path, target_name)
    try:
        ManifestFileStore(destination).write(manifest)
    except OSError as exc:
        failed = D("SST-MAN007", path=str(exc.filename or destination.parent), detail=exc.strerror or str(exc))
        return CommandResult(ERROR, DiagnosticBag((*shown, failed)))
    result = healthy if selected is None else compiling.selected_result(paths.project_dir, healthy, (selected,))
    data = _compiled_data(result, destination, manifest, excluded)
    if emit_ddl_dir is not None:
        files = _emit(project_path(paths, emit_ddl_dir), result, agents_only=False)
        data.update(ddl_dir=str(emit_ddl_dir), ddl_files=files)
    if agent_spec_dir is not None:
        files = _emit(project_path(paths, agent_spec_dir), result, agents_only=True)
        data.update(agent_spec_dir=str(agent_spec_dir), agent_spec_files=files)
    return CommandResult(
        OK if split is None else ERROR,
        shown,
        data,
        human=lambda: _print_compiled(data, excluded),
        show_diagnostics=split is not None,
    )


def _emit(directory: Path, result: CompileResult, *, agents_only: bool) -> list[str]:
    """Write one payload file per compiled artifact into `directory`; return the names written.

    With `agents_only`, only agents are written, each as its rendered JSON specification.
    """
    written = []
    for item in result.compiled:
        if agents_only and item.artifact_type != "agent":
            continue
        suffix = artifact_suffix(item.rendered_artifact.render_dialect)
        name = f"{item.name.casefold()}{suffix}"
        content = item.rendered_artifact.content
        write_text(directory / name, content if suffix in (".json", ".yaml") else content.rstrip() + ";\n")
        written.append(name)
    return written


def _compiled_data(
    result: CompileResult, destination: Path, manifest: Manifest, excluded: tuple[str, ...] | None
) -> dict[str, object]:
    artifacts = [
        {
            "artifact_key": item.artifact_key,
            "fingerprint": item.rendered_artifact.fingerprint,
            "target": item.rendered_artifact.target.sql,
        }
        for item in result.compiled
    ]
    counts: dict[str, int] = {}
    for item in result.compiled:
        counts[item.artifact_type] = counts.get(item.artifact_type, 0) + 1
    data: dict[str, object] = {
        "manifest_path": str(destination),
        "manifest_id": manifest.manifest_id,
        "artifacts": artifacts,
        "artifact_counts": dict(sorted(counts.items())),
        "checksums": {item.artifact_key: item.rendered_artifact.fingerprint for item in result.compiled},
    }
    if excluded is not None:
        data["partial"] = {"excluded": list(excluded)}
    return data


def _print_compiled(data: dict[str, object], excluded: tuple[str, ...] | None) -> None:
    """Print a summary line, then where each emitted set of files was written."""
    artifacts = data["artifacts"]
    assert isinstance(artifacts, list)
    partial_note = f" (partial: {len(excluded)} excluded)" if excluded is not None else ""
    click.echo(f"compiled {len(artifacts)} artifact(s){partial_note}; manifest {data['manifest_path']}")
    for files_key, dir_key in (("ddl_files", "ddl_dir"), ("agent_spec_files", "agent_spec_dir")):
        written = data.get(files_key)
        if isinstance(written, list):
            click.echo(f"wrote {len(written)} file(s) to {data[dir_key]}")
