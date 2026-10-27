"""`sst compile`: render every artifact and write the canonical manifest, offline.

The canonical manifest, `sst_target/manifest.json`, always holds everything that compiled,
even with `--select`, which narrows only what is reported and emitted. `--emit-ddl` writes
every rendered payload and `--emit-agent-spec` every agent's and CoWork profile's rendered
specification, one file per artifact; both are offline, because rendering is a pure function.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import output_root
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.partial import partial_refusal, partial_split
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
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
from snowflake_semantic_tools.cli.render.diagnostics import plural
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, project_path, write_text
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.lineage.graph import manifest_lineage
from snowflake_semantic_tools.domain.state import Manifest

_DIRECTORY = click.Path(file_okay=False, path_type=Path)
# The artifact types `--emit-agent-spec` writes: each publishes an AGENT from its specification.
_SPEC_TYPES = frozenset(
    (
        "agent",
        # Snowflake private preview (PrPr): CoWork profiles
        "cowork_profile",
        # Snowflake private preview (PrPr): Cortex Code agent profiles
        "coco_profile",
    )
)


@click.command(cls=SstCommand)
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
    selected: tuple[str, ...] | None,
    partial: bool,
    manifest_path: Path | None,
    dbt_dir: Path | None,
    semantic_dir: Path | None,
) -> CommandResult:
    """Compile every artifact and write the canonical manifest, offline.

    The manifest, `sst_target/manifest.json`, always holds everything that compiled; --select
    narrows only what is reported and emitted. --emit-ddl writes each rendered payload, one
    file per artifact under a folder named for its type, as `tool/<name>.sql`; and
    --emit-agent-spec each agent's specification, one file per agent, and each CoWork
    profile's under `cowork_profile/`. With --partial, a compile with errors still writes the
    manifest of the artifacts no error touches, and exits 1. Nothing connects: --target,
    --defer-target, and --database only decide what names resolve to.

    Exit 0 when everything compiled; 1 when an error stopped the compile, the manifest could
    not be written, --partial left an artifact out, or two emitted files would collide; 4 when
    --select matches nothing.

    Diagnostics:
        SST-MAN008: the compile failed, so no manifest was written.
        SST-MAN007: the canonical manifest could not be written.
        SST-PLN033: --partial cannot trace an error to the artifacts it would leave out.
        SST-PRT111: two artifacts of one type would write the same emitted file; nothing is emitted.
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
        ManifestFileStore(destination, root=paths.project_dir).write(manifest)
    except OSError as exc:
        failed = D("SST-MAN007", path=str(exc.filename or destination.parent), detail=exc.strerror or str(exc))
        return CommandResult(ERROR, DiagnosticBag((*shown, failed)))
    narrowed = (
        None
        if selected is None
        else compiling.selected_within(paths.project_dir, healthy, selected, graph=lambda: manifest_lineage(manifest))
    )
    result = healthy if narrowed is None else narrowed.result
    # A selector that matched nothing is shown even when the compile's own warnings are not.
    notes = DiagnosticBag() if narrowed is None else narrowed.report
    data = _compiled_data(result, destination, manifest, excluded)
    emits = tuple(
        (key, folder, *_payload_files(result, agents_only=only))
        for key, folder, only in (("ddl", emit_ddl_dir, False), ("agent_spec", agent_spec_dir, True))
        if folder is not None
    )
    clashes = tuple(diagnostic for *_, found in emits for diagnostic in found)
    if clashes:
        return CommandResult(ERROR, DiagnosticBag((*notes, *shown, *clashes)))
    for key, folder, files, _ in emits:
        directory = project_path(paths, folder)
        root = output_root(paths.project_dir, directory)
        for relative, content in files.items():
            write_text(root, directory / relative, content)
        data.update({f"{key}_dir": str(folder), f"{key}_files": list(files)})
    artifacts = data["artifacts"]
    assert isinstance(artifacts, list)
    return CommandResult(
        OK if split is None else ERROR,
        DiagnosticBag((*notes, *shown)),
        data,
        human=lambda: _print_compiled(data),
        show_diagnostics=split is not None or bool(notes),
        headline=f"compiled {plural(len(artifacts), 'artifact')}",
    )


def _payload_files(result: CompileResult, *, agents_only: bool) -> tuple[dict[str, str], tuple[Diagnostic, ...]]:
    """Name and render each file `--emit-ddl`, or with `agents_only` `--emit-agent-spec`, writes.

    A name is unique only within its type, so `--emit-ddl` puts each payload in a folder named
    for its type; `--emit-agent-spec` writes each agent's JSON specification side by side, and
    each CoWork profile's in a folder named for its type, as `--emit-ddl` does, so a profile
    named for an agent never takes its file. A file is named by `file_name`, so no artifact
    name can place it outside its folder.

    Returns:
        Each file's path below the folder, POSIX, and its content; and one SST-PRT111 for each
        artifact whose file another already names, which is left out.

    Diagnostics:
        SST-PRT111: two artifacts of one type would write the same file.
    """
    files: dict[str, str] = {}
    owners: dict[str, str] = {}
    clashes: list[Diagnostic] = []
    for item in result.compiled:
        if agents_only and item.artifact_type not in _SPEC_TYPES:
            continue
        suffix = artifact_suffix(item.rendered_artifact.render_dialect)
        name = f"{file_name(item.name.casefold())}{suffix}"
        side_by_side = agents_only and item.artifact_type == "agent"
        relative = name if side_by_side else f"{file_name(item.artifact_type)}/{name}"
        if relative in owners:
            clashes.append(
                D("SST-PRT111", subject=item.artifact_key, path=relative, a=owners[relative], b=item.artifact_key)
            )
            continue
        content = item.rendered_artifact.content
        owners[relative] = item.artifact_key
        files[relative] = content if suffix in (".json", ".yaml") else content.rstrip() + ";\n"
    return files, tuple(clashes)


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


def _print_compiled(data: dict[str, object]) -> None:
    """Print where the manifest was written, then where each emitted set of files was."""
    click.echo(f"wrote {data['manifest_path']}")
    for files_key, dir_key in (("ddl_files", "ddl_dir"), ("agent_spec_files", "agent_spec_dir")):
        written = data.get(files_key)
        if isinstance(written, list):
            click.echo(f"wrote {plural(len(written), 'file')} to {data[dir_key]}")
