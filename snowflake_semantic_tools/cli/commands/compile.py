"""`sst compile`: render every artifact and write the canonical manifest, offline.

The canonical manifest, `target/sst/manifest.json`, always holds everything that compiled,
even with `--select`; `--manifest-output` writes another copy, of the selection when there
is one. `--print-ddl` and `--emit-ddl` act only for human output.
"""

from __future__ import annotations

from pathlib import Path

import click

from ...adapters.fs.local import ManifestFileStore
from ...app.compile import CompileResult
from ...app.partial import partial_refusal, partial_split
from ...domain.model.diagnostic import DiagnosticBag
from ...domain.state import Manifest
from ..exit_codes import ERROR, OK
from ..options import manifest_option, output_option, partial_option, project_dir_option, select_option, target_option
from ..plan_output import artifact_suffix
from ..runner import CommandResult, command_body
from ..wiring import compile as compiling
from ..wiring.manifest import build_manifest
from ..wiring.project import target_dir


@click.command()
@project_dir_option()
@click.option("--emit-ddl", "emit_ddl_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--print-ddl", is_flag=True)
@click.option("--ddl-output-dir", type=click.Path(file_okay=False, path_type=Path), hidden=True)
@click.option("--manifest-output", type=click.Path(dir_okay=False, path_type=Path))
@select_option(multiple=False)
@partial_option()
@target_option()
@manifest_option()
@output_option()
@command_body("compile")
def compile(
    project_dir: Path,
    emit_ddl_dir: Path | None,
    print_ddl: bool,
    ddl_output_dir: Path | None,
    manifest_output: Path | None,
    selected: str | None,
    partial: bool,
    target_name: str | None,
    manifest_path: Path | None,
    output: str,
) -> CommandResult:
    """Compile semantic views and write the canonical manifest."""
    full_result = compiling.compile_result(project_dir, target_name, manifest_path)
    split = partial_split(full_result) if partial and not full_result.success else None
    if not full_result.success and split is None:
        refusal = partial_refusal(full_result) if partial else None
        return CommandResult(ERROR, DiagnosticBag((*full_result.diagnostics, *((refusal,) if refusal else ()))))
    # `--partial` writes the manifest for what can publish and still exits 1.
    healthy = full_result if split is None else split.healthy
    shown = full_result.diagnostics if split is None else DiagnosticBag((*healthy.diagnostics, *split.notices))
    excluded = None if split is None else split.excluded
    result, destination, manifest = _write_manifests(project_dir, healthy, selected, manifest_output, manifest_path)
    ddl_dir = emit_ddl_dir or ddl_output_dir
    return CommandResult(
        OK if split is None else ERROR,
        shown,
        _compiled_data(result, destination, manifest, excluded),
        human=lambda: _print_compiled(result, destination, excluded, print_ddl=print_ddl, ddl_dir=ddl_dir),
        show_diagnostics=split is not None,
    )


def _write_manifests(
    project_dir: Path,
    result: CompileResult,
    selected: str | None,
    manifest_output: Path | None,
    manifest_path: Path | None,
) -> tuple[CompileResult, Path, Manifest]:
    """Write the canonical manifest and any `--manifest-output`; return what to report, and where.

    With `--select`, the reported result is the selection, and `--manifest-output` holds
    only that; the canonical manifest is written before the selector is resolved.

    Raises:
        SstUsageError: the selector cannot be parsed.
        ProjectError: the selector matched no artifact.
    """
    default_manifest = target_dir(project_dir) / "manifest.json"
    full_manifest = build_manifest(project_dir, result, manifest_path)
    ManifestFileStore(default_manifest).write(full_manifest)
    if selected is not None:
        chosen = compiling.selected_result(project_dir, result, selected)
        if manifest_output is None:
            return chosen, default_manifest, full_manifest
        manifest = build_manifest(project_dir, chosen, manifest_path)
        ManifestFileStore(manifest_output).write(manifest)
        return chosen, manifest_output, manifest
    if manifest_output is not None and manifest_output != default_manifest:
        ManifestFileStore(manifest_output).write(full_manifest)
        return result, manifest_output, full_manifest
    return result, default_manifest, full_manifest


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
    data: dict[str, object] = {
        "manifest_path": str(destination),
        "manifest_id": manifest.manifest_id,
        "artifacts": artifacts,
    }
    if excluded is not None:
        data["partial"] = {"excluded": list(excluded)}
    return data


def _print_compiled(
    result: CompileResult,
    destination: Path,
    excluded: tuple[str, ...] | None,
    *,
    print_ddl: bool,
    ddl_dir: Path | None,
) -> None:
    """Print the rendered DDL, or write one payload file per artifact into `ddl_dir`, or a summary."""
    if print_ddl:
        click.echo("\n\n".join(item.rendered_artifact.content for item in result.compiled))
    elif ddl_dir is not None:
        ddl_dir.mkdir(parents=True, exist_ok=True)
        for item in result.compiled:
            suffix = artifact_suffix(item.rendered_artifact.render_dialect)
            (ddl_dir / f"{item.name.casefold()}{suffix}").write_text(
                (
                    item.rendered_artifact.content
                    if suffix in (".json", ".yaml")
                    else item.rendered_artifact.content.rstrip() + ";\n"
                ),
                encoding="utf-8",
            )
        click.echo(f"wrote {len(result.compiled)} artifact payload file(s) to {ddl_dir}")
    else:
        partial_note = f" (partial: {len(excluded)} excluded)" if excluded is not None else ""
        click.echo(f"compiled {len(result.compiled)} artifact(s){partial_note}; manifest {destination}")
