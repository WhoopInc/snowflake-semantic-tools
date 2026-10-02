"""`sst list`: the compiled artifacts, with the status the local state file records for each.

`TYPE` is a registered artifact type, so a newly registered type is listable without editing this
command. `--select` and `--exclude` take the same selectors as `sst plan`. Besides `table`, `plain`
and `json`, `list` prints `yaml`, the envelope as YAML, and `csv`, one row per artifact.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.fs.local import STATE_FILE_GLOB, StateFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.listing import ArtifactSummary, list_artifacts
from snowflake_semantic_tools.app.manifest import read_notes
from snowflake_semantic_tools.cli.options import model_path_options, selection_options, with_model_paths
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.compile import manifest_universe, selection
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest, compiled_manifest
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.selectors import Selectable

LIST_OUTPUTS = ("table", "plain", "json", "yaml", "csv")


@click.command(name="list")
@click.argument(
    "artifact_type", metavar="[TYPE]", required=False, type=click.Choice(sorted(SEMANTIC_REGISTRY.artifacts))
)
@selection_options()
@click.option("--long", "long_format", is_flag=True)
@click.option("--no-manifest", is_flag=True)
@model_path_options()
@command_body("list", outputs=LIST_OUTPUTS)
def list_command(
    paths: ProjectPaths,
    artifact_type: str | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    long_format: bool,
    no_manifest: bool,
    dbt_dir: Path | None,
    semantic_dir: Path | None,
    manifest_path: Path | None,
) -> CommandResult:
    """List compiled artifacts and their cached application status, optionally of one TYPE.

    With --no-manifest the project's files are compiled in memory instead of reading the
    manifest `sst compile` wrote, and nothing is written.
    """
    paths = with_model_paths(paths, dbt_dir, semantic_dir)
    if no_manifest:
        manifest = build_manifest(paths, compiling.compile_result(paths, None, manifest_path), manifest_path)
    else:
        manifest = compiled_manifest(paths.project_dir)
    notes = read_notes(manifest, str(target_dir(paths.project_dir) / "manifest.json"))
    states = tuple(sorted(target_dir(paths.project_dir).glob(STATE_FILE_GLOB)))
    state = StateFileStore(states[0]).read_local() if len(states) == 1 else None
    universe = manifest_universe(manifest)
    chosen = _chosen(selected, universe)
    left_out = _chosen(excluded, universe) or frozenset()
    summaries = tuple(
        item
        for item in list_artifacts(manifest, state)
        if (artifact_type is None or split_artifact_key(item.key)[0] == artifact_type)
        and (chosen is None or item.key in chosen)
        and item.key not in left_out
    )
    items = [_item(item, manifest.artifacts[item.key].source_files if long_format else None) for item in summaries]
    data = {"type": artifact_type, "items": items, "count": len(items)}
    return CommandResult(
        diagnostics=DiagnosticBag(notes),
        data=data,
        human=lambda: _print_summaries(summaries, long_format),
        rows=items,
    )


def _chosen(values: tuple[str, ...], universe: tuple[Selectable, ...]) -> frozenset[str] | None:
    """Return the keys the selectors name, a type expanding to its artifacts; None without selectors."""
    if not values:
        return None
    types, keys = selection(values, universe)
    named = set(keys or ())
    named.update(item.key for item in universe if types and item.type in types)
    return frozenset(named)


def _item(summary: ArtifactSummary, source_files: tuple[str, ...] | None) -> dict[str, object]:
    item = dataclasses.asdict(summary)
    if source_files is not None:
        item["source_files"] = list(source_files)
    return item


def _print_summaries(summaries: tuple[ArtifactSummary, ...], long_format: bool) -> None:
    for item in summaries:
        line = f"{item.key} {item.status} {item.target} {item.fingerprint}"
        if long_format:
            line += f" applied={item.applied_fingerprint or '-'} version={item.version or '-'}"
        click.echo(line)
