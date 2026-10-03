"""`sst list`: the compiled artifacts, with the status the local state file records for each.

`TYPE` names a registered artifact or member type in the plural, hyphenated -- `semantic-views`,
`metrics`, `verified-queries` -- or `tables`, the dbt models the artifacts read; a newly
registered type is listable without editing this command. A member or table is listed with the
artifacts it attaches to or is read by. `--select` and `--exclude` take the same selectors as
`sst plan`, and keep a member when an artifact it belongs to is kept. Besides `table`, `plain`
and `json`, `list` prints `yaml`, the envelope as YAML, and `csv`, one row per item.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.fs.local import STATE_FILE_GLOB, StateFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.listing import (
    ArtifactSummary,
    MemberSummary,
    list_artifacts,
    list_members,
    list_tables,
)
from snowflake_semantic_tools.app.manifest import read_notes
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.options import model_path_options, selection_options, with_model_paths
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.compile import manifest_universe, selection_scope
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest, compiled_manifest
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.selectors import SelectionScope

LIST_OUTPUTS = ("table", "plain", "json", "yaml", "csv")


def type_name(registered: str) -> str:
    """Name a registered type as `TYPE` spells it: plural and hyphenated, as `verified-queries`."""
    stem = registered.replace("_", "-")
    return f"{stem[:-1]}ies" if stem.endswith("y") else f"{stem}s"


# Each `TYPE`, and what it lists: an artifact type, a member type, or the tables.
LIST_TYPES: dict[str, tuple[str, str]] = {
    **{type_name(name): ("artifact", name) for name in SEMANTIC_REGISTRY.artifacts},
    **{type_name(name): ("member", name) for name in SEMANTIC_REGISTRY.members},
    "tables": ("table", "table"),
}


@click.command(cls=SstCommand, name="list")
@click.argument("artifact_type", metavar="[TYPE]", required=False, type=click.Choice(sorted(LIST_TYPES)))
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

    TYPE is an artifact type such as semantic-views or agents, a member type such as
    metrics or verified-queries, or tables. With --no-manifest the project's files are
    compiled in memory instead of reading the manifest `sst compile` wrote, and nothing
    is written.
    """
    paths = with_model_paths(paths, dbt_dir, semantic_dir)
    if no_manifest:
        manifest = build_manifest(paths, compiling.compile_result(paths, None, manifest_path), manifest_path)
    else:
        manifest = compiled_manifest(paths.project_dir)
    notes = read_notes(manifest, str(target_dir(paths.project_dir) / "manifest.json"))
    universe = manifest_universe(manifest)
    scope = selection_scope(selected, excluded, universe)
    kind, registered = LIST_TYPES[artifact_type] if artifact_type is not None else ("artifact", None)
    if kind != "artifact":
        listed = list_tables(manifest) if kind == "table" else list_members(manifest, registered or "")
        return _member_result(artifact_type, listed, scope, notes)
    states = tuple(sorted(target_dir(paths.project_dir).glob(STATE_FILE_GLOB)))
    state = StateFileStore(states[0]).read_local() if len(states) == 1 else None
    summaries = tuple(
        item
        for item in list_artifacts(manifest, state)
        if (registered is None or split_artifact_key(item.key)[0] == registered)
        and scope.covers(split_artifact_key(item.key)[0], item.key)
    )
    items = [_item(item, manifest.artifacts[item.key].source_files if long_format else None) for item in summaries]
    data = {"type": artifact_type, "items": items, "count": len(items)}
    return CommandResult(
        diagnostics=DiagnosticBag(notes),
        data=data,
        human=lambda: _print_summaries(summaries, long_format),
        rows=items,
    )


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


def _member_result(
    list_type: str | None,
    listed: tuple[MemberSummary, ...],
    scope: SelectionScope,
    notes: tuple[Diagnostic, ...],
) -> CommandResult:
    """List members or tables: those belonging to an artifact the scope keeps.

    One that belongs to no artifact is kept only when no `--select` was given.
    """
    members = tuple(
        item
        for item in listed
        if any(scope.covers(split_artifact_key(key)[0], key) for key in item.artifacts)
        or (not item.artifacts and scope.selected is None)
    )
    rows = [_member_item(item) for item in members]
    return CommandResult(
        diagnostics=DiagnosticBag(notes),
        data={"type": list_type, "items": rows, "count": len(rows)},
        human=lambda: _print_members(members),
        rows=rows,
    )


def _member_item(summary: MemberSummary) -> dict[str, object]:
    item: dict[str, object] = {"key": summary.key, "name": summary.name, "artifacts": list(summary.artifacts)}
    if summary.relation:
        item["relation"] = summary.relation
    return item


def _print_members(members: tuple[MemberSummary, ...]) -> None:
    for item in members:
        relation = f" {item.relation}" if item.relation else ""
        click.echo(f"{item.key}{relation} {', '.join(item.artifacts) or '-'}")
