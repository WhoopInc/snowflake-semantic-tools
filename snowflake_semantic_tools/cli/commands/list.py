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
import json
from collections.abc import Mapping
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
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
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.options import (
    database_option,
    defer_target_option,
    model_path_options,
    selection_options,
    target_option,
    with_model_paths,
)
from snowflake_semantic_tools.cli.output import render_policy
from snowflake_semantic_tools.cli.render.diagnostics import plural
from snowflake_semantic_tools.cli.render.report import table
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.compile import checked_scope, manifest_universe
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest, compiled_manifest
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.selectors import SelectionScope
from snowflake_semantic_tools.domain.state import Manifest
from snowflake_semantic_tools.domain.state.manifest import ArtifactEntry

LIST_OUTPUTS = ("table", "plain", "agent", "json", "yaml", "csv")


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
@target_option()
@defer_target_option()
@database_option()
@selection_options()
@click.option("--long", "long_format", is_flag=True)
@click.option("--no-manifest", is_flag=True)
@model_path_options()
@command_body("list", outputs=LIST_OUTPUTS)
def list_command(
    paths: ProjectPaths,
    artifact_type: str | None,
    target_name: str | None,
    database: str | None,
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
    metrics or verified-queries, or tables. An artifact's status -- pending, applied,
    failed, or deactivated -- is what the one local state file under `target/sst` records;
    with none there, or more than one target's, every artifact is pending. Without
    --no-manifest the manifest `sst compile` wrote is read, and its absence exits 4; with
    it, the project's files are compiled in memory, nothing is written, and what fails to
    compile is reported and exits 1. A --select or --exclude value that matches nothing is
    reported; when the selectors choose no artifact and nothing failed to compile, it exits 4.
    --target, --defer-target and --database resolve the in-memory compile as `sst compile`
    would, offline; with a compiled manifest, a --target other than the one it was compiled
    for is refused (SST-MAN006).
    """
    paths = with_model_paths(paths, dbt_dir, semantic_dir)
    if no_manifest:
        compiled = compiling.compile_result(paths, target_name, manifest_path, database=database)
        manifest = build_manifest(paths, compiled, manifest_path, target_name)
        # What failed to compile is missing from the list, so its errors are reported, exit 1.
        found: tuple[Diagnostic, ...] = tuple(compiled.diagnostics)
    else:
        manifest = compiled_manifest(paths.project_dir)
        _refuse_another_target(manifest, target_name)
        found = read_notes(manifest, str(target_dir(paths.project_dir) / "manifest.json"))
    failed = DiagnosticBag(found).has_errors
    universe = manifest_universe(manifest)
    scope, report = checked_scope(paths.project_dir, selected, excluded, universe, refuse=not failed)
    notes = (*found, *report)
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
    entries = {item.key: manifest.artifacts[item.key] for item in summaries} if long_format else {}
    items = [_item(item, entries.get(item.key)) for item in summaries]
    data = {"type": artifact_type, "items": items, "count": len(items)}
    return CommandResult(
        _exit_code(notes),
        DiagnosticBag(notes),
        data,
        human=lambda: _print_summaries(summaries, entries),
        rows=items,
        gated=True,
        headline=f"listed {plural(len(items), 'artifact')}",
    )


# How much of a fingerprint the table shows.
_SHORT = 12


def _refuse_another_target(manifest: Manifest, target_name: str | None) -> None:
    """Refuse a --target other than the one the compiled manifest was compiled for.

    A manifest compiled without a target, or a run with none, agrees with any.

    Raises:
        ProjectError: the manifest records another target (SST-MAN006).
    """
    found = str(manifest.project.get("target") or "")
    if target_name and found and found != target_name:
        mismatch = D("SST-MAN006", found=found, expected=target_name)
        raise ProjectError(mismatch.message, diagnostics=(mismatch,))


def _item(summary: ArtifactSummary, entry: ArtifactEntry | None) -> dict[str, object]:
    """One listed artifact; with `--long`, its source files and its settings with their sources too."""
    item = dataclasses.asdict(summary)
    if entry is not None:
        item["source_files"] = list(entry.source_files)
        item["settings"] = {setting.key: setting.as_dict() for setting in entry.settings}
    return item


def _print_summaries(summaries: tuple[ArtifactSummary, ...], entries: Mapping[str, ArtifactEntry]) -> None:
    """Print a table of the artifacts; with `--long`, each row is followed by its settings and their sources.

    Fingerprints are cut to their first 12 characters, enough to tell them apart by eye; JSON
    and CSV carry them whole.
    """
    header = ["ARTIFACT", "STATUS", "TARGET", "FINGERPRINT"] + (["APPLIED", "VERSION"] if entries else [])
    rows = [header]
    for item in summaries:
        row = [item.key, item.status, item.target, item.fingerprint[:_SHORT]]
        if entries:
            row += [(item.applied_fingerprint or "-")[:_SHORT], str(item.version or "-")]
        rows.append(row)
    terminal = render_policy().terminal
    style = Style(terminal, stdout=True)
    lines = table(rows, terminal.width)
    click.echo(style(lines[0], "dim"), color=True if terminal.color_out else None)
    for item, line in zip(summaries, lines[1:], strict=True):
        click.echo(line)
        entry = entries.get(item.key)
        for setting in entry.settings if entry is not None else ():
            click.echo(f"    {setting.key}={json.dumps(setting.value, sort_keys=True)} ({setting.source})")


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
        _exit_code(notes),
        DiagnosticBag(notes),
        {"type": list_type, "items": rows, "count": len(rows)},
        human=lambda: _print_members(members),
        rows=rows,
        gated=True,
        headline=f"listed {plural(len(rows), 'table' if list_type == 'tables' else 'member')}",
    )


def _exit_code(diagnostics: tuple[Diagnostic, ...]) -> int:
    """Exit 1 when an error is reported, such as a file `--no-manifest` could not compile."""
    return ERROR if DiagnosticBag(diagnostics).has_errors else OK


def _member_item(summary: MemberSummary) -> dict[str, object]:
    item: dict[str, object] = {"key": summary.key, "name": summary.name, "artifacts": list(summary.artifacts)}
    if summary.relation:
        item["relation"] = summary.relation
    return item


def _print_members(members: tuple[MemberSummary, ...]) -> None:
    """Print a table of the members: each key, its relation when it has one, and its artifacts."""
    related = any(item.relation for item in members)
    header = ["MEMBER", *(["RELATION"] if related else []), "ARTIFACTS"]
    rows = [header] + [
        [item.key, *([item.relation or "-"] if related else []), ", ".join(item.artifacts) or "-"] for item in members
    ]
    terminal = render_policy().terminal
    lines = table(rows, terminal.width)
    click.echo(Style(terminal, stdout=True)(lines[0], "dim"), color=True if terminal.color_out else None)
    for line in lines[1:]:
        click.echo(line)
