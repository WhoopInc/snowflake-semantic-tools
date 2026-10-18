"""`sst lineage NODE`: what one node of the project's lineage reads, and what reads it, offline.

NODE is an artifact key, a member key such as `metric:<name>`, `model:<name>`,
`source:<source>.<table>` or `exposure:<name>`, or a bare name, which names every node of that
name; a key's name may glob. The graph is the one the compiled manifest records, or with
`--no-manifest` the one the project compiles to now. `--state` marks what changed since another
manifest: an artifact by its fingerprint, a member by the fingerprints its views record.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.manifest import read_notes
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    database_option,
    defer_target_option,
    model_path_options,
    state_option,
    target_option,
    with_model_paths,
)
from snowflake_semantic_tools.cli.output import render_policy
from snowflake_semantic_tools.cli.render.diagnostics import plural
from snowflake_semantic_tools.cli.render.report import table
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.manifest import build_manifest, compiled_manifest, refuse_another_target
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.lineage.graph import manifest_lineage
from snowflake_semantic_tools.domain.lineage.trace import (
    DOWNSTREAM,
    UPSTREAM,
    Trace,
    changed_nodes,
    find_nodes,
    near_miss,
    trace,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.state import Manifest

LINEAGE_OUTPUTS = ("table", "plain", "agent", "json")


@click.command(cls=SstCommand, name="lineage")
@click.argument("node", metavar="NODE")
@click.option("--upstream", "direction", flag_value=UPSTREAM)
@click.option("--downstream", "direction", flag_value=DOWNSTREAM)
@click.option("--depth", type=click.IntRange(min=1))
@state_option()
@target_option()
@defer_target_option()
@database_option()
@click.option("--no-manifest", is_flag=True)
@model_path_options()
@command_body("lineage", outputs=LINEAGE_OUTPUTS)
def lineage_command(
    paths: ProjectPaths,
    node: str,
    direction: str | None,
    depth: int | None,
    state_dir: Path | None,
    target_name: str | None,
    database: str | None,
    no_manifest: bool,
    dbt_dir: Path | None,
    semantic_dir: Path | None,
    manifest_path: Path | None,
) -> CommandResult:
    """Show what NODE reads and what reads it: dbt models and sources, members, and artifacts.

    NODE is an artifact key such as semantic_view:sales, a member key such as
    metric:total_revenue, model:<name>, source:<source>.<table>, exposure:<name>, or a bare name,
    which names every node of that name; the name after a kind may glob. Both directions are
    shown unless --upstream or --downstream picks one, as far as the graph goes unless --depth
    stops it. Without --no-manifest the manifest `sst compile` wrote is read, and its absence
    exits 4; with it, the project is compiled in memory, nothing is written, and what fails to
    compile is reported and exits 1. --state marks what changed since that directory's
    manifest. A NODE the graph does not hold exits 3 (SST-PRT112).
    """
    paths = with_model_paths(paths, dbt_dir, semantic_dir)
    if no_manifest:
        compiled = compiling.compile_result(paths, target_name, manifest_path, database=database)
        manifest = build_manifest(paths, compiled, manifest_path, target_name)
        notes: tuple[Diagnostic, ...] = tuple(compiled.diagnostics)
    else:
        manifest = compiled_manifest(paths.project_dir)
        refuse_another_target(manifest, target_name)
        notes = read_notes(manifest, str(target_dir(paths.project_dir) / "manifest.json"))
    graph = manifest_lineage(manifest)
    start = find_nodes(graph, node, SEMANTIC_REGISTRY.artifacts)
    if not start:
        close = near_miss(graph, node)
        refusal = D("SST-PRT112", subject="cli", value=node, detail=f"; did you mean {close}?" if close else "")
        raise SstUsageError(refusal.message, diagnostic=refusal)
    changed = changed_nodes(_previous(state_dir), manifest) if state_dir is not None else frozenset()
    traced = trace(
        graph, start, upstream=direction != DOWNSTREAM, downstream=direction != UPSTREAM, depth=depth, changed=changed
    )
    data = _data(node, start, traced, compared=state_dir is not None)
    return CommandResult(
        ERROR if DiagnosticBag(notes).has_errors else OK,
        DiagnosticBag(notes),
        data,
        human=lambda: _print(traced, compared=state_dir is not None),
        gated=True,
        headline=f"traced {plural(len(traced.nodes), 'node')} from {', '.join(start)}",
    )


def _previous(state_dir: Path) -> Manifest:
    """Return the manifest in `state_dir`, which the trace marks changes since.

    Raises:
        ProjectError: the directory holds no SST manifest (SST-MAN001).
    """
    path = state_dir / "manifest.json"
    manifest = ManifestFileStore(path).read()
    if manifest is None:
        diagnostic = D("SST-MAN001", path=str(path))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return manifest


def _data(node: str, start: tuple[str, ...], traced: Trace, *, compared: bool) -> dict[str, object]:
    """Return the trace as `--output json` lists it: each node, then each edge between two of them."""
    nodes = []
    for item in traced.nodes:
        entry: dict[str, object] = {
            "key": item.node.key,
            "kind": item.node.kind,
            "name": item.node.name,
            "direction": item.direction,
            "depth": item.depth,
            "relation": item.node.relation,
        }
        if compared:
            entry["changed"] = item.changed
        nodes.append(entry)
    edges = [{"from": parent, "to": child} for parent, child in traced.edges]
    return {"node": node, "start": list(start), "nodes": nodes, "edges": edges, "count": len(nodes)}


def _print(traced: Trace, *, compared: bool) -> None:
    """Print a table of the nodes: starts first, then upstream and downstream, each by depth."""
    header = ["DIRECTION", "DEPTH", "NODE", "KIND", "RELATION", *(["CHANGED"] if compared else [])]
    rows = [header] + [
        [
            item.direction,
            str(item.depth),
            item.node.key,
            item.node.kind,
            item.node.relation or "-",
            *(["yes" if item.changed else "-"] if compared else []),
        ]
        for item in traced.nodes
    ]
    terminal = render_policy().terminal
    lines = table(rows, terminal.width, keep=2)
    click.echo(Style(terminal, stdout=True)(lines[0], "dim"), color=True if terminal.color_out else None)
    for line in lines[1:]:
        click.echo(line)
