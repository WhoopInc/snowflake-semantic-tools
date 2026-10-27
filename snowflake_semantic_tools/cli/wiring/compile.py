"""Compile the project from its files, and narrow a compiled result to what a selector names.

Commands call `compile_result` through this module, as `compiling.compile_result(...)`, so
replacing it here changes what every command compiles.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.manifest import dbt_manifest_moved
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.lineage.graph import GraphSource, LineageGraph
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.selectors import Selectable, Selection, SelectionScope, resolve_selectors
from snowflake_semantic_tools.domain.state import Manifest


def compile_result(
    paths: ProjectPaths, target_name: str | None, manifest_path: Path | None, *, database: str | None = None
) -> CompileResult:
    """Compile every artifact the project declares, from its files.

    With a dbt manifest given, its models are digested before and after the compile, and a
    manifest rewritten in between, by a concurrent `dbt compile`, fails the compile. Without
    one, SST runs `dbt parse` itself and so rewrites the manifest on purpose; nothing is checked.
    The whole compile, any `dbt parse` included, is timed as the run's `load` phase.

    Args:
        database: Resolve refs against this database instead of the target's; None keeps it.

    Diagnostics:
        SST-MAN031: the given dbt manifest's models changed while the project compiled.
    """
    with RUN_TIMINGS.phase("load"):
        return _compile_result(paths, target_name, manifest_path, database)


def _compile_result(
    paths: ProjectPaths, target_name: str | None, manifest_path: Path | None, database: str | None
) -> CompileResult:
    inputs = project_inputs(paths, target_name, manifest_path, database=database)
    if manifest_path is None:
        return CompileProject(inputs).run()
    before = inputs.manifest_sources()
    result = CompileProject(inputs).run()
    moved = dbt_manifest_moved(before, inputs.manifest_sources())
    if moved is None:
        return result
    return dataclasses.replace(result, diagnostics=DiagnosticBag((*result.diagnostics, moved)))


@dataclasses.dataclass(frozen=True, slots=True)
class Narrowed:
    """A compiled result narrowed by `--select` and `--exclude`, with what the selectors' report says.

    Attributes:
        result: The compile result holding only the artifacts chosen.
        scope: The scope `result` was narrowed to.
        report: What `selector_report` says of the selectors. It is never added to `result`'s
            diagnostics, so a narrowed compile reports what an unnarrowed one does.
    """

    result: CompileResult
    scope: SelectionScope
    report: DiagnosticBag


def selected_within(
    project_dir: Path,
    result: CompileResult,
    selected: tuple[str, ...],
    excluded: tuple[str, ...] = (),
    *,
    graph: GraphSource | None = None,
) -> Narrowed:
    """Return `result` with only the artifacts `selected` names, less those `excluded` names.

    No selector keeps every artifact, and no exclusion removes any. `graph` builds the lineage a
    selector with graph operators walks; without it, such a selector is refused.

    Raises:
        SstUsageError: a selector is refused, as `selection` says.
        ProjectError: the selectors matched no artifact (SST-DIS010).
    """
    universe = compiled_universe(result.compiled)
    scope, report = checked_scope(project_dir, selected, excluded, universe, graph=graph)
    compiled = tuple(item for item in result.compiled if scope.covers(item.artifact_type, item.artifact_key))
    return Narrowed(dataclasses.replace(result, compiled=compiled), scope, report)


def checked_scope(
    project_dir: Path,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    universe: tuple[Selectable, ...],
    *,
    refuse: bool = True,
    graph: GraphSource | None = None,
) -> tuple[SelectionScope, DiagnosticBag]:
    """Resolve the selectors over `universe`, with `selector_report`'s report, refusing a scope of nothing.

    Args:
        refuse: False to report a scope of nothing only as the report does, as where the
            compile's own errors explain why an artifact is not there to select.
        graph: As `selection` takes it.

    Raises:
        SstUsageError: a selector is refused, as `selection` says.
        ProjectError: selectors were given and the scope chooses no artifact (SST-DIS010).

    Diagnostics:
        SST-DIS010: raised, one per `--select` value, when the scope chooses no artifact.
        SST-DIS015: raised instead when `--select` chose artifacts and `--exclude` removed them.
    """
    scope = selection_scope(selected, excluded, universe, graph=graph)
    if refuse and (selected or excluded) and not any(scope.covers(item.type, item.key) for item in universe):
        raise ProjectError(
            f"selector {' '.join(selected)!r} matched no artifact in {project_dir}",
            diagnostics=scope.misses(selected, universe),
        )
    return scope, selector_report(selected, excluded, universe, graph)


def selector_report(
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    universe: Iterable[Selectable],
    graph: GraphSource | None = None,
) -> DiagnosticBag:
    """Report each selector value that matches no artifact, then what `--exclude` removed.

    A `state:` selector names no artifact of this compile, only a change since another, so it
    is not reported.

    Args:
        universe: The artifacts the selectors resolve against: a compile's, or a manifest's.
        graph: As `selection` takes it.

    Raises:
        SstUsageError: a selector cannot be parsed, as `selection` says.

    Diagnostics:
        SST-DIS010: a `--select` value matched no artifact, one per value in the order given.
        SST-DIS011: an `--exclude` value matched no artifact, one per value in the order given.
        SST-DIS201: `--exclude` left artifacts out, counting their distinct source files.
    """
    artifacts = tuple(universe)
    selected = tuple(value for value in selected if not _is_state(value))
    excluded = tuple(value for value in excluded if not _is_state(value))
    found = [D("SST-DIS010", selector=value) for value in selected if not _matching(artifacts, (value,), graph)]
    found.extend(D("SST-DIS011", selector=value) for value in excluded if not _matching(artifacts, (value,), graph))
    left_out = _matching(artifacts, excluded, graph) if excluded else ()
    if left_out:
        found.append(D("SST-DIS201", count=len({path for item in left_out for path in item.source_files})))
    return DiagnosticBag(found)


def _matching(
    artifacts: tuple[Selectable, ...], selectors: tuple[str, ...], graph: GraphSource | None
) -> tuple[Selectable, ...]:
    named = selection(selectors, artifacts, graph=graph)
    return tuple(item for item in artifacts if named.names(item.type, item.key))


def _is_state(selector: str) -> bool:
    return selector.casefold().startswith("state:")


def compiled_universe(compiled: Iterable[CompiledArtifact]) -> tuple[Selectable, ...]:
    """Return each compiled artifact as a selector can name it."""
    return tuple(
        Selectable(
            item.artifact_key,
            item.artifact_type,
            item.name.casefold(),
            item.rendered_artifact.fingerprint,
            item.source_files,
        )
        for item in compiled
    )


def manifest_universe(manifest: Manifest) -> tuple[Selectable, ...]:
    """Return every artifact of a compiled manifest as a selector can name it."""
    return tuple(
        Selectable(key, entry.type, entry.name, entry.fingerprint, entry.source_files)
        for key, entry in sorted(manifest.artifacts.items())
    )


def selection(
    values: tuple[str, ...],
    universe: tuple[Selectable, ...] = (),
    previous: Mapping[str, str] | None = None,
    *,
    graph: GraphSource | None = None,
) -> Selection:
    """Resolve selectors into the artifact types and the artifact keys they name.

    Args:
        universe: The compiled artifacts names, paths, and states resolve against.
        previous: Each key's fingerprint in the `--state` manifest; None without `--state`.
        graph: Builds the lineage a selector with graph operators walks, once and only when one
            is given; None refuses such a selector.

    Raises:
        SstUsageError: a selector is refused, carrying its diagnostic.
    """
    resolved = resolve_selectors(
        values, artifact_types=SEMANTIC_REGISTRY.artifacts, universe=universe, previous=previous, graph=graph
    )
    if isinstance(resolved, Diagnostic):
        raise SstUsageError(resolved.message, diagnostic=resolved)
    return resolved


def selection_scope(
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    universe: tuple[Selectable, ...],
    previous: Mapping[str, str] | None = None,
    *,
    graph: GraphSource | None = None,
) -> SelectionScope:
    """Resolve `--select` and `--exclude` into the scope every command applies the same way.

    No selector selects every artifact, and no exclusion leaves any out.

    Raises:
        SstUsageError: a selector is refused, as `selection` says.
    """
    return SelectionScope(
        selection(selected, universe, previous, graph=graph) if selected else None,
        selection(excluded, universe, previous, graph=graph) if excluded else None,
    )


def check_selectors(
    ctx: click.Context, param: click.Parameter, values: tuple[str, ...] | str | None
) -> tuple[str, ...] | None:
    """Refuse a malformed `--select` or `--exclude` while the command line is parsed, before anything runs.

    Each value is split on whitespace, so `--select "a b"` is `--select a --select b`, and
    click passes the command the split values; a value of only whitespace is kept as given.
    Only the grammar is checked here; what a selector names is resolved once the project is.

    Returns:
        Every selector given, in order; None when the option was not given at all.

    Raises:
        SstUsageError: a selector is refused, carrying its diagnostic.
    """
    if values is None:
        return None
    given = tuple(part for value in ((values,) if isinstance(values, str) else values) for part in _split(value))
    # An empty graph checks a graph operator's grammar; what it walks is resolved later.
    resolved = resolve_selectors(
        given, artifact_types=SEMANTIC_REGISTRY.artifacts, previous={}, graph=lambda: LineageGraph()
    )
    if isinstance(resolved, Diagnostic):
        raise SstUsageError(resolved.message, ctx, diagnostic=resolved)
    return given


def _split(value: str) -> list[str]:
    return value.split() or [value]
