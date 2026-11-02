"""Compile the project from its files, and narrow a compiled result to what a selector names.

Commands call `compile_result` through this module, as `compiling.compile_result(...)`, so
replacing it here changes what every command compiles.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.app import selection as app_selection
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.manifest import dbt_manifest_moved

# Commands and tests read these through this module, as they read every selector here.
from snowflake_semantic_tools.app.selection import Narrowed as Narrowed
from snowflake_semantic_tools.app.selection import SelectorRefused, narrowed
from snowflake_semantic_tools.app.selection import compiled_universe as compiled_universe
from snowflake_semantic_tools.app.selection import manifest_universe as manifest_universe
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.lineage.graph import GraphSource, LineageGraph
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.selectors import Selectable, Selection, SelectionScope, resolve_selectors


def compile_result(
    paths: ProjectPaths, target_name: str | None, manifest_path: Path | None, *, database: str | None = None
) -> CompileResult:
    """Compile every artifact the project declares, from its files.

    With a dbt manifest given, its models are digested before and after the compile, and a
    manifest rewritten in between, by a concurrent `dbt compile`, fails the compile. Without
    one, SST runs `dbt parse` itself and so rewrites the manifest on purpose; nothing is checked.
    The whole compile, any `dbt parse` included, is timed as the run's `load` phase, and each
    step of it as a `load.<step>` detail.

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
        return CompileProject(inputs, phases=RUN_TIMINGS).run()
    with RUN_TIMINGS.phase("load.manifest_sources"):
        before = inputs.manifest_sources()
    result = CompileProject(inputs, phases=RUN_TIMINGS).run()
    with RUN_TIMINGS.phase("load.manifest_sources"):
        moved = dbt_manifest_moved(before, inputs.manifest_sources())
    if moved is None:
        return result
    return dataclasses.replace(result, diagnostics=DiagnosticBag((*result.diagnostics, moved)))


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
    with _usage():
        chosen = narrowed(result, selected, excluded, graph=graph)
    if chosen.misses:
        raise _matched_nothing(project_dir, selected, chosen.misses)
    return chosen


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
        raise _matched_nothing(project_dir, selected, scope.misses(selected, universe))
    return scope, selector_report(selected, excluded, universe, graph)


def _matched_nothing(project_dir: Path, selected: tuple[str, ...], misses: tuple[Diagnostic, ...]) -> ProjectError:
    return ProjectError(f"selector {' '.join(selected)!r} matched no artifact in {project_dir}", diagnostics=misses)


@contextmanager
def _usage() -> Iterator[None]:
    """Refuse a selector the use case refuses as a usage error, carrying its diagnostic."""
    try:
        yield
    except SelectorRefused as refused:
        raise SstUsageError(str(refused), diagnostic=refused.diagnostic) from None


def selector_report(
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    universe: Iterable[Selectable],
    graph: GraphSource | None = None,
) -> DiagnosticBag:
    """Report each selector value that matches no artifact, then what `--exclude` removed.

    As `app.selection.selector_report` reports it.

    Raises:
        SstUsageError: a selector cannot be parsed, as `selection` says.
    """
    with _usage():
        return app_selection.selector_report(selected, excluded, universe, graph)


def selection(
    values: tuple[str, ...],
    universe: tuple[Selectable, ...] = (),
    previous: Mapping[str, str] | None = None,
    *,
    graph: GraphSource | None = None,
) -> Selection:
    """Resolve selectors into the artifact types and the artifact keys they name.

    As `app.selection.selection` resolves them.

    Raises:
        SstUsageError: a selector is refused, carrying its diagnostic.
    """
    with _usage():
        return app_selection.selection(values, universe, previous, graph=graph)


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
    with _usage():
        return app_selection.selection_scope(selected, excluded, universe, previous, graph=graph)


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
