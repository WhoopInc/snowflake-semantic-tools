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
from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.manifest import dbt_manifest_moved
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
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

    Args:
        database: Resolve refs against this database instead of the target's; None keeps it.

    Diagnostics:
        SST-MAN031: the given dbt manifest's models changed while the project compiled.
    """
    inputs = project_inputs(paths, target_name, manifest_path, database=database)
    if manifest_path is None:
        return CompileProject(inputs).run()
    before = inputs.manifest_sources()
    result = CompileProject(inputs).run()
    moved = dbt_manifest_moved(before, inputs.manifest_sources())
    if moved is None:
        return result
    return dataclasses.replace(result, diagnostics=DiagnosticBag((*result.diagnostics, moved)))


def selected_result(
    project_dir: Path, result: CompileResult, selected: tuple[str, ...], excluded: tuple[str, ...] = ()
) -> CompileResult:
    """Return `result` with only the artifacts `selected` names, less those `excluded` names.

    No selector keeps every artifact, and no exclusion removes any.

    Raises:
        SstUsageError: a selector is refused, as `selection` says.
        ProjectError: the selectors matched no artifact (SST-DIS010).
    """
    return selected_within(project_dir, result, selected, excluded)[0]


def selected_within(
    project_dir: Path, result: CompileResult, selected: tuple[str, ...], excluded: tuple[str, ...] = ()
) -> tuple[CompileResult, SelectionScope]:
    """Return `selected_result`'s narrowing of `result`, with the scope it narrowed to.

    Raises:
        SstUsageError: a selector is refused, as `selection` says.
        ProjectError: the selectors matched no artifact (SST-DIS010).
    """
    universe = compiled_universe(result.compiled)
    scope = selection_scope(selected, excluded, universe)
    compiled = tuple(item for item in result.compiled if scope.covers(item.artifact_type, item.artifact_key))
    if not compiled:
        raise ProjectError(
            f"selector {' '.join(selected)!r} matched no artifact in {project_dir}",
            diagnostics=tuple(D("SST-DIS010", selector=value) for value in selected),
        )
    return dataclasses.replace(result, compiled=compiled), scope


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
    values: tuple[str, ...], universe: tuple[Selectable, ...] = (), previous: Mapping[str, str] | None = None
) -> Selection:
    """Resolve selectors into the artifact types and the artifact keys they name.

    Args:
        universe: The compiled artifacts names, paths, and states resolve against.
        previous: Each key's fingerprint in the `--state` manifest; None without `--state`.

    Raises:
        SstUsageError: a selector is refused, carrying its diagnostic.
    """
    resolved = resolve_selectors(
        values, artifact_types=SEMANTIC_REGISTRY.artifacts, universe=universe, previous=previous
    )
    if isinstance(resolved, Diagnostic):
        raise SstUsageError(resolved.message, diagnostic=resolved)
    return resolved


def selection_scope(
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    universe: tuple[Selectable, ...],
    previous: Mapping[str, str] | None = None,
) -> SelectionScope:
    """Resolve `--select` and `--exclude` into the scope every command applies the same way.

    No selector selects every artifact, and no exclusion leaves any out.

    Raises:
        SstUsageError: a selector is refused, as `selection` says.
    """
    return SelectionScope(
        selection(selected, universe, previous) if selected else None,
        selection(excluded, universe, previous) if excluded else None,
    )


def check_selectors(ctx: click.Context, param: click.Parameter, values: tuple[str, ...] | str | None) -> object:
    """Refuse a malformed `--select` or `--exclude` while the command line is parsed, before anything runs.

    Only the grammar is checked here; what a selector names is resolved once the project is.

    Raises:
        SstUsageError: a selector is refused, carrying its diagnostic.
    """
    given = (values,) if isinstance(values, str) else tuple(values or ())
    resolved = resolve_selectors(given, artifact_types=SEMANTIC_REGISTRY.artifacts, previous={})
    if isinstance(resolved, Diagnostic):
        raise SstUsageError(resolved.message, ctx, diagnostic=resolved)
    return values
