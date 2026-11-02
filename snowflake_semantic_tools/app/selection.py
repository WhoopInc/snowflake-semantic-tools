"""Narrow a compiled project to what `--select` and `--exclude` name, as every command does.

The selectors' grammar and graph walk are `domain.plan.selectors`; this module resolves them
against compiled artifacts and says what they matched. A selector that is refused raises
`SelectorRefused` with its diagnostic, which the CLI turns into a usage error and the library
into a `ProjectError`, so both refuse the same selectors the same way.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.lineage.graph import GraphSource
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.selectors import Selectable, Selection, SelectionScope, resolve_selectors
from snowflake_semantic_tools.domain.state import Manifest


class SelectorRefused(Exception):
    """A selector that cannot be resolved: malformed, or a graph operator with no graph to walk.

    Attributes:
        diagnostic: Why it is refused.
    """

    def __init__(self, diagnostic: Diagnostic) -> None:
        super().__init__(diagnostic.message)
        self.diagnostic = diagnostic


@dataclasses.dataclass(frozen=True, slots=True)
class Narrowed:
    """A compiled result narrowed by `--select` and `--exclude`, with what the selectors' report says.

    Attributes:
        result: The compile result holding only the artifacts chosen.
        scope: The scope `result` was narrowed to.
        report: What `selector_report` says of the selectors. It is never added to `result`'s
            diagnostics, so a narrowed compile reports what an unnarrowed one does.
        misses: Why the selectors chose no artifact at all; empty when they chose some, or when
            none was given.
    """

    result: CompileResult
    scope: SelectionScope
    report: DiagnosticBag
    misses: tuple[Diagnostic, ...]


def narrowed(
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
        SelectorRefused: a selector is refused, as `selection` says.
    """
    universe = compiled_universe(result.compiled)
    scope = selection_scope(selected, excluded, universe, graph=graph)
    chose = any(scope.covers(item.type, item.key) for item in universe)
    misses = scope.misses(selected, universe) if (selected or excluded) and not chose else ()
    compiled = tuple(item for item in result.compiled if scope.covers(item.artifact_type, item.artifact_key))
    return Narrowed(
        dataclasses.replace(result, compiled=compiled),
        scope,
        selector_report(selected, excluded, universe, graph),
        misses,
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
        SelectorRefused: a selector is refused, carrying its diagnostic.
    """
    resolved = resolve_selectors(
        values, artifact_types=SEMANTIC_REGISTRY.artifacts, universe=universe, previous=previous, graph=graph
    )
    if isinstance(resolved, Diagnostic):
        raise SelectorRefused(resolved)
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
        SelectorRefused: a selector is refused, as `selection` says.
    """
    return SelectionScope(
        selection(selected, universe, previous, graph=graph) if selected else None,
        selection(excluded, universe, previous, graph=graph) if excluded else None,
    )


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
        SelectorRefused: a selector cannot be parsed, as `selection` says.

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
