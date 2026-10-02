"""What `--select` and `--exclude` did to a compiled project.

A selector that matched nothing is reported, and so is how many files `--exclude` left out.
"""

from __future__ import annotations

from collections.abc import Iterable

from snowflake_semantic_tools.app.compile import CompiledArtifact
from snowflake_semantic_tools.cli.wiring.compile import selection
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.plan.selectors import Selectable


def selector_report(
    selected: tuple[str, ...], excluded: tuple[str, ...], compiled: Iterable[CompiledArtifact]
) -> DiagnosticBag:
    """Report each `--select` selector that matches no compiled artifact, then what `--exclude` removed.

    Raises:
        SstUsageError: a selector cannot be parsed, as `selection` says.

    A `state:` selector names no artifact of this compile, only a change since another, so it
    is not reported.

    Diagnostics:
        SST-DIS010: a selector matched no artifact, one per selector in the order given.
        SST-DIS201: `--exclude` left artifacts out, counting their distinct source files.
    """
    artifacts = tuple(compiled)
    selected = tuple(value for value in selected if not value.casefold().startswith("state:"))
    excluded = tuple(value for value in excluded if not value.casefold().startswith("state:"))
    found = [D("SST-DIS010", selector=value) for value in selected if not _matching(artifacts, (value,))]
    if excluded:
        left_out = _matching(artifacts, excluded)
        if left_out:
            files = {path for item in left_out for path in item.source_files}
            found.append(D("SST-DIS201", count=len(files)))
    return DiagnosticBag(found)


def _matching(artifacts: tuple[CompiledArtifact, ...], selectors: tuple[str, ...]) -> tuple[CompiledArtifact, ...]:
    universe = tuple(
        Selectable(
            item.artifact_key,
            item.artifact_type,
            item.name.casefold(),
            item.rendered_artifact.fingerprint,
            item.source_files,
        )
        for item in artifacts
    )
    types, keys = selection(selectors, universe)
    return tuple(
        item
        for item in artifacts
        if (types is not None and item.artifact_type in types) or (keys is not None and item.artifact_key in keys)
    )
