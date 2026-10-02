"""What `--select` and `--exclude` did to a compiled project.

A selector that matched nothing is reported, and so is how many files `--exclude` left out.
"""

from __future__ import annotations

from collections.abc import Iterable

from snowflake_semantic_tools.app.compile import CompiledArtifact
from snowflake_semantic_tools.cli.wiring.compile import selection
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag


def selector_report(
    selected: tuple[str, ...], excluded: tuple[str, ...], compiled: Iterable[CompiledArtifact]
) -> DiagnosticBag:
    """Report each `--select` selector that matches no compiled artifact, then what `--exclude` removed.

    Raises:
        SstUsageError: a selector cannot be parsed, as `selection` says.

    Diagnostics:
        SST-DIS010: a selector matched no artifact, one per selector in the order given.
        SST-DIS201: `--exclude` left artifacts out, counting their distinct source files.
    """
    artifacts = tuple(compiled)
    found = [D("SST-DIS010", selector=value) for value in selected if not _matching(artifacts, (value,))]
    if excluded:
        left_out = _matching(artifacts, excluded)
        if left_out:
            files = {path for item in left_out for path in item.source_files}
            found.append(D("SST-DIS201", count=len(files)))
    return DiagnosticBag(found)


def _matching(artifacts: tuple[CompiledArtifact, ...], selectors: tuple[str, ...]) -> tuple[CompiledArtifact, ...]:
    types, keys = selection(selectors)
    return tuple(
        item
        for item in artifacts
        if (types is not None and item.artifact_type in types) or (keys is not None and item.artifact_key in keys)
    )
