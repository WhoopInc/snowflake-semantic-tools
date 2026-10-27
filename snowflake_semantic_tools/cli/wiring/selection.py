"""Narrow a compile to what `--select` and `--exclude` choose, as `sst test` and `sst eval` run on it."""

from __future__ import annotations

import dataclasses
from pathlib import Path

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.cli.runner import CommandResult
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.manifest import lineage_of
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag


def narrowed(
    paths: ProjectPaths,
    compiled: CompileResult,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    manifest_path: Path | None,
    target_name: str | None,
) -> tuple[CompileResult, DiagnosticBag]:
    """Return the artifacts of `compiled` the selectors choose, with what the selectors' report says.

    With no selector, `compiled` itself and an empty report.

    Raises:
        SstUsageError: a selector is refused.
        ProjectError: the selectors matched no artifact (SST-DIS010).
    """
    if not selected and not excluded:
        return compiled, DiagnosticBag()
    graph = lineage_of(paths, compiled, manifest_path, target_name)
    chosen = compiling.selected_within(paths.project_dir, compiled, selected, excluded, graph=graph)
    return chosen.result, chosen.report


def noted(result: CommandResult, notes: DiagnosticBag) -> CommandResult:
    """Report what the selectors' report says ahead of the run's own diagnostics."""
    if not notes:
        return result
    return dataclasses.replace(result, diagnostics=DiagnosticBag((*notes, *result.diagnostics)))
