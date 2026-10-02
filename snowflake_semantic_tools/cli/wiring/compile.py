"""Compile the project from its files, and narrow a compiled result to what a selector names.

Commands call `compile_result` through this module, as `compiling.compile_result(...)`, so
replacing it here changes what every command compiles.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.manifest import dbt_manifest_moved
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY


def compile_result(project_dir: Path, target_name: str | None, manifest_path: Path | None) -> CompileResult:
    """Compile every artifact the project declares, from its files.

    With a dbt manifest given, its models are digested before and after the compile, and a
    manifest rewritten in between, by a concurrent `dbt compile`, fails the compile. Without
    one, SST runs `dbt parse` itself and so rewrites the manifest on purpose; nothing is checked.

    Diagnostics:
        SST-MAN031: the given dbt manifest's models changed while the project compiled.
    """
    inputs = project_inputs(project_dir, target_name, manifest_path)
    if manifest_path is None:
        return CompileProject(inputs).run()
    before = inputs.manifest_sources()
    result = CompileProject(inputs).run()
    moved = dbt_manifest_moved(before, inputs.manifest_sources())
    if moved is None:
        return result
    return dataclasses.replace(result, diagnostics=DiagnosticBag((*result.diagnostics, moved)))


def selected_result(project_dir: Path, result: CompileResult, selected: str) -> CompileResult:
    """Return `result` with only the artifacts the one selector `selected` names.

    Raises:
        SstUsageError: the selector cannot be parsed, as `selection` says.
        ProjectError: the selector matched no artifact.
    """
    selected_types, selected_keys = selection((selected,))
    compiled = tuple(
        item
        for item in result.compiled
        if (selected_types is None or item.artifact_type in selected_types)
        and (selected_keys is None or item.artifact_key in selected_keys)
    )
    if not compiled:
        raise ProjectError(f"selector {selected!r} matched no artifact in {project_dir}")
    return dataclasses.replace(result, compiled=compiled)


def selection(values: tuple[str, ...]) -> tuple[frozenset[str] | None, frozenset[str] | None]:
    """Parse selectors into the artifact types and the artifact keys they name; None for neither.

    A bare name is a semantic view, `type:<type>` every artifact of a type, and
    `<type>:<name>` one artifact; names are compared case-insensitively.

    Raises:
        SstUsageError: a selector holds a comma, or names an unknown type or no name.
    """
    if not values:
        return None, None
    keys: set[str] = set()
    types: set[str] = set()
    for value in values:
        if "," in value:
            raise SstUsageError("commas are not accepted in selectors; pass space-separated selectors")
        if ":" in value:
            prefix, name = split_artifact_key(value)
            if prefix == "type":
                if name not in SEMANTIC_REGISTRY.artifacts:
                    raise SstUsageError(f"unknown artifact type {name!r}")
                types.add(name)
                continue
            if prefix not in SEMANTIC_REGISTRY.artifacts or not name:
                raise SstUsageError(f"unsupported selector {value!r}")
            keys.add(artifact_key(prefix, name.casefold()))
            continue
        keys.add(artifact_key("semantic_view", value.casefold()))
    return (frozenset(types) if types else None), (frozenset(keys) if keys else None)
