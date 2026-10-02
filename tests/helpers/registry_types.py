"""Small artifact registries for the registry's integrity tests.

`artifact` and `member` build a type that passes every check alone, with `**changes` applied,
so a test states only the field that breaks a rule. `freeze` builds a registry from them whose
resolver set is exactly the reference functions they declare, so a test registry is not refused
for the template functions the real package resolves.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

import pytest

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, D, RegistryIntegrityError
from snowflake_semantic_tools.domain.model.registry import (
    NON_ARTIFACT_FUNCTIONS,
    ArtifactType,
    AttachRule,
    MemberSource,
    MemberType,
    Registry,
    build_registry,
)


def artifact(name: str, position: int = 100, **changes: Any) -> ArtifactType:
    """Return an object artifact type named `name` at DDL position `position`."""
    value = ArtifactType(
        name=name,
        root_key=f"{name}s",
        ddl_position=position,
        ref_function=name,
        member_types=(),
        object_type=name.upper(),
        summary=f"A {name}.",
        authored_in=f"`project.{name}s_dir`",
        publishes=name.upper(),
        validation_rules=("shared",),
        dir_key=f"{name}s_dir",
    )
    return replace(value, **changes)


def member(name: str, owner: str = "semantic_view", position: int = 10, **changes: Any) -> MemberType:
    """Return a file-sourced member type named `name`, owned by `owner` at clause position `position`."""
    value = MemberType(
        name, f"{name}s", MemberSource.FILES, position, AttachRule.TABLE_MEMBERSHIP, owner, None, ("shared",)
    )
    return replace(value, **changes)


def freeze(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...] = ()) -> Registry:
    """Build the registry of `artifacts` and `members`, resolving exactly the functions they name."""
    declared = {entry.ref_function for entry in (*artifacts, *members) if entry.ref_function is not None}
    return build_registry(artifacts, members, functions=frozenset(declared) | NON_ARTIFACT_FUNCTIONS)


def refused(build: Callable[[], object], code: str, message: str) -> Mapping[str, object]:
    """Assert `build` raises `code`, a non-demotable error, with `message`; return its context.

    The message is checked twice: as the error states it, and as `D` formats the code's
    template from the error's context, so the context is exactly what the template needs.
    """
    with pytest.raises(RegistryIntegrityError) as caught:
        build()
    error = caught.value
    assert (error.code, str(error)) == (code, f"{code}: {message}")
    assert ERROR_REGISTRY[code].always_error
    assert D(code, **error.context).message == message
    return error.context
