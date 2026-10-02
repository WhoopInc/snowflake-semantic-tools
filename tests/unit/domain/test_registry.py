"""The artifact registry is closed, total, and deterministic."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import RULE_SETS
from snowflake_semantic_tools.domain.model.registry import (
    SEMANTIC_REGISTRY,
    ArtifactLifecycle,
    ArtifactType,
    MemberType,
    RegistryBuilder,
)
from tests.helpers.registry_types import artifact, freeze


def test_registry_has_m4_artifacts_and_all_seven_semantic_member_types() -> None:
    assert tuple(SEMANTIC_REGISTRY.artifacts) == (
        "semantic_view",
        "tool",
        "skill",
        "plugin",
        "profile",
        "agent",
        "eval",
    )
    assert [value.ddl_position for value in SEMANTIC_REGISTRY.artifacts.values()] == [100, 200, 250, 260, 270, 300, 400]
    assert SEMANTIC_REGISTRY.artifacts["profile"].ref_function is None
    assert SEMANTIC_REGISTRY.artifacts["tool"].dependency_types == ("semantic_view",)
    assert SEMANTIC_REGISTRY.artifacts["agent"].dependency_types == ("semantic_view", "tool", "skill", "plugin")
    assert SEMANTIC_REGISTRY.artifacts["agent"].pins_versions_of == ("skill", "plugin")
    assert all(not value.pins_versions_of for name, value in SEMANTIC_REGISTRY.artifacts.items() if name != "agent")
    for name in ("skill", "plugin"):
        assert SEMANTIC_REGISTRY.artifacts[name].lifecycle is ArtifactLifecycle.COMPOSITE
        assert SEMANTIC_REGISTRY.artifacts[name].ref_function == name
    assert SEMANTIC_REGISTRY.artifacts["eval"].dependency_types == ("agent",)
    assert SEMANTIC_REGISTRY.artifacts["eval"].lifecycle is ArtifactLifecycle.COMPOSITE
    assert tuple(SEMANTIC_REGISTRY.members) == (
        "relationship",
        "fact",
        "dimension",
        "metric",
        "filter",
        "verified_query",
        "custom_instruction",
    )


def test_registry_ref_functions_are_unique_when_present() -> None:
    functions = [
        *(value.ref_function for value in SEMANTIC_REGISTRY.artifacts.values() if value.ref_function is not None),
        *(value.ref_function for value in SEMANTIC_REGISTRY.members.values() if value.ref_function is not None),
    ]
    assert len(functions) == len(set(functions))


def test_every_type_names_registered_rule_sets_and_a_discovery_directory() -> None:
    types: tuple[ArtifactType | MemberType, ...] = (
        *SEMANTIC_REGISTRY.artifacts.values(),
        *SEMANTIC_REGISTRY.members.values(),
    )
    for value in types:
        assert value.validation_rules
        assert set(value.validation_rules) <= set(RULE_SETS)
    for value in SEMANTIC_REGISTRY.artifacts.values():
        assert value.dir_key.endswith("_dir")
    assert SEMANTIC_REGISTRY.artifacts["eval"].nested_under == "agent"


def test_registry_allows_types_without_root_keys_or_reference_functions() -> None:
    registry = freeze((artifact("semantic_view"), artifact("profile", 200, root_key=None, ref_function=None)))
    assert registry.artifacts["profile"].root_key is None


def test_a_builder_freezes_once_and_its_registry_is_read_only() -> None:
    builder = RegistryBuilder()
    builder.artifact(artifact("semantic_view"))
    registry = builder.freeze(functions=frozenset({"semantic_view"}))
    assert tuple(registry.artifacts) == ("semantic_view",)
    with pytest.raises(TypeError):
        registry.artifacts["x"] = artifact("x")  # type: ignore[index]
