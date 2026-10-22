"""The artifact registry is closed, total, and deterministic."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import snowflake_semantic_tools
from snowflake_semantic_tools.domain.diagnostics import RULE_SETS
from snowflake_semantic_tools.domain.model.registry import (
    RESOLVED_FUNCTIONS,
    SEMANTIC_REGISTRY,
    ArtifactLifecycle,
    ArtifactType,
    MemberType,
    RegistryBuilder,
)
from snowflake_semantic_tools.domain.parse.template import scan_template_calls
from snowflake_semantic_tools.domain.resolve import template
from snowflake_semantic_tools.domain.resolve.calls import KNOWN_FUNCTIONS, call_problem
from snowflake_semantic_tools.domain.resolve.ref_fields import REFERENCE_FIELDS
from tests.helpers.registry_types import artifact, freeze


def test_registry_has_m4_artifacts_and_all_seven_semantic_member_types() -> None:
    assert tuple(SEMANTIC_REGISTRY.artifacts) == (
        "semantic_view",
        "tool",
        "skill",
        "plugin",
        "coco_desktop_profile",
        "agent",
        "cowork_profile",
        "eval",
    )
    assert [value.ddl_position for value in SEMANTIC_REGISTRY.artifacts.values()] == [
        100,
        200,
        250,
        260,
        270,
        300,
        350,
        400,
    ]
    assert SEMANTIC_REGISTRY.artifacts["coco_desktop_profile"].ref_function is None
    assert SEMANTIC_REGISTRY.artifacts["tool"].dependency_types == ("semantic_view",)
    assert SEMANTIC_REGISTRY.artifacts["agent"].dependency_types == ("semantic_view", "tool", "skill", "plugin")
    pinning = ("agent", "cowork_profile")
    for name in pinning:
        assert SEMANTIC_REGISTRY.artifacts[name].pins_versions_of == ("skill", "plugin")
    assert all(not value.pins_versions_of for name, value in SEMANTIC_REGISTRY.artifacts.items() if name not in pinning)
    assert SEMANTIC_REGISTRY.artifacts["cowork_profile"].dependency_types == ("agent", "tool", "skill", "plugin")
    assert SEMANTIC_REGISTRY.artifacts["cowork_profile"].ref_function is None
    for name in ("skill", "plugin"):
        assert SEMANTIC_REGISTRY.artifacts[name].lifecycle is ArtifactLifecycle.COMPOSITE
        assert SEMANTIC_REGISTRY.artifacts[name].ref_function == name
    assert SEMANTIC_REGISTRY.artifacts["eval"].dependency_types == ("agent", "cowork_profile")
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


def _literals(node: ast.expr, bound: dict[str, frozenset[str]]) -> frozenset[str]:
    """The function names `node` spells: a string, a tuple or `frozenset` of them, or a loop variable."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return frozenset((node.value,))
    if isinstance(node, ast.Name):
        return bound.get(node.id, frozenset())
    if isinstance(node, ast.Tuple):
        return frozenset().union(*(_literals(item, bound) for item in node.elts))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "frozenset" and node.args:
        return _literals(node.args[0], bound)
    return frozenset()


# The loader entry points that take the one function a field may call as their second argument,
# and the checks that refuse a call a field does not accept, whose second argument is the set.
_ONE_FUNCTION_ENTRY_POINTS = frozenset(("single_template_call", "replace_template_calls"))
_ENTRY_POINTS = _ONE_FUNCTION_ENTRY_POINTS | {"call_problem", "required_call"}


def _functions_loaders_resolve() -> frozenset[str]:
    """Every function a module of the package hands to a template entry point by name."""
    found: set[str] = set()
    for path in Path(snowflake_semantic_tools.__file__).parent.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for scope in ast.walk(tree):
            if not isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef | ast.Module):
                continue
            bound = {
                loop.target.id: _literals(loop.iter, {})
                for loop in ast.walk(scope)
                if isinstance(loop, ast.For) and isinstance(loop.target, ast.Name)
            }
            for call in ast.walk(scope):
                if not isinstance(call, ast.Call) or len(call.args) < 2:
                    continue
                name = call.func.attr if isinstance(call.func, ast.Attribute) else getattr(call.func, "id", "")
                if name in _ENTRY_POINTS:
                    found |= _literals(call.args[1], bound)
    return frozenset(found)


def _legacy_refusals() -> frozenset[str]:
    """The functions `call_problem` refuses as legacy spellings of `ref`, whatever a field allows."""
    refused = set()
    for function, code in (("table", "SST-REF034"), ("column", "SST-REF035")):
        (call,) = scan_template_calls(f"{{{{ {function}('model', 'column') }}}}")
        problem = call_problem(call, frozenset(KNOWN_FUNCTIONS), None, field="f", artifact="a")
        if problem is not None and problem.code == code:
            refused.add(function)
    return frozenset(refused)


def test_every_dialect_function_has_a_resolver_in_the_package() -> None:
    # Built from the code, not from another list: the functions template.py's field policies let
    # it resolve, the ones a loader hands to a template entry point by name, and the legacy
    # spellings call_problem refuses. A dialect function none of them handles, or a handled one
    # the dialect or the registry's resolver set lacks, fails here.
    policies = [value for value in vars(template).values() if isinstance(value, template.RefPolicy)]
    by_policy = frozenset().union(*(policy.allowed for policy in policies))
    by_reference = frozenset().union(*REFERENCE_FIELDS.values())
    resolved = by_policy | by_reference | _functions_loaders_resolve() | _legacy_refusals()
    assert resolved == KNOWN_FUNCTIONS
    assert RESOLVED_FUNCTIONS == KNOWN_FUNCTIONS


def test_every_field_policy_calls_only_resolved_functions() -> None:
    policies = [value for value in vars(template).values() if isinstance(value, template.RefPolicy)]
    assert policies
    for policy in policies:
        assert policy.allowed <= RESOLVED_FUNCTIONS


def test_member_reference_functions_name_their_member_types() -> None:
    members = SEMANTIC_REGISTRY.members
    for name in ("relationship", "filter", "verified_query"):
        assert members[name].ref_function == name
    assert members["custom_instruction"].ref_function == "custom_instructions"


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
    registry = freeze(
        (
            artifact("semantic_view"),
            artifact("coco_desktop_profile", 200, root_key=None, singular_root=None, ref_function=None),
        )
    )
    assert registry.artifacts["coco_desktop_profile"].root_key is None


def test_a_builder_freezes_once_and_its_registry_is_read_only() -> None:
    builder = RegistryBuilder()
    builder.artifact(artifact("semantic_view"))
    registry = builder.freeze(functions=frozenset({"semantic_view"}))
    assert tuple(registry.artifacts) == ("semantic_view",)
    with pytest.raises(TypeError):
        registry.artifacts["x"] = artifact("x")  # type: ignore[index]
