"""Every integrity check the artifact registry and the error registry run, on each branch it has."""

from __future__ import annotations

from typing import Any

import pytest

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, RegistryIntegrityError, Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from snowflake_semantic_tools.domain.diagnostics.integrity import severity_comparisons
from snowflake_semantic_tools.domain.model.registry import (
    NON_ARTIFACT_FUNCTIONS,
    ArtifactLifecycle,
    ArtifactType,
    GrantPreservation,
    MemberType,
    RegistryBuilder,
    check_member_index,
)
from snowflake_semantic_tools.domain.model.registry import build_registry as build_types
from tests.helpers.registry_types import artifact, freeze, member

VIEW = artifact("semantic_view")
COMPOSITE: dict[str, Any] = {
    "lifecycle": ArtifactLifecycle.COMPOSITE,
    "object_type": "",
    "prunable": False,
    "replaces_on_update": False,
    "grant_preservation": GrantPreservation.NONE,
}


def owning(*members: str) -> ArtifactType:
    return artifact("semantic_view", member_types=members)


FAULTS: list[tuple[str, tuple[ArtifactType, ...], tuple[MemberType, ...], str]] = [
    ("unnamed", (artifact(""),), (), "SST-REG001"),
    ("member field", (owning("metric"),), (member("metric", validation_rules=()),), "SST-REG001"),
    ("root key", (owning("metric"),), (member("metric", root_key="semantic_views"),), "SST-REG003"),
    ("ddl position", (VIEW, artifact("tool")), (), "SST-REG004"),
    ("clause position", (owning("a", "b"),), (member("a"), member("b")), "SST-REG004"),
    ("unknown owner", (owning("metric"),), (member("metric", "nowhere"),), "SST-REG007"),
    ("unknown member", (owning("metric"),), (), "SST-REG007"),
    ("unknown dependency", (artifact("semantic_view", dependency_types=("x",)),), (), "SST-REG007"),
    ("pin without dependency", (VIEW, artifact("agent", 300, pins_versions_of=("semantic_view",))), (), "SST-REG001"),
    ("second owner", (VIEW, artifact("agent", 300, member_types=("m",))), (member("m", "agent"),), "SST-REG022"),
    ("undeclared member", (VIEW,), (member("metric"),), "SST-REG001"),
    ("member owned elsewhere", (owning("metric"), artifact("tool", 200)), (member("metric", "tool"),), "SST-REG007"),
    ("unknown rule", (artifact("semantic_view", validation_rules=("nope",)),), (), "SST-REG006"),
    (
        "cycle",
        (
            artifact("semantic_view", dependency_types=("tool",)),
            artifact("tool", 200, dependency_types=("semantic_view",)),
        ),
        (),
        "SST-REG011",
    ),
    ("order", (VIEW, artifact("tool", 50, dependency_types=("semantic_view",))), (), "SST-REG010"),
    (
        "replaces without grants",
        (artifact("semantic_view", grant_preservation=GrantPreservation.NONE),),
        (),
        "SST-REG021",
    ),
    ("grants without replacing", (artifact("semantic_view", replaces_on_update=False),), (), "SST-REG021"),
    ("composite object type", (artifact("skill", **{**COMPOSITE, "object_type": "X"}),), (), "SST-REG021"),
    ("composite prunable", (artifact("skill", **{**COMPOSITE, "prunable": True}),), (), "SST-REG021"),
    ("unobservable object", (artifact("semantic_view", object_type=""),), (), "SST-REG001"),
    ("ref function twice", (artifact("semantic_view", ref_function="tool"), artifact("tool", 200)), (), "SST-REG005"),
]


@pytest.mark.parametrize(
    ("artifacts", "members", "code"), [case[1:] for case in FAULTS], ids=[case[0] for case in FAULTS]
)
def test_each_registry_fault_raises_its_code(
    artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...], code: str
) -> None:
    with pytest.raises(RegistryIntegrityError) as caught:
        freeze(artifacts, members)
    assert caught.value.code == code


def test_a_registry_without_faults_freezes_through_every_check() -> None:
    diamond = (
        VIEW,
        artifact("tool", 200, dependency_types=("semantic_view",)),
        artifact("skill", 250, dependency_types=("semantic_view",), **COMPOSITE),
        artifact("agent", 300, dependency_types=("tool", "skill"), pins_versions_of=("skill",)),
        artifact("profile", 400, root_key=None, ref_function=None, object_types=("STAGE",), object_type=""),
    )
    registry = freeze(diamond)
    assert list(registry.artifacts) == ["semantic_view", "tool", "skill", "agent", "profile"]


def test_the_builder_refuses_duplicates_and_every_registration_after_freeze() -> None:
    builder = RegistryBuilder()
    builder.artifact(VIEW)
    builder.member(member("metric"))
    with pytest.raises(RegistryIntegrityError, match="SST-REG002"):
        builder.artifact(VIEW)
    with pytest.raises(RegistryIntegrityError, match="SST-REG002"):
        builder.member(member("metric"))
    with pytest.raises(RegistryIntegrityError, match="SST-REG001"):
        builder.freeze(functions=frozenset({"semantic_view"}))
    frozen = RegistryBuilder()
    frozen.freeze(functions=frozenset())
    for attempt in (lambda: frozen.member(member("x")), lambda: frozen.freeze()):
        with pytest.raises(RegistryIntegrityError, match="SST-REG900"):
            attempt()


@pytest.mark.parametrize(
    ("functions", "direction"),
    [
        (frozenset({"semantic_view", "dashboard"}), "a resolver addresses no registered type"),
        (frozenset(), "no resolver handles the type's function"),
    ],
)
def test_the_resolver_set_and_the_reference_functions_must_agree(functions: frozenset[str], direction: str) -> None:
    with pytest.raises(RegistryIntegrityError) as caught:
        build_types((VIEW,), (), functions=functions | NON_ARTIFACT_FUNCTIONS)
    assert caught.value.context["direction"] == direction


def test_a_member_index_must_cover_exactly_the_declared_member_types() -> None:
    registry = freeze((owning("metric"),), (member("metric"),))
    check_member_index(("metric",), registry)
    with pytest.raises(RegistryIntegrityError, match="SST-REG023"):
        check_member_index(("metric", "filter"), registry)


ERROR_FAULTS = [
    ("malformed", spec("SST-AB001", Severity.ERROR, "T", "{value}", None), "SST-REG012"),
    ("no template", spec("SST-ABC001", Severity.ERROR, "T", "", None), "SST-REG001"),
    ("retired", spec("SST-VAL620", Severity.ERROR, "T", "{value}", None), "SST-REG017"),
    ("internal warning", spec("SST-ABC900", Severity.WARNING, "T", "{value}", None), "SST-REG014"),
    ("demotable internal", spec("SST-INT999", Severity.ERROR, "T", "{value}", None), "SST-REG014"),
    ("placeholder", spec("SST-ABC001", Severity.ERROR, "T", "{nope}", None), "SST-REG016"),
    ("deprecated", spec("SST-ABC001", Severity.ERROR, "T", "{value}", None, deprecated_in="1.1.0"), "SST-REG018"),
    ("internal detail", spec("SST-ABC001", Severity.ERROR, "T", "{value}", None, internal_detail=True), "SST-REG019"),
]


@pytest.mark.parametrize(("entry", "code"), [case[1:] for case in ERROR_FAULTS], ids=[case[0] for case in ERROR_FAULTS])
def test_each_error_registry_fault_raises_its_code(entry: object, code: str) -> None:
    with pytest.raises(RegistryIntegrityError) as caught:
        build_registry((entry,))  # type: ignore[arg-type]
    assert caught.value.code == code


def test_the_error_registry_accepts_every_registered_code_and_a_documented_successor() -> None:
    successor = spec(
        "SST-ABC001", Severity.INFO, "T", "{value}", None, deprecated_in="1.1.0", superseded_by="SST-ABC002"
    )
    assert len(build_registry((*ERROR_REGISTRY.values(), successor))) == len(ERROR_REGISTRY) + 1


def test_severity_comparisons_flag_a_severity_operand_and_nothing_else() -> None:
    source = (
        "a = item.severity is Severity.ERROR\n"
        "b = Severity.WARNING == level\n"
        "c = item.code == other.code\n"
        "d = level.name == 'x'\n"
        "e = count > 1\n"
    )
    assert [error.context["module"] for error in severity_comparisons("m", source)] == ["m:1", "m:2"]


def test_a_subsystem_other_than_the_area_and_an_unreferenced_type_are_refused() -> None:
    from dataclasses import replace

    with pytest.raises(RegistryIntegrityError, match="SST-REG013"):
        build_registry((replace(spec("SST-ABC001", Severity.ERROR, "T", "{value}", None), subsystem="XYZ"),))
    with pytest.raises(RegistryIntegrityError, match="the type has no ref_function"):
        freeze((VIEW, artifact("tool", 200, ref_function=None)))
