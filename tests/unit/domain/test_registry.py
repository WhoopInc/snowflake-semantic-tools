"""The artifact registry is closed, total, and deterministic."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.registry import (
    SEMANTIC_REGISTRY,
    ArtifactLifecycle,
    ArtifactType,
    AttachRule,
    GrantPreservation,
    MemberSource,
    MemberType,
    RegistryIntegrityError,
    build_registry,
)


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
        value.ref_function
        for value in (*SEMANTIC_REGISTRY.artifacts.values(), *SEMANTIC_REGISTRY.members.values())
        if value.ref_function is not None
    ]
    assert len(functions) == len(set(functions))


def test_registry_refuses_an_unowned_member() -> None:
    artifact = ArtifactType("semantic_view", "semantic_views", 100, "semantic_view", ("metric",), "SEMANTIC VIEW")
    member = MemberType(
        "metric", "snowflake_metrics", MemberSource.FILES, 50, AttachRule.TABLE_MEMBERSHIP, "missing", "metric"
    )
    with pytest.raises(RegistryIntegrityError, match="unknown owner"):
        build_registry((artifact,), (member,))


def test_registry_refuses_duplicate_ref_functions() -> None:
    artifact = ArtifactType("semantic_view", "semantic_views", 100, "metric", ("metric",), "SEMANTIC VIEW")
    member = MemberType(
        "metric", "snowflake_metrics", MemberSource.FILES, 50, AttachRule.TABLE_MEMBERSHIP, "semantic_view", "metric"
    )
    with pytest.raises(RegistryIntegrityError, match="duplicate ref function"):
        build_registry((artifact,), (member,))


def test_registry_refuses_duplicate_artifacts_members_roots_and_positions() -> None:
    semantic_view = ArtifactType("semantic_view", "semantic_views", 100, "semantic_view", ("metric",), "SEMANTIC VIEW")
    metric = MemberType(
        "metric", "snowflake_metrics", MemberSource.FILES, 50, AttachRule.TABLE_MEMBERSHIP, "semantic_view", "metric"
    )
    with pytest.raises(RegistryIntegrityError, match="duplicate artifact type"):
        build_registry((semantic_view, semantic_view), (metric,))
    with pytest.raises(RegistryIntegrityError, match="duplicate member type"):
        build_registry((semantic_view,), (metric, metric))

    duplicate_root = MemberType(
        "metric", "semantic_views", MemberSource.FILES, 50, AttachRule.TABLE_MEMBERSHIP, "semantic_view", "metric"
    )
    with pytest.raises(RegistryIntegrityError, match="duplicate root key"):
        build_registry((semantic_view,), (duplicate_root,))

    other = MemberType("other", "other", MemberSource.FILES, 50, AttachRule.TABLE_MEMBERSHIP, "semantic_view", "other")
    with pytest.raises(RegistryIntegrityError, match="duplicate member clause position"):
        build_registry(
            (
                ArtifactType(
                    "semantic_view", "semantic_views", 100, "semantic_view", ("metric", "other"), "SEMANTIC VIEW"
                ),
            ),
            (metric, other),
        )

    with pytest.raises(RegistryIntegrityError, match="duplicate artifact DDL position"):
        build_registry(
            (
                semantic_view,
                ArtifactType("other", "others", 100, "other", (), "OTHER"),
            ),
            (metric,),
        )


def test_registry_refuses_incomplete_member_declaration() -> None:
    artifact = ArtifactType("semantic_view", "semantic_views", 100, "semantic_view", (), "SEMANTIC VIEW")
    member = MemberType(
        "metric", "snowflake_metrics", MemberSource.FILES, 50, AttachRule.TABLE_MEMBERSHIP, "semantic_view", "metric"
    )
    with pytest.raises(RegistryIntegrityError, match="member types are not total"):
        build_registry((artifact,), (member,))


def test_registry_allows_reused_clause_positions_across_owners_and_singleton_roots() -> None:
    first = ArtifactType("first", "firsts", 100, "first", ("one",), "FIRST")
    second = ArtifactType("second", None, 200, "second", ("two",), "SECOND", dependency_types=("first",))
    one = MemberType("one", "ones", MemberSource.FILES, 10, AttachRule.TABLE_MEMBERSHIP, "first", None)
    two = MemberType("two", "twos", MemberSource.FILES, 10, AttachRule.TABLE_MEMBERSHIP, "second", None)
    registry = build_registry((first, second), (one, two))
    assert registry.artifacts["first"] is first
    assert registry.members["two"] is two


def test_registry_refuses_dependency_order_inversion() -> None:
    first = ArtifactType("first", "firsts", 200, "first", (), "FIRST")
    second = ArtifactType("second", "seconds", 100, "second", (), "SECOND", dependency_types=("first",))
    with pytest.raises(RegistryIntegrityError, match="must follow dependency"):
        build_registry((first, second), ())


def test_registry_refuses_version_pins_on_types_it_does_not_depend_on() -> None:
    first = ArtifactType("first", "firsts", 100, "first", (), "FIRST")
    second = ArtifactType("second", "seconds", 200, "second", (), "SECOND", pins_versions_of=("first",))
    with pytest.raises(RegistryIntegrityError, match="pins versions of types it does not depend on"):
        build_registry((first, second), ())


def test_registry_refuses_object_lifecycle_without_observable_type() -> None:
    with pytest.raises(RegistryIntegrityError, match="requires an observable object type"):
        build_registry(
            (
                ArtifactType(
                    "virtual",
                    None,
                    100,
                    None,
                    (),
                    "",
                    prunable=False,
                    replaces_on_update=False,
                    grant_preservation=GrantPreservation.NONE,
                ),
            ),
            (),
        )


def test_registry_refuses_invalid_lifecycle_metadata() -> None:
    metric = MemberType(
        "metric", "metrics", MemberSource.FILES, 10, AttachRule.TABLE_MEMBERSHIP, "semantic_view", "metric"
    )
    with pytest.raises(RegistryIntegrityError, match="unknown dependencies"):
        build_registry(
            (
                ArtifactType(
                    "semantic_view",
                    "semantic_views",
                    100,
                    "semantic_view",
                    ("metric",),
                    "SEMANTIC VIEW",
                    dependency_types=("missing",),
                ),
            ),
            (metric,),
        )
    with pytest.raises(RegistryIntegrityError, match="replaces without grant preservation"):
        build_registry(
            (
                ArtifactType(
                    "semantic_view",
                    "semantic_views",
                    100,
                    "semantic_view",
                    ("metric",),
                    "SEMANTIC VIEW",
                    grant_preservation=GrantPreservation.NONE,
                ),
            ),
            (metric,),
        )
    with pytest.raises(RegistryIntegrityError, match="preserves grants without replacing"):
        build_registry(
            (
                ArtifactType(
                    "semantic_view",
                    "semantic_views",
                    100,
                    "semantic_view",
                    ("metric",),
                    "SEMANTIC VIEW",
                    replaces_on_update=False,
                ),
            ),
            (metric,),
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"object_type": "TABLE"}, "observable object type"),
        ({"prunable": True}, "generic prune or replace lifecycle"),
        (
            {"replaces_on_update": True, "grant_preservation": GrantPreservation.CLAUSE},
            "generic prune or replace lifecycle",
        ),
    ),
)
def test_registry_refuses_object_lifecycle_metadata_on_composite_artifacts(
    changes: dict[str, object], message: str
) -> None:
    values: dict[str, object] = {
        "name": "composite",
        "root_key": None,
        "ddl_position": 100,
        "ref_function": None,
        "member_types": (),
        "object_type": "",
        "prunable": False,
        "replaces_on_update": False,
        "grant_preservation": GrantPreservation.NONE,
        "lifecycle": ArtifactLifecycle.COMPOSITE,
    }
    values.update(changes)
    with pytest.raises(RegistryIntegrityError, match=message):
        build_registry((ArtifactType(**values),), ())  # type: ignore[arg-type]


def test_registry_refuses_generic_grant_preservation_on_composite_after_earlier_checks() -> None:
    artifact = ArtifactType(
        "composite",
        None,
        100,
        None,
        (),
        "",
        prunable=False,
        replaces_on_update=False,
        grant_preservation=GrantPreservation.CLAUSE,
        lifecycle=ArtifactLifecycle.COMPOSITE,
    )
    with pytest.raises(RegistryIntegrityError, match="preserves grants without replacing"):
        build_registry((artifact,), ())

    class LateCompositeGrant:
        name = "late"
        root_key = None
        ddl_position = 100
        ref_function = None
        member_types: tuple[str, ...] = ()
        object_type = ""
        prunable = False
        dependency_types: tuple[str, ...] = ()
        pins_versions_of: tuple[str, ...] = ()
        object_types: tuple[str, ...] = ()
        lifecycle = ArtifactLifecycle.COMPOSITE
        _replace_reads = 0
        _grant_reads = 0

        @property
        def replaces_on_update(self) -> bool:
            self._replace_reads += 1
            return self._replace_reads < 3

        @property
        def grant_preservation(self) -> GrantPreservation:
            self._grant_reads += 1
            if self._grant_reads < 3:
                return GrantPreservation.CLAUSE
            if self._grant_reads == 3:
                return GrantPreservation.NONE
            return GrantPreservation.CLAUSE

    with pytest.raises(RegistryIntegrityError, match="generic grant preservation"):
        build_registry((LateCompositeGrant(),), ())  # type: ignore[arg-type]
