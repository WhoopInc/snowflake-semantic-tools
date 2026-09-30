"""Closed artifact registry and the members each artifact type owns."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from types import MappingProxyType
from typing import Mapping


class MemberSource(Enum):
    FILES = auto()
    DBT_META = auto()


class AttachRule(Enum):
    TABLE_MEMBERSHIP = auto()
    VIEW_NAME = auto()


class GrantPreservation(Enum):
    CLAUSE = auto()
    REPLAY = auto()
    NONE = auto()


class ArtifactLifecycle(Enum):
    OBJECT = auto()
    COMPOSITE = auto()


@dataclass(frozen=True, slots=True)
class ArtifactType:
    name: str
    root_key: str | None
    ddl_position: int
    ref_function: str | None
    member_types: tuple[str, ...]
    object_type: str
    prunable: bool = True
    replaces_on_update: bool = True
    grant_preservation: GrantPreservation = GrantPreservation.CLAUSE
    dependency_types: tuple[str, ...] = ()
    # Dependencies whose published version this type's payload names. A write of
    # this type is planned only together with them, so it cannot pin a version
    # that was never created.
    pins_versions_of: tuple[str, ...] = ()
    object_types: tuple[str, ...] = ()
    lifecycle: ArtifactLifecycle = ArtifactLifecycle.OBJECT
    # Reference-page text; the generated artifact reference renders these rows.
    summary: str = ""
    authored_in: str = ""
    publishes: str = ""


@dataclass(frozen=True, slots=True)
class MemberType:
    name: str
    root_key: str
    source: MemberSource
    clause_position: int
    attaches_by: AttachRule
    owner_type: str
    ref_function: str | None


@dataclass(frozen=True, slots=True)
class Registry:
    artifacts: Mapping[str, ArtifactType]
    members: Mapping[str, MemberType]

    @property
    def semantic_root_keys(self) -> frozenset[str]:
        return frozenset(
            [artifact.root_key for artifact in self.artifacts.values() if artifact.root_key is not None]
            + [member.root_key for member in self.members.values() if member.source is MemberSource.FILES]
        )

    def owner_of(self, root_key: str) -> ArtifactType | MemberType | None:
        for artifact in self.artifacts.values():
            if artifact.root_key == root_key:
                return artifact
        for member in self.members.values():
            if member.root_key == root_key:
                return member
        return None

    @property
    def ref_functions(self) -> Mapping[str, ArtifactType | MemberType]:
        functions: dict[str, ArtifactType | MemberType] = {}
        for artifact in self.artifacts.values():
            if artifact.ref_function is not None:
                functions[artifact.ref_function] = artifact
        for member in self.members.values():
            if member.ref_function is not None:
                functions[member.ref_function] = member
        return MappingProxyType(functions)


class RegistryIntegrityError(RuntimeError):
    pass


def build_registry(artifact_types: tuple[ArtifactType, ...], member_types: tuple[MemberType, ...]) -> Registry:
    artifacts = {artifact.name: artifact for artifact in artifact_types}
    members = {member.name: member for member in member_types}
    if len(artifacts) != len(artifact_types):
        raise RegistryIntegrityError("duplicate artifact type")
    if len(members) != len(member_types):
        raise RegistryIntegrityError("duplicate member type")
    root_keys = [artifact.root_key for artifact in artifact_types if artifact.root_key is not None] + [
        member.root_key for member in member_types
    ]
    if len(root_keys) != len(set(root_keys)):
        raise RegistryIntegrityError("duplicate root key")
    positions = [(member.owner_type, member.clause_position) for member in member_types]
    if len(positions) != len(set(positions)):
        raise RegistryIntegrityError("duplicate member clause position for one owner")
    artifact_positions = [artifact.ddl_position for artifact in artifact_types]
    if len(artifact_positions) != len(set(artifact_positions)):
        raise RegistryIntegrityError("duplicate artifact DDL position")
    for member in member_types:
        if member.owner_type not in artifacts:
            raise RegistryIntegrityError(f"member {member.name} has unknown owner {member.owner_type}")
    for artifact in artifact_types:
        unknown_dependencies = set(artifact.dependency_types) - set(artifacts)
        if unknown_dependencies:
            raise RegistryIntegrityError(
                f"artifact {artifact.name} has unknown dependencies {sorted(unknown_dependencies)}"
            )
        if not set(artifact.pins_versions_of) <= set(artifact.dependency_types):
            raise RegistryIntegrityError(f"artifact {artifact.name} pins versions of types it does not depend on")
        if artifact.replaces_on_update and artifact.grant_preservation is GrantPreservation.NONE:
            raise RegistryIntegrityError(f"artifact {artifact.name} replaces without grant preservation")
        if not artifact.replaces_on_update and artifact.grant_preservation is not GrantPreservation.NONE:
            raise RegistryIntegrityError(f"artifact {artifact.name} preserves grants without replacing")
        if artifact.lifecycle is ArtifactLifecycle.COMPOSITE:
            if artifact.object_type or artifact.object_types:
                raise RegistryIntegrityError(
                    f"composite artifact {artifact.name} cannot declare an observable object type"
                )
            if artifact.prunable or artifact.replaces_on_update:
                raise RegistryIntegrityError(
                    f"composite artifact {artifact.name} cannot use generic prune or replace lifecycle"
                )
            if artifact.grant_preservation is not GrantPreservation.NONE:
                raise RegistryIntegrityError(
                    f"composite artifact {artifact.name} cannot declare generic grant preservation"
                )
        elif not artifact.object_type and not artifact.object_types:
            raise RegistryIntegrityError(f"object artifact {artifact.name} requires an observable object type")
        declared = set(artifact.member_types)
        owned = {member.name for member in member_types if member.owner_type == artifact.name}
        if declared != owned:
            raise RegistryIntegrityError(f"artifact {artifact.name} member types are not total")
        for dependency in artifact.dependency_types:
            if artifacts[dependency].ddl_position >= artifact.ddl_position:
                raise RegistryIntegrityError(
                    f"artifact {artifact.name} must follow dependency {dependency} in DDL order"
                )
    functions = [artifact.ref_function for artifact in artifact_types if artifact.ref_function is not None]
    functions.extend(member.ref_function for member in member_types if member.ref_function is not None)
    if len(functions) != len(set(functions)):
        raise RegistryIntegrityError("duplicate ref function")
    return Registry(MappingProxyType(artifacts), MappingProxyType(members))


ARTIFACT_REGISTRY = build_registry(
    (
        ArtifactType(
            name="semantic_view",
            root_key="semantic_views",
            ddl_position=100,
            ref_function="semantic_view",
            member_types=(
                "relationship",
                "fact",
                "dimension",
                "metric",
                "filter",
                "verified_query",
                "custom_instruction",
            ),
            object_type="SEMANTIC VIEW",
            summary="A Snowflake semantic view built from dbt models and the semantic members attached to them.",
            authored_in="`semantic_views:` in `project.semantic_models_dir`",
            publishes="SEMANTIC VIEW",
        ),
        ArtifactType(
            name="tool",
            root_key="tools",
            ddl_position=200,
            ref_function="tool",
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=True,
            grant_preservation=GrantPreservation.REPLAY,
            dependency_types=("semantic_view",),
            object_types=("CORTEX SEARCH SERVICE", "PROCEDURE", "FUNCTION", "STAGE"),
            summary="A Snowflake object an agent tool calls, published before any agent that uses it.",
            authored_in="`project.tools_dir`",
            publishes="CORTEX SEARCH SERVICE, PROCEDURE, FUNCTION, or STAGE",
        ),
        ArtifactType(
            name="skill",
            root_key=None,
            ddl_position=250,
            ref_function="skill",
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary=(
                "One `SKILL.md` folder, flattened and published as a Cortex Extension version "
                "whose alias is a hash of its content."
            ),
            authored_in="a `SKILL.md` folder in `project.skills_dir`",
            publishes="CORTEX EXTENSION (TYPE = 'SKILL') and its bundle stage",
        ),
        ArtifactType(
            name="plugin",
            root_key=None,
            ddl_position=260,
            ref_function="plugin",
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary="A named set of project skills, published together as one plugin-type Cortex Extension.",
            authored_in="`plugin.yml` in `project.plugins_dir`",
            publishes="CORTEX EXTENSION (TYPE = 'PLUGIN') and its bundle stage",
        ),
        ArtifactType(
            name="profile",
            root_key=None,
            ddl_position=270,
            ref_function=None,
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary=(
                "A CoCo Desktop profile: content-addressed skill, prompt, MCP, and hook trees "
                "behind one profile registry row."
            ),
            authored_in="`profile.yml` in `project.profiles_dir`",
            publishes="profile stage trees and one profile registry row",
        ),
        ArtifactType(
            name="agent",
            root_key=None,
            ddl_position=300,
            ref_function="agent",
            member_types=(),
            object_type="AGENT",
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            dependency_types=("semantic_view", "tool", "skill", "plugin"),
            pins_versions_of=("skill", "plugin"),
            summary="A Cortex Agent whose specification SST renders and pins to exactly what it uses.",
            authored_in="`agent.yml` in `project.agents_dir`",
            publishes="AGENT",
        ),
        ArtifactType(
            name="eval",
            root_key=None,
            ddl_position=400,
            ref_function=None,
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            dependency_types=("agent",),
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary="An agent evaluation: its question dataset, the table it reads, and its run configuration.",
            authored_in="an agent's `evals/` folder",
            publishes="TABLE, DATASET, and a staged run configuration",
        ),
    ),
    (
        MemberType(
            "relationship",
            "snowflake_relationships",
            MemberSource.FILES,
            10,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "relationship",
        ),
        MemberType("fact", "facts", MemberSource.DBT_META, 30, AttachRule.TABLE_MEMBERSHIP, "semantic_view", None),
        MemberType(
            "dimension", "dimensions", MemberSource.DBT_META, 40, AttachRule.TABLE_MEMBERSHIP, "semantic_view", None
        ),
        MemberType(
            "metric",
            "snowflake_metrics",
            MemberSource.FILES,
            50,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "metric",
        ),
        MemberType(
            "filter",
            "snowflake_filters",
            MemberSource.FILES,
            60,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "filter",
        ),
        MemberType(
            "verified_query",
            "snowflake_verified_queries",
            MemberSource.FILES,
            70,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "verified_query",
        ),
        MemberType(
            "custom_instruction",
            "snowflake_custom_instructions",
            MemberSource.FILES,
            80,
            AttachRule.VIEW_NAME,
            "semantic_view",
            "custom_instructions",
        ),
    ),
)

# The name the semantic-view compiler uses; the registry describes every artifact type.
SEMANTIC_REGISTRY = ARTIFACT_REGISTRY
