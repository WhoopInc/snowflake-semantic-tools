"""Closed artifact registry and the members each artifact type owns.

Types are registered on a `RegistryBuilder`, whose `freeze` runs every integrity check and
returns the read-only `Registry`; a registration after `freeze` is refused. The checks run in a
fixed order and the first failure raises, so a registry with several faults always reports the
same one. Each raises `RegistryIntegrityError` carrying an SST-REG code.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum, auto
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import RULE_SETS, ErrorSpec
from snowflake_semantic_tools.domain.diagnostics.integrity import registry_fault


class MemberSource(Enum):
    """Where a member type is authored.

    FILES members are declared in SST's own YAML files, under their type's root key. DBT_META
    members are read from the `config.meta.sst` block of dbt model columns.
    """

    FILES = auto()
    DBT_META = auto()


class AttachRule(Enum):
    """How a member finds the semantic views it belongs to.

    TABLE_MEMBERSHIP attaches it to every view that holds all the tables it needs. VIEW_NAME
    attaches it to each view that names it.
    """

    TABLE_MEMBERSHIP = auto()
    VIEW_NAME = auto()


class GrantPreservation(Enum):
    """How an artifact type's grants survive an update.

    CLAUSE keeps them across a replace with `COPY GRANTS`; REPLAY re-applies them after a
    replace; NONE leaves them alone, because the object is never replaced.
    """

    CLAUSE = auto()
    REPLAY = auto()
    NONE = auto()


class ArtifactLifecycle(Enum):
    """How plan and apply reconcile an artifact type.

    An OBJECT type is one Snowflake object, listed by its object type and planned generically.
    A COMPOSITE type is several objects, planned and applied together by its own lifecycle
    handler.
    """

    OBJECT = auto()
    COMPOSITE = auto()


@dataclass(frozen=True, slots=True)
class ArtifactType:
    """One kind of artifact SST publishes, and how plan and apply treat it.

    `RegistryBuilder.freeze` refuses a set of types that contradict one another; it states the rules.

    Attributes:
        name: The type's name, which is the kind part of its artifacts' keys.
        root_key: The YAML key its declarations are listed under; None for a type authored some
            other way, such as one folder per artifact.
        ddl_position: Where its changes fall in plan order, lowest first; it is higher than the
            position of every type it depends on.
        ref_function: The template function another artifact names it with; None when none can.
        member_types: The member types it owns: exactly those whose `owner_type` names it.
        object_type: The Snowflake object type its objects are listed by; "" for a composite
            type, or for one that lists several `object_types` instead.
        prunable: Whether `--prune` drops its object once the object's source is deleted.
        replaces_on_update: Whether an update replaces the object rather than altering it in place.
        dependency_types: The types it may depend on, which it is planned after.
        pins_versions_of: The dependency types whose published version its payload names.
        object_types: The object types its objects may be, when there are several.
        validation_rules: The rule sets, by `RULE_SETS` name, whose codes validate it.
        dir_key: The `project.` configuration key naming the directory its files are under.
        nested_under: The type inside each of whose artifact folders its own files are; "" for
            a type whose directory is `dir_key` itself.
    """

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
    validation_rules: tuple[str, ...] = ()
    dir_key: str = ""
    nested_under: str = ""


@dataclass(frozen=True, slots=True)
class MemberType:
    """One kind of semantic member, and how it is authored and attached to its owner's artifacts.

    Attributes:
        name: The type's name, which is the kind part of its members' keys.
        root_key: The YAML key its declarations are listed under; unique across the registry.
        clause_position: Orders its owner's member types, lowest first; unique per owner.
        owner_type: The artifact type whose artifacts it attaches to.
        ref_function: The template function that names a member of this type; None when none can.
        validation_rules: The rule sets, by `RULE_SETS` name, whose codes validate it.
    """

    name: str
    root_key: str
    source: MemberSource
    clause_position: int
    attaches_by: AttachRule
    owner_type: str
    ref_function: str | None
    validation_rules: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Registry:
    """The artifact and member types, each map keyed by type name.

    A registry `RegistryBuilder.freeze` returns has passed every integrity check, and its maps
    are read-only.
    """

    artifacts: Mapping[str, ArtifactType]
    members: Mapping[str, MemberType]


# Fields every registration fills, because the generated references and discovery read them.
REQUIRED_ARTIFACT_FIELDS = ("name", "summary", "authored_in", "publishes", "dir_key", "validation_rules")
REQUIRED_MEMBER_FIELDS = ("name", "root_key", "owner_type", "validation_rules")
# Only these artifact types may own members (SST-REG022). Widening the set is the act of
# merging the member and artifact registries, so the decision is recorded here, by name.
MEMBER_OWNING_TYPES = frozenset({"semantic_view"})
# Every template function some resolver in the package handles: `domain.resolve.template` for
# `ref`, `metric`, `var`, `custom_instructions` and tags, refusing the legacy `table` and
# `column`; the semantic loader for `custom_instructions` entries; the agent loader for `file`,
# `skill` and `plugin`; agent tool resolution for `semantic_view`, `tool` and `agent`; and the
# eval config reader for `eval_metric`.
RESOLVED_FUNCTIONS = frozenset(
    {
        *("ref", "table", "column", "metric", "var", "tag", "custom_instructions", "file"),
        *("eval_metric", "semantic_view", "tool", "agent", "skill", "plugin"),
    }
)
# Resolved functions that address something outside this registry: dbt models and columns,
# project variables and files, tags, and the shared eval metrics.
NON_ARTIFACT_FUNCTIONS = frozenset({"ref", "table", "column", "var", "tag", "file", "eval_metric"})
# Artifact types nothing references, by design: an eval and a profile are leaves, so they need
# no template function. Named rather than inferred, so a type that forgot one still fails.
DELIBERATELY_UNREFERENCED = frozenset({"eval", "profile"})


class RegistryBuilder:
    """Collects artifact and member type registrations until `freeze` checks them once.

    A name registered twice is refused at once; everything else is checked by `freeze`.
    """

    def __init__(self) -> None:
        self._artifacts: dict[str, ArtifactType] = {}
        self._members: dict[str, MemberType] = {}
        self._frozen = False

    def artifact(self, artifact: ArtifactType) -> None:
        """Register one artifact type.

        Raises:
            RegistryIntegrityError: SST-REG900 after `freeze`; SST-REG002 for a repeated name.
        """
        self._open(f"artifact type {artifact.name}")
        if artifact.name in self._artifacts:
            raise registry_fault("SST-REG002", type=artifact.name)
        self._artifacts[artifact.name] = artifact

    def member(self, member: MemberType) -> None:
        """Register one member type.

        Raises:
            RegistryIntegrityError: SST-REG900 after `freeze`; SST-REG002 for a repeated name.
        """
        self._open(f"member type {member.name}")
        if member.name in self._members:
            raise registry_fault("SST-REG002", type=member.name)
        self._members[member.name] = member

    def freeze(
        self,
        *,
        rule_sets: Mapping[str, tuple[ErrorSpec, ...]] = RULE_SETS,
        functions: frozenset[str] = RESOLVED_FUNCTIONS,
    ) -> Registry:
        """Check every registration against the others, and return the read-only registry.

        The checks run in this order: required fields, root keys, positions, the names types
        refer to, member ownership, rules, the dependency graph's cycles and then its order,
        grant handling and lifecycle, reference functions, and the resolver set.

        Args:
            rule_sets: The validation rule sets a type's `validation_rules` may name.
            functions: The template functions the package resolves.

        Raises:
            RegistryIntegrityError: SST-REG900 when already frozen; otherwise the first of
                SST-REG001, 003, 004, 007, 022, 006, 011, 010, 021, 005 and 020 that applies.
        """
        self._open("freeze")
        artifacts, members = tuple(self._artifacts.values()), tuple(self._members.values())
        _check_required(artifacts, members)
        _check_root_keys(artifacts, members)
        _check_positions(artifacts, members)
        _check_names(self._artifacts, self._members)
        _check_ownership(artifacts, members)
        _check_rules((*artifacts, *members), rule_sets)
        _check_cycles(self._artifacts)
        _check_dependency_order(self._artifacts)
        for artifact in artifacts:
            _check_grant_preservation(artifact)
        _check_ref_functions(artifacts, members)
        _check_resolvers(artifacts, members, functions)
        self._frozen = True
        return Registry(MappingProxyType(dict(self._artifacts)), MappingProxyType(dict(self._members)))

    def _open(self, what: str) -> None:
        if self._frozen:
            raise registry_fault("SST-REG900", detail=f"{what} after the registry was frozen")


def build_registry(
    artifact_types: tuple[ArtifactType, ...],
    member_types: tuple[MemberType, ...],
    *,
    rule_sets: Mapping[str, tuple[ErrorSpec, ...]] = RULE_SETS,
    functions: frozenset[str] = RESOLVED_FUNCTIONS,
) -> Registry:
    """Register every type on a new `RegistryBuilder`, artifacts first, and freeze it.

    Raises:
        RegistryIntegrityError: As `RegistryBuilder.artifact`, `member` and `freeze` do.
    """
    builder = RegistryBuilder()
    for artifact in artifact_types:
        builder.artifact(artifact)
    for member in member_types:
        builder.member(member)
    return builder.freeze(rule_sets=rule_sets, functions=functions)


def check_member_index(covered: Iterable[str], registry: Registry) -> None:
    """Refuse a member index that does not hold exactly the member types `semantic_view` declares.

    Raises:
        RegistryIntegrityError: SST-REG023.
    """
    declared = sorted(registry.artifacts["semantic_view"].member_types)
    indexed = sorted(covered)
    if indexed != declared:
        raise registry_fault("SST-REG023", covered=indexed, declared=declared)


def _check_required(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse a registration that leaves a required field empty (SST-REG001)."""
    for kind, fields in ((artifacts, REQUIRED_ARTIFACT_FIELDS), (members, REQUIRED_MEMBER_FIELDS)):
        for entry in kind:
            for field in fields:
                if getattr(entry, field) in (None, "", ()):
                    raise registry_fault("SST-REG001", type=entry.name or "<unnamed>", field=field)


def _check_root_keys(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse a YAML root key claimed twice; artifact types without one claim nothing (SST-REG003)."""
    claims: dict[str, list[str]] = {}
    for entry in (*artifacts, *members):
        if entry.root_key is not None:
            claims.setdefault(entry.root_key, []).append(entry.name)
    for root_key, types in claims.items():
        if len(types) > 1:
            raise registry_fault("SST-REG003", root_key=root_key, types=types)


def _check_positions(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse two members at one clause position of one owner, then two artifacts at one DDL position."""
    clauses: dict[tuple[str, int], list[str]] = {}
    for member in members:
        clauses.setdefault((member.owner_type, member.clause_position), []).append(member.name)
    positions: dict[int, list[str]] = {}
    for artifact in artifacts:
        positions.setdefault(artifact.ddl_position, []).append(artifact.name)
    for position, types in (*((key[1], value) for key, value in clauses.items()), *positions.items()):
        if len(types) > 1:
            raise registry_fault("SST-REG004", position=position, types=types)


def _check_names(artifacts: Mapping[str, ArtifactType], members: Mapping[str, MemberType]) -> None:
    """Refuse an owner, member type, or dependency that names an unregistered type (SST-REG007)."""
    for member in members.values():
        if member.owner_type not in artifacts:
            raise registry_fault("SST-REG007", type=member.name, member_type=member.owner_type)
    for artifact in artifacts.values():
        for name in (*artifact.member_types, *artifact.dependency_types):
            if name not in members and name not in artifacts:
                raise registry_fault("SST-REG007", type=artifact.name, member_type=name)
        for name in artifact.pins_versions_of:
            if name not in artifact.dependency_types:
                raise registry_fault("SST-REG001", type=artifact.name, field=f"dependency_types: {name}")


def _check_ownership(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse members on any type but `semantic_view`, and declared members that do not match owners."""
    for artifact in artifacts:
        if artifact.member_types and artifact.name not in MEMBER_OWNING_TYPES:
            raise registry_fault("SST-REG022", artifact=artifact.name, member_types=list(artifact.member_types))
        owned = {member.name for member in members if member.owner_type == artifact.name}
        for name in sorted(owned - set(artifact.member_types)):
            raise registry_fault("SST-REG001", type=artifact.name, field=f"member_types: {name}")
        for name in sorted(set(artifact.member_types) - owned):
            raise registry_fault("SST-REG007", type=artifact.name, member_type=name)


def _check_rules(entries: tuple[ArtifactType | MemberType, ...], rule_sets: Mapping[str, object]) -> None:
    """Refuse a type naming a validation rule set that is not registered (SST-REG006)."""
    for entry in entries:
        for rule_id in entry.validation_rules:
            if rule_id not in rule_sets:
                raise registry_fault("SST-REG006", type=entry.name, rule_id=rule_id)


def _check_cycles(artifacts: Mapping[str, ArtifactType]) -> None:
    """Refuse a cycle in the type dependency graph, named from its first type back to it (SST-REG011)."""
    done: set[str] = set()

    def visit(name: str, path: tuple[str, ...]) -> None:
        if name in path:
            cycle = (*path[path.index(name) :], name)
            raise registry_fault("SST-REG011", cycle=" -> ".join(cycle))
        if name in done:
            return
        for dependency in artifacts[name].dependency_types:
            visit(dependency, (*path, name))
        done.add(name)

    for name in artifacts:
        visit(name, ())


def _check_dependency_order(artifacts: Mapping[str, ArtifactType]) -> None:
    """Refuse an artifact whose DDL position does not follow every type it depends on (SST-REG010)."""
    for artifact in artifacts.values():
        for dependency in artifact.dependency_types:
            if artifacts[dependency].ddl_position >= artifact.ddl_position:
                raise registry_fault(
                    "SST-REG010", type=artifact.name, position=artifact.ddl_position, blocker=dependency
                )


def _check_grant_preservation(artifact: ArtifactType) -> None:
    """Refuse grant handling, or object lifecycle metadata, that contradicts how the type publishes.

    Raises:
        RegistryIntegrityError: SST-REG021 for a contradiction; SST-REG001 for an object type
            nothing can observe.
    """
    reason = _grant_contradiction(artifact)
    if reason is not None:
        raise registry_fault("SST-REG021", artifact=artifact.name, reason=reason)
    if artifact.lifecycle is ArtifactLifecycle.OBJECT and not artifact.object_type and not artifact.object_types:
        raise registry_fault("SST-REG001", type=artifact.name, field="object_type")


def _grant_contradiction(artifact: ArtifactType) -> str | None:
    if artifact.replaces_on_update and artifact.grant_preservation is GrantPreservation.NONE:
        return "it replaces its object and declares no grant preservation"
    if not artifact.replaces_on_update and artifact.grant_preservation is not GrantPreservation.NONE:
        return "it never replaces its object and declares grant preservation"
    if artifact.lifecycle is ArtifactLifecycle.COMPOSITE:
        if artifact.object_type or artifact.object_types:
            return "a composite type's grants are its handler's, so it cannot declare an object type"
        if artifact.prunable or artifact.replaces_on_update:
            return "a composite type is pruned and replaced by its handler, not generically"
    return None


def _check_ref_functions(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse a reference function claimed by two types; types without one claim nothing (SST-REG005)."""
    seen: set[str] = set()
    for entry in (*artifacts, *members):
        if entry.ref_function is None:
            continue
        if entry.ref_function in seen:
            raise registry_fault("SST-REG005", ref_function=entry.ref_function)
        seen.add(entry.ref_function)


def _check_resolvers(
    artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...], functions: frozenset[str]
) -> None:
    """Refuse a resolver and a registry that disagree in any of the three directions (SST-REG020).

    A resolved function that addresses no registered type, a type's function that no resolver
    handles, and an artifact type with no function that is not deliberately unreferenced.
    """
    declared = {entry.ref_function for entry in (*artifacts, *members) if entry.ref_function is not None}
    for function in sorted(functions - NON_ARTIFACT_FUNCTIONS - declared):
        raise registry_fault("SST-REG020", ref_function=function, direction="a resolver addresses no registered type")
    for function in sorted(declared - functions):
        raise registry_fault("SST-REG020", ref_function=function, direction="no resolver handles the type's function")
    for artifact in artifacts:
        if artifact.ref_function is None and artifact.name not in DELIBERATELY_UNREFERENCED:
            raise registry_fault("SST-REG020", ref_function=artifact.name, direction="the type has no ref_function")


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
            validation_rules=("shared", "semantic_view"),
            dir_key="semantic_models_dir",
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
            validation_rules=("shared", "tool"),
            dir_key="tools_dir",
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
            validation_rules=("shared", "skill"),
            dir_key="skills_dir",
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
            validation_rules=("shared", "skill"),
            dir_key="plugins_dir",
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
            validation_rules=("shared", "skill"),
            dir_key="profiles_dir",
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
            validation_rules=("shared", "agent"),
            dir_key="agents_dir",
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
            validation_rules=("shared", "eval"),
            dir_key="agents_dir",
            nested_under="agent",
        ),
    ),
    (
        # A relationship, filter, or verified query is attached by the tables it reads, and no
        # template function names one, so none has a ref_function.
        MemberType(
            "relationship",
            "snowflake_relationships",
            MemberSource.FILES,
            10,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            None,
            ("relationship",),
        ),
        MemberType(
            "fact",
            "facts",
            MemberSource.DBT_META,
            30,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            None,
            ("semantic_view",),
        ),
        MemberType(
            "dimension",
            "dimensions",
            MemberSource.DBT_META,
            40,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            None,
            ("semantic_view",),
        ),
        MemberType(
            "metric",
            "snowflake_metrics",
            MemberSource.FILES,
            50,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "metric",
            ("metric",),
        ),
        MemberType(
            "filter",
            "snowflake_filters",
            MemberSource.FILES,
            60,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            None,
            ("filter",),
        ),
        MemberType(
            "verified_query",
            "snowflake_verified_queries",
            MemberSource.FILES,
            70,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            None,
            ("filter",),
        ),
        MemberType(
            "custom_instruction",
            "snowflake_custom_instructions",
            MemberSource.FILES,
            80,
            AttachRule.VIEW_NAME,
            "semantic_view",
            "custom_instructions",
            ("filter",),
        ),
    ),
)

# The name the semantic-view compiler uses; the registry describes every artifact type.
SEMANTIC_REGISTRY = ARTIFACT_REGISTRY
